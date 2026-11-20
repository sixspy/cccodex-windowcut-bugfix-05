"""入账幂等红测：同键双影 / 同键双人争用。

顺序要求：本文件先落双影失败测见红，再落争用失败测见红；
两条红测齐备后才允许改 ingest_svc / audit_svc / locks 写路径。
"""
from fastapi.testclient import TestClient

from app import app, init_db
import pytest


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("db.DB_PATH", tmp_path / "idem.db")
    init_db()
    yield


def _auth(c: TestClient, username: str = "admin", password: str = "123456") -> dict:
    tok = c.post(
        "/api/login", json={"username": username, "password": password}
    ).json()["token"]
    return {"Authorization": "Bearer " + tok}


def test_same_key_double_ingest_must_not_double_image():
    """同窗同 biz_key 连打两次：不得双影，且对账不得被 DISTINCT 掩护。"""
    c = TestClient(app)
    h = _auth(c)
    payload = {
        "window_name": "W-1102",
        "biz_key": "DUP-IMG-1",
        "event_ts": "2026-11-02T12:00:00",
        "amount": 5,
    }

    first = c.post("/api/events", headers=h, json=payload)
    assert first.status_code == 200
    assert first.json()["disposition"] == "窗内"

    second = c.post("/api/events", headers=h, json=payload)

    # 核心缺陷：同键在事件列表出现两行（双影）。
    events = c.get("/api/events", headers=h).json()["items"]
    dup_rows = [e for e in events if e["biz_key"] == "DUP-IMG-1"]
    assert len(dup_rows) == 1, f"同键双影：事件列表出现 {len(dup_rows)} 行"

    # 甲路径：第二笔整笔拒绝，正式零增。
    assert second.status_code == 409, "重复入账未被整笔拒绝"
    assert "拒绝" in second.json().get("detail", "")

    detail = c.get("/api/windows/2", headers=h).json()
    assert detail["in_window_count"] == 1, "窗口详情与事件列表不同态"
    assert detail["in_window_amount"] == 5, "窗口详情金额被重复入账抬高"

    # 对账必须与事件列表/窗口详情同态，不能用 DISTINCT 掩盖。
    recon = c.get("/api/reconcile/2", headers=h).json()
    assert recon["window_events"] == 1, "对账笔数与事件列表不同态"
    assert recon["window_amount"] == 5, "对账金额被重复入账抬高"

    # 跳过必须留审计，不是只改提示文案。
    audits = c.get("/api/audit?limit=200", headers=h).json()["items"]
    assert any(
        a["action"] == "ingest_dup_skip" and a["target"] == "DUP-IMG-1"
        for a in audits
    ), "重复入账缺少 skip 审计"


def test_concurrent_same_key_must_take_key_lock_and_log_contention():
    """两人同时写同窗同键：先到占键锁，后到只记争用，不写业务。"""
    c = TestClient(app)
    h1 = _auth(c)
    h2 = _auth(c, "closer", "close123")
    key = "ingest-key:W-1102:CONTEND-1"

    # 甲已占住该键锁（仍在写入中）。
    assert (
        c.post("/api/locks/acquire", headers=h1, json={"lock_key": key}).status_code
        == 200
    )

    # 乙同时写同键：必须被键锁挡下，只记争用。
    second = c.post(
        "/api/events",
        headers=h2,
        json={
            "window_name": "W-1102",
            "biz_key": "CONTEND-1",
            "event_ts": "2026-11-02T13:00:00",
            "amount": 7,
        },
    )
    assert second.status_code == 409, "后到者未被键锁挡住"
    assert "争用" in second.json().get("detail", "")

    # 丙处（争用）与其余四处同态：业务零写。
    events = c.get("/api/events", headers=h2).json()["items"]
    assert not [e for e in events if e["biz_key"] == "CONTEND-1"], "争用方仍写了业务"
    detail = c.get("/api/windows/2", headers=h2).json()
    assert detail["in_window_count"] == 0
    assert detail["in_window_amount"] == 0
    recon = c.get("/api/reconcile/2", headers=h2).json()
    assert recon["window_events"] == 0
    assert recon["window_amount"] == 0
    contention = c.get("/api/contention", headers=h1).json()["items"]
    assert any(
        x["lock_key"] == key and x["actor"] == "closer" for x in contention
    ), "后到争用未登记"
    audits = c.get("/api/audit?limit=200", headers=h1).json()["items"]
    assert not any(
        a["action"] == "ingest_in_window" and a["target"] == "CONTEND-1"
        for a in audits
    ), "争用方仍落了入账审计"

    # 甲释放后，同键才可正常入账一次。
    assert c.post("/api/locks/release", headers=h1, json={"lock_key": key}).status_code == 200
    ok = c.post(
        "/api/events",
        headers=h2,
        json={
            "window_name": "W-1102",
            "biz_key": "CONTEND-1",
            "event_ts": "2026-11-02T13:00:00",
            "amount": 7,
        },
    )
    assert ok.status_code == 200
    events = c.get("/api/events", headers=h2).json()["items"]
    assert len([e for e in events if e["biz_key"] == "CONTEND-1"]) == 1


def test_parallel_same_key_writes_leave_single_image():
    """真并发两线程写同窗同键：只允许一行正式事件，五处同态。"""
    import threading

    c = TestClient(app)
    h1 = _auth(c)
    h2 = _auth(c, "closer", "close123")
    payload = {
        "window_name": "W-1102",
        "biz_key": "RACE-1",
        "event_ts": "2026-11-02T14:00:00",
        "amount": 6,
    }
    results: list[int] = []
    barrier = threading.Barrier(2)

    def fire(headers):
        client = TestClient(app)
        barrier.wait()
        r = client.post("/api/events", headers=headers, json=payload)
        results.append(r.status_code)

    t1 = threading.Thread(target=fire, args=(h1,))
    t2 = threading.Thread(target=fire, args=(h2,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert sorted(results).count(200) == 1, f"应恰有一笔成功，实际 {results}"

    events = c.get("/api/events", headers=h1).json()["items"]
    rows = [e for e in events if e["biz_key"] == "RACE-1" and e["disposition"] == "窗内"]
    assert len(rows) == 1, f"并发同键出现 {len(rows)} 行正式事件"

    detail = c.get("/api/windows/2", headers=h1).json()
    assert detail["in_window_count"] == 1
    assert detail["in_window_amount"] == 6
    recon = c.get("/api/reconcile/2", headers=h1).json()
    assert recon["window_events"] == 1
    assert recon["window_amount"] == 6

    audits = c.get("/api/audit?limit=200", headers=h1).json()["items"]
    assert not any(
        a["action"] == "幂等偏了" for a in audits
    ), "实现偏离入账幂等册"


def test_deviation_from_ledger_is_rejected_and_audited(monkeypatch):
    """实现偏离幂等册时拒写：记 `幂等偏了`，业务零增。"""
    import ingest_svc

    monkeypatch.setattr(ingest_svc, "IDEMPOTENCY_PATH", "乙")
    c = TestClient(app)
    h = _auth(c)
    r = c.post(
        "/api/events",
        headers=h,
        json={
            "window_name": "W-1102",
            "biz_key": "DEV-1",
            "event_ts": "2026-11-02T15:00:00",
            "amount": 2,
        },
    )
    assert r.status_code == 409
    assert "偏离" in r.json().get("detail", "")
    events = c.get("/api/events", headers=h).json()["items"]
    assert not [e for e in events if e["biz_key"] == "DEV-1"]
    audits = c.get("/api/audit?limit=200", headers=h).json()["items"]
    assert any(
        a["action"] == "幂等偏了" and a["target"] == "DEV-1" for a in audits
    ), "偏离幂等册未记审计"
    assert not any(
        a["action"] == "ingest_in_window" and a["target"] == "DEV-1"
        for a in audits
    )
