"""事件入账：窗内 / 拒收 / 迟到桶分流。

入账幂等口径见 `ops/入账幂等册.md`，本模块锁定**甲路径：查重拒绝**：
- 同窗同键先占独占键锁（单独提交，使并发后到者可见并只记争用）；
- 同窗同键已存在正式入账行时整笔拒绝（409），正式零增，落 `ingest_dup_skip` 审计；
- 不走覆盖/generation 路径，两边不得混用。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime

from fastapi import APIRouter, HTTPException

from auth import AuthHeader, require_user
from db import connect, write_audit
from locks import (
    begin_write,
    ingest_key_lock,
    release,
    try_acquire_exclusive,
    require_lock,
)
from models import EventIn

router = APIRouter(tags=["ingest"])

# 入账幂等册锁定的正确性口径（甲=查重拒绝 / 乙=审计覆盖）。
# 本仓只实现甲；实现偏离时拒写并记 `幂等偏了`。
IDEMPOTENCY_PATH = "甲"
LEDGER_PATH = "甲"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _guard_ledger_path(conn, user: str, biz_key: str) -> None:
    if IDEMPOTENCY_PATH == LEDGER_PATH:
        return
    write_audit(
        conn,
        user,
        "幂等偏了",
        biz_key,
        {"expected": LEDGER_PATH, "actual": IDEMPOTENCY_PATH},
    )
    conn.commit()
    raise HTTPException(409, f"入账幂等偏离幂等册（{IDEMPOTENCY_PATH}），拒写")


@router.post("/api/events")
def ingest_event(body: EventIn, authorization: AuthHeader = None):
    user = require_user(authorization)
    key_lock = ingest_key_lock(body.window_name, body.biz_key)
    window_lock = f"ingest:{body.window_name}"
    with connect() as conn:
        acquired_key = False
        acquired_window = False
        try:
            # 第一段：独占键锁独立提交，并发后到者立即可见并只记争用。
            begin_write(conn)
            acquired_key = try_acquire_exclusive(conn, key_lock, user)
            conn.commit()
            if not acquired_key:
                raise HTTPException(
                    409, "争用：同键已有写入占锁，后到只记争用，不写业务"
                )

            _guard_ledger_path(conn, user, body.biz_key)

            # 第二段：业务写在同一事务内完成锁校验与查重，防先读后插竞态。
            begin_write(conn)
            require_lock(conn, window_lock, user)
            acquired_window = True

            w = conn.execute(
                "SELECT * FROM windows WHERE name=?", (body.window_name,)
            ).fetchone()
            if not w:
                raise HTTPException(404, "窗口不存在")
            ts = body.event_ts
            created = _now()

            if w["status"] == "已关闭":
                cur = conn.execute(
                    """INSERT INTO events(window_id,biz_key,event_ts,amount,disposition,created_at)
                       VALUES (?,?,?,?,?,?)""",
                    (None, body.biz_key, ts, body.amount, "迟到桶", created),
                )
                eid = cur.lastrowid
                conn.execute(
                    """INSERT INTO late_bucket(event_id,window_name,biz_key,event_ts,amount,reason,created_at)
                       VALUES (?,?,?,?,?,?,?)""",
                    (
                        eid,
                        body.window_name,
                        body.biz_key,
                        ts,
                        body.amount,
                        "关窗后到达",
                        created,
                    ),
                )
                write_audit(
                    conn,
                    user,
                    "ingest_late",
                    body.biz_key,
                    {"window": body.window_name},
                )
                conn.commit()
                return {"disposition": "迟到桶", "event_id": eid}

            if not (w["open_at"] <= ts <= w["close_at"]):
                conn.execute(
                    """INSERT INTO events(window_id,biz_key,event_ts,amount,disposition,created_at)
                       VALUES (?,?,?,?,?,?)""",
                    (None, body.biz_key, ts, body.amount, "拒收", created),
                )
                conn.execute(
                    """INSERT INTO reject_slips(window_name,biz_key,event_ts,amount,reason,created_at)
                       VALUES (?,?,?,?,?,?)""",
                    (
                        body.window_name,
                        body.biz_key,
                        ts,
                        body.amount,
                        "窗外且窗仍开启",
                        created,
                    ),
                )
                write_audit(
                    conn,
                    user,
                    "ingest_reject",
                    body.biz_key,
                    {"window": body.window_name},
                )
                conn.commit()
                raise HTTPException(409, "事件时间落在窗外且窗仍开启：拒收")

            # 甲路径：同窗同 biz_key 已有正式入账行 → 整笔拒绝，正式零增。
            dup = conn.execute(
                """SELECT id FROM events
                   WHERE window_id=? AND biz_key=? AND disposition='窗内'
                   ORDER BY id LIMIT 1""",
                (w["id"], body.biz_key),
            ).fetchone()
            if dup:
                write_audit(
                    conn,
                    user,
                    "ingest_dup_skip",
                    body.biz_key,
                    {
                        "window_id": w["id"],
                        "existing_event_id": dup["id"],
                        "path": "甲",
                        "outcome": "拒绝，正式零增",
                    },
                )
                conn.commit()
                raise HTTPException(
                    409, "同窗同业务键已入账：整笔拒绝，正式零增"
                )

            try:
                cur = conn.execute(
                    """INSERT INTO events(window_id,biz_key,event_ts,amount,disposition,created_at)
                       VALUES (?,?,?,?,?,?)""",
                    (w["id"], body.biz_key, ts, body.amount, "窗内", created),
                )
            except sqlite3.IntegrityError:
                # 库级唯一索引兜底命中：仍按甲路径记重复拒绝，不落正式行。
                write_audit(
                    conn,
                    user,
                    "ingest_dup_skip",
                    body.biz_key,
                    {
                        "window_id": w["id"],
                        "path": "甲",
                        "outcome": "唯一索引拦截，正式零增",
                    },
                )
                conn.commit()
                raise HTTPException(
                    409, "同窗同业务键已入账：整笔拒绝，正式零增"
                )
            write_audit(
                conn,
                user,
                "ingest_in_window",
                body.biz_key,
                {"window_id": w["id"]},
            )
            conn.commit()
            return {
                "disposition": "窗内",
                "window_id": w["id"],
                "event_id": cur.lastrowid,
            }
        except HTTPException:
            # 业务未写；提交以保留争用/审计记录，再抛出。
            conn.commit()
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            # 释放只删自己持有的锁；未占到的锁不会误删他人。
            if acquired_window:
                release(conn, window_lock, user)
            if acquired_key:
                release(conn, key_lock, user)
            conn.commit()


@router.get("/api/events")
def list_events(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM events ORDER BY id DESC LIMIT 200"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.get("/api/rejects")
def list_rejects(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM reject_slips ORDER BY id DESC LIMIT 100"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}
