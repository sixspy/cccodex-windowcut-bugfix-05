"""窗口锁与争用登记（公共并发面，题面争用句应对得上）。

入账幂等册（ops/入账幂等册.md）锁定甲路径：
- 同窗同键先占 `ingest-key:<window_name>:<biz_key>` 独占键锁；
- 后到者只写 contention_log，业务零写；
- 键锁对同一持有人也不可重入，避免同人双击形成双影。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from auth import AuthHeader, require_user
from db import connect, write_audit

router = APIRouter(tags=["locks"])


class LockIn(BaseModel):
    lock_key: str
    action: str = "acquire"


def begin_write(conn) -> None:
    """以 IMMEDIATE 事务开始写入：锁检查与业务写在同一个写事务内，避免先读后插竞态。"""
    conn.execute("BEGIN IMMEDIATE")


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _log_contention(conn, lock_key: str, holder: str, existing_holder: str) -> None:
    conn.execute(
        """INSERT INTO contention_log(lock_key,actor,action,detail,created_at)
           VALUES (?,?,?,?,?)""",
        (lock_key, holder, "争用", f"持有者={existing_holder}", _now()),
    )


def try_acquire(conn, lock_key: str, holder: str) -> bool:
    row = conn.execute(
        "SELECT holder FROM window_locks WHERE lock_key=?", (lock_key,)
    ).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO window_locks(lock_key,holder,acquired_at) VALUES (?,?,?)",
            (lock_key, holder, _now()),
        )
        return True
    if row["holder"] == holder:
        return True
    _log_contention(conn, lock_key, holder, row["holder"])
    return False


def try_acquire_exclusive(conn, lock_key: str, holder: str) -> bool:
    """独占键锁：只要键上已有持有人（含同一持有人）即判后到争用。"""
    row = conn.execute(
        "SELECT holder FROM window_locks WHERE lock_key=?", (lock_key,)
    ).fetchone()
    if row is None:
        try:
            conn.execute(
                "INSERT INTO window_locks(lock_key,holder,acquired_at) VALUES (?,?,?)",
                (lock_key, holder, _now()),
            )
            return True
        except sqlite3.IntegrityError:
            row = conn.execute(
                "SELECT holder FROM window_locks WHERE lock_key=?", (lock_key,)
            ).fetchone()
    _log_contention(conn, lock_key, holder, row["holder"] if row else "并发写入")
    return False


def release(conn, lock_key: str, holder: str) -> None:
    conn.execute(
        "DELETE FROM window_locks WHERE lock_key=? AND holder=?",
        (lock_key, holder),
    )


def require_lock(conn, lock_key: str, holder: str) -> None:
    if not try_acquire(conn, lock_key, holder):
        raise HTTPException(409, "争用：后到只记争用，不写业务")


def ingest_key_lock(window_name: str, biz_key: str) -> str:
    """入账键锁键名：同窗同键唯一（与入账幂等册第 2 节一致）。"""
    return f"ingest-key:{window_name}:{biz_key}"


@router.get("/api/locks")
def list_locks(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM window_locks ORDER BY acquired_at DESC"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.get("/api/contention")
def list_contention(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM contention_log ORDER BY id DESC LIMIT 100"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.post("/api/locks/acquire")
def acquire_lock(body: LockIn, authorization: AuthHeader = None):
    user = require_user(authorization)
    with connect() as conn:
        ok = try_acquire(conn, body.lock_key, user)
        write_audit(
            conn,
            user,
            "lock_acquire" if ok else "lock_contention",
            body.lock_key,
            {"ok": ok},
        )
        conn.commit()
        if not ok:
            raise HTTPException(409, "争用：后到只记争用")
        return {"ok": True, "lock_key": body.lock_key, "holder": user}


@router.post("/api/locks/release")
def release_lock(body: LockIn, authorization: AuthHeader = None):
    user = require_user(authorization)
    with connect() as conn:
        release(conn, body.lock_key, user)
        write_audit(conn, user, "lock_release", body.lock_key, {})
        conn.commit()
    return {"ok": True}
