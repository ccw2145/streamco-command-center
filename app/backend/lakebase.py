"""Lakebase (Postgres) for operational serving: the synced per-subscriber tables and the app's approvals.

Low traffic, so no pool: each call opens a short connection with an OAuth database credential, cached for 45 min
(credentials last about an hour). Locally it connects as the presenter; deployed, as the app's service principal
(the app's `postgres` resource in resources/app.yml gives it a role with CAN_CONNECT_AND_CREATE).
"""
from __future__ import annotations

import os
import threading
import time

import psycopg
from psycopg.rows import dict_row

from .config import get_settings
from .databricks import workspace

_lock = threading.Lock()
_cred: dict = {}           # token, expires, host, user
_TTL_S = 45 * 60


def _target() -> dict:
    with _lock:
        if _cred and _cred["expires"] > time.time():
            return dict(_cred)
        s, w = get_settings(), workspace()
        host = os.getenv("PGHOST") or w.postgres.get_endpoint(name=s.lakebase_endpoint).status.hosts.host
        user = os.getenv("PGUSER") or w.current_user.me().user_name
        token = w.postgres.generate_database_credential(endpoint=s.lakebase_endpoint).token
        _cred.update(token=token, expires=time.time() + _TTL_S, host=host, user=user)
        return dict(_cred)


def query(stmt: str, params: dict | None = None, *, timeout_s: float = 10) -> list[dict]:
    """Run one statement; returns rows as dicts (empty for statements without a result)."""
    return timed_query(stmt, params, timeout_s=timeout_s)[0]


def timed_query(stmt: str, params: dict | None = None, *, timeout_s: float = 10) -> tuple[list[dict], float]:
    """Like query(), plus the statement's own time in ms (excluding connection setup)."""
    t = _target()
    with psycopg.connect(host=t["host"], dbname=get_settings().lakebase_database, user=t["user"], password=t["token"],
                         sslmode="require", connect_timeout=int(timeout_s), autocommit=True,
                         options=f"-c statement_timeout={int(timeout_s * 1000)}", row_factory=dict_row) as conn:
        start = time.perf_counter()
        cur = conn.execute(stmt, params or {})
        rows = cur.fetchall() if cur.description else []
        return rows, (time.perf_counter() - start) * 1000
