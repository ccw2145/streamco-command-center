"""Live call or recorded copy.

Screens always come from app/fixture.json, so the headline numbers never change mid-demo. The interactive
parts (Ask, Genie follow-ups, Regenerate, Approve) try live first and fall back to the fixture on any error
or timeout. `call()` runs one blocking client call with a timeout; a dependency that times out or can't be
reached is skipped for BREAKER_S seconds so a dead warehouse doesn't make every click wait. FORCE_FIXTURE=1
(server) or the X-Mode: recorded header (the F key in the UI) never calls live.
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import time
from collections.abc import Callable

import requests
from fastapi import Request

from .config import FIXTURE_PATH, get_settings

log = logging.getLogger(__name__)
BREAKER_S = 30.0
_open_until: dict[str, float] = {}
_fixture: dict | None = None
_fixture_mtime = 0.0


class LiveUnavailable(Exception):
    pass


def fixture() -> dict:
    """The recorded copy, reloaded when the file changes (scripts/export_fixture.py rewrites it)."""
    global _fixture, _fixture_mtime
    mtime = FIXTURE_PATH.stat().st_mtime
    if _fixture is None or mtime != _fixture_mtime:
        _fixture = json.loads(FIXTURE_PATH.read_text())
        _fixture_mtime = mtime
    return _fixture


def recorded(*path: str):
    node = fixture()
    for key in path:
        node = node[key]
    return copy.deepcopy(node)


def live_allowed(request: Request | None) -> bool:
    if get_settings().force_fixture:
        return False
    return not (request is not None and request.headers.get("x-mode", "").lower() == "recorded")


def _unreachable(exc: BaseException) -> bool:
    return isinstance(exc, (TimeoutError, asyncio.TimeoutError, ConnectionError, requests.ConnectionError,
                            requests.Timeout))


async def call(dep: str, fn: Callable, *args, timeout_s: float, **kwargs):
    """Run fn(*args) in a thread with a timeout. Raises LiveUnavailable on any failure."""
    if get_settings().force_fixture:
        raise LiveUnavailable("FORCE_FIXTURE is set")
    if _open_until.get(dep, 0) > time.monotonic():
        raise LiveUnavailable(f"{dep} is paused after a timeout")
    try:
        return await asyncio.wait_for(asyncio.to_thread(fn, *args, **kwargs), timeout_s)
    except Exception as exc:  # noqa: BLE001 - every live failure becomes a fallback
        if _unreachable(exc):
            _open_until[dep] = time.monotonic() + BREAKER_S
        log.warning("%s call failed: %s: %s", dep, type(exc).__name__, str(exc)[:300])
        raise LiveUnavailable(f"{dep}: {type(exc).__name__}: {str(exc)[:200]}") from exc


async def sql(statement: str, **params) -> list[dict]:
    """One warehouse query (bare object names resolve in CATALOG_NAME.SCHEMA_NAME)."""
    from .databricks import run_sql

    s = get_settings()
    return await call("sql", run_sql, statement, params, timeout_s=s.sql_timeout_s + 2)


def reset_breakers() -> None:
    _open_until.clear()
