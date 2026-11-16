"""Screens: GET /api/screens/{name}, GET /api/provenance/{metric_id}, POST /api/reset, GET /api/health.

Screen payloads and provenance come from app/fixture.json (built by scripts/export_fixture.py), so the
headline numbers on One Truth, Forecast, Save Subscribers and the drawer never change mid-demo.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from ..config import get_settings
from ..fallback import fixture, live_allowed, recorded, reset_breakers
from . import save

router = APIRouter(prefix="/api", tags=["screens"])
SCREENS = ("truth", "forecast", "ask", "save", "governance")


@router.get("/screens/{name}")
async def screen(name: str) -> dict:
    if name not in SCREENS:
        raise HTTPException(404, f"There is no screen '{name}'.")
    payload = recorded("screens", name)
    if name == "save":
        payload["approval"] = save.APPROVAL
    return payload


@router.get("/provenance/{metric_id}")
async def provenance(metric_id: str) -> dict:
    p = fixture()["provenance"].get(metric_id)
    if p is None:
        raise HTTPException(404, "That number has no recorded source.")
    return {"metric_id": metric_id, **p}


@router.post("/reset")
async def reset() -> dict:
    save.reset()
    reset_breakers()
    return {"ok": True}


@router.get("/health")
async def health(request: Request) -> dict:
    f = fixture()
    s = get_settings()
    return {"ok": True, "live": live_allowed(request), "force_fixture": s.force_fixture,
            "illustrative": f.get("illustrative", False), "fixture_source": f.get("source"),
            "generated_at": f.get("generated_at"), "gateway_model": s.gateway_model}
