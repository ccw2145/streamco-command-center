import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture
def client(monkeypatch):
    """A test client in recorded-only mode (FORCE_FIXTURE=1), with the Ask replay sped up."""
    monkeypatch.setenv("FORCE_FIXTURE", "1")
    from backend.config import get_settings

    get_settings.cache_clear()
    from fastapi.testclient import TestClient

    from backend import fallback
    from backend.main import app
    from backend.routers import ask, save

    monkeypatch.setattr(ask, "REPLAY_SPEED", 200.0)
    save.reset()
    fallback.reset_breakers()
    yield TestClient(app)
    get_settings.cache_clear()


@pytest.fixture
def live_client(client, monkeypatch):
    """Same client with live calls allowed (each test stubs the Databricks clients it needs)."""
    monkeypatch.delenv("FORCE_FIXTURE")
    from backend.config import get_settings

    get_settings.cache_clear()
    return client


def sse_events(text: str) -> list[tuple[str, dict]]:
    out = []
    for frame in text.split("\n\n"):
        lines = [x for x in frame.splitlines() if x and not x.startswith(":")]
        if not lines:
            continue
        event = next(x[6:].strip() for x in lines if x.startswith("event:"))
        data = json.loads(next(x[5:].strip() for x in lines if x.startswith("data:")))
        out.append((event, data))
    return out
