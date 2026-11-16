"""Settings from the environment (app.yaml when deployed, the shell locally)."""
from __future__ import annotations

import datetime as dt
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

APP_DIR = Path(__file__).resolve().parent.parent  # app/
FIXTURE_PATH = APP_DIR / "fixture.json"
FRONTEND_DIST = APP_DIR / "frontend" / "dist"
ET = ZoneInfo("America/New_York")
_IDENT = re.compile(r"^[A-Za-z0-9_.\-]*$")

# Game-night schedule (ET). The replay and every time window are derived from GAME_DATE.
CLOCK = {"t0": "18:00", "kickoff": "18:30", "halftime_start": "20:15", "halftime_end": "20:35", "end": "22:30"}


def _ident(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    if not _IDENT.match(value):
        raise ValueError(f"{name} has an invalid value: {value!r}")
    return value


@dataclass(frozen=True)
class Settings:
    catalog: str
    schema: str
    warehouse_id: str
    genie_agent_id: str
    agent_name: str
    gateway_model: str
    game_date: dt.date
    force_fixture: bool
    sql_timeout_s: float
    genie_timeout_s: float
    llm_timeout_s: float
    mlflow_trace_url: str
    lakebase_endpoint: str
    lakebase_database: str
    lakebase_serving_schema: str
    lakebase_app_schema: str

    @property
    def fq(self) -> str:
        return f"{self.catalog}.{self.schema}"

    @property
    def day_after(self) -> dt.date:
        return self.game_date + dt.timedelta(days=1)

    def clock(self) -> dict[str, dt.datetime]:
        """Game-night clock as aware ET datetimes."""
        out = {}
        for key, hhmm in CLOCK.items():
            h, m = (int(x) for x in hhmm.split(":"))
            out[key] = dt.datetime.combine(self.game_date, dt.time(h, m), tzinfo=ET)
        return out


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        catalog=_ident("CATALOG_NAME", "cindy_wu_catalog"),
        schema=_ident("SCHEMA_NAME", "streamco"),
        warehouse_id=_ident("DATABRICKS_WAREHOUSE_ID", ""),
        genie_agent_id=_ident("GENIE_AGENT_ID", ""),
        agent_name=_ident("AGENT_NAME", "agent-bricks-streamco-ask"),
        gateway_model=_ident("GATEWAY_MODEL", "system.ai.gpt-5-6-sol"),
        game_date=dt.date.fromisoformat(os.getenv("GAME_DATE", "2027-02-14")),
        force_fixture=os.getenv("FORCE_FIXTURE", "") in ("1", "true", "yes"),
        sql_timeout_s=float(os.getenv("SQL_TIMEOUT_S", "15")),
        genie_timeout_s=float(os.getenv("GENIE_TIMEOUT_S", "45")),
        llm_timeout_s=float(os.getenv("LLM_TIMEOUT_S", "20")),
        mlflow_trace_url=os.getenv("MLFLOW_TRACE_URL", "").strip(),
        lakebase_endpoint=os.getenv("LAKEBASE_ENDPOINT", "projects/streamco/branches/production/endpoints/primary").strip(),
        lakebase_database=_ident("LAKEBASE_DATABASE", "databricks_postgres"),
        lakebase_serving_schema=_ident("LAKEBASE_SERVING_SCHEMA", "streamco"),   # where the synced tables land
        lakebase_app_schema=_ident("LAKEBASE_APP_SCHEMA", "streamco_app"),        # created and owned by the app
    )
