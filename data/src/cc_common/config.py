"""Job-parameter access and identifier validation. Owner: Stream 0 (lead)."""
from __future__ import annotations

import re
from dataclasses import dataclass

_IDENT = re.compile(r"^[A-Za-z0-9_.\-]+$")

PARAMS = (
    "catalog", "schema", "warehouse_id", "lakebase_project", "lakebase_branch", "lakebase_database", "gateway_model",
    "app_name", "agent_name", "genie_agent_name", "game_date", "seed", "arpu_monthly",
    "retention_months", "scale_subscribers",
)
_FREE_TEXT = {"genie_agent_name"}  # display names are never interpolated into SQL

# Spike S1: system.ai.gpt-5-6-sol returns HTTP 400 for function tools unless reasoning is off.
# Pass these on EVERY tool-using call (LangGraph: ChatDatabricks(..., extra_params=GATEWAY_TOOL_CALL_PARAMS)).
GATEWAY_TOOL_CALL_PARAMS = {"reasoning_effort": "none"}


def validate_identifier(value: str, name: str = "identifier") -> str:
    """Reject anything that is not a plain catalog/schema/model identifier."""
    if not value or not _IDENT.match(value):
        raise ValueError(f"Invalid {name}: {value!r}")
    return value


@dataclass(frozen=True)
class Config:
    catalog: str
    schema: str
    warehouse_id: str
    lakebase_project: str
    lakebase_branch: str
    lakebase_database: str
    gateway_model: str
    app_name: str
    agent_name: str
    genie_agent_name: str
    game_date: str
    seed: int
    arpu_monthly: float
    retention_months: int
    scale_subscribers: int

    def fq(self, name: str) -> str:
        """Fully qualified UC name for a table, view, function, volume or model in the demo schema."""
        return f"{self.catalog}.{self.schema}.{validate_identifier(name, 'object name')}"

    @property
    def lakebase_endpoint(self) -> str:
        """Autoscaling endpoint name for w.postgres.get_endpoint / generate_database_credential (spike S3)."""
        return f"projects/{self.lakebase_project}/branches/{self.lakebase_branch}/endpoints/primary"

    def volume_path(self, volume: str, *parts: str) -> str:
        return "/".join([f"/Volumes/{self.catalog}/{self.schema}/{validate_identifier(volume, 'volume')}", *parts])


# StreamCo defaults: every parameter has one, so a notebook also runs outside the job. Lakebase is not used.
DEFAULTS = {
    "catalog": "cindy_wu_catalog", "schema": "streamco", "warehouse_id": "<warehouse-id>",
    "lakebase_project": "none", "lakebase_branch": "none", "lakebase_database": "none",
    "gateway_model": "system.ai.gpt-5-6-sol", "app_name": "streamco-command-center", "agent_name": "streamco-ask",
    "genie_agent_name": "StreamCo Subscriber Insights", "game_date": "2027-02-14", "seed": "20270214",
    "arpu_monthly": "11.99", "retention_months": "12", "scale_subscribers": "4000000",
}


def load(dbutils) -> Config:
    """Read job parameters (pushed to tasks as widgets), falling back to DEFAULTS; validate the identifiers."""
    raw = {}
    for key in PARAMS:
        try:
            raw[key] = dbutils.widgets.get(key) or DEFAULTS[key]
        except Exception:  # noqa: BLE001 - widget API raises a generic error when missing
            raw[key] = DEFAULTS[key]
    for key, value in raw.items():
        if key not in _FREE_TEXT and key not in {"game_date"}:
            validate_identifier(value, key)
    return Config(
        catalog=raw["catalog"], schema=raw["schema"], warehouse_id=raw["warehouse_id"],
        lakebase_project=raw["lakebase_project"], lakebase_branch=raw["lakebase_branch"],
        lakebase_database=raw["lakebase_database"],
        gateway_model=raw["gateway_model"], app_name=raw["app_name"], agent_name=raw["agent_name"],
        genie_agent_name=raw["genie_agent_name"], game_date=raw["game_date"], seed=int(raw["seed"]),
        arpu_monthly=float(raw["arpu_monthly"]), retention_months=int(raw["retention_months"]),
        scale_subscribers=int(raw["scale_subscribers"]),
    )
