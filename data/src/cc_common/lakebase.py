"""Notebook-side Lakebase (Autoscaling) access: connect, apply SQL files, upsert, reset. Owner: Stream D.

Spike S3: Autoscaling only. Credentials come from w.postgres.generate_database_credential(endpoint=...)
and last about an hour, so every notebook opens a fresh connection (no pool needed for batch work).
The app (app/backend/services/lakebase.py) uses the same tables with a refreshing pool.
"""
from __future__ import annotations

import datetime as dt
import json
import re
import uuid
from collections.abc import Iterable, Sequence
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from . import planted
from .config import Config

OPS_SCHEMA = "ops"
_PG_IDENT = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_SPLIT = re.compile(r";\s*\n")
DDL_FILE = Path(__file__).resolve().parents[2] / "sql" / "lakebase" / "001_ops_schema.sql"
SESSION_ID = "stage"
DEFAULT_SPEED = 60.0


def validate_pg_identifier(value: str) -> str:
    if not _PG_IDENT.match(value or ""):
        raise ValueError(f"Invalid Postgres identifier: {value!r}")
    return value


def workspace_client(w=None):
    if w is not None:
        return w
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def connect(cfg: Config, *, w=None, autocommit: bool = True) -> psycopg.Connection:
    """Fresh connection to the branch's primary endpoint as the identity running the notebook."""
    w = workspace_client(w)
    endpoint = w.postgres.get_endpoint(name=cfg.lakebase_endpoint)
    host = endpoint.status.hosts.host if endpoint.status and endpoint.status.hosts else None
    if not host:
        raise RuntimeError(f"Lakebase endpoint {cfg.lakebase_endpoint} has no host. Is the compute running?")
    cred = w.postgres.generate_database_credential(endpoint=cfg.lakebase_endpoint)
    return psycopg.connect(host=host, dbname=cfg.lakebase_database, user=w.current_user.me().user_name,
                           password=cred.token, sslmode="require", autocommit=autocommit,
                           application_name="command-center-notebook")


def ident(table: str, schema: str = OPS_SCHEMA) -> sql.Composed:
    return sql.SQL("{}.{}").format(sql.Identifier(validate_pg_identifier(schema)), sql.Identifier(table))


def render_sql_file(path: str | Path = DDL_FILE, ops_schema: str = OPS_SCHEMA) -> list[str]:
    """Statements of a Postgres .sql file with the validated {ops_schema} token substituted."""
    text = Path(path).read_text().replace("{ops_schema}", validate_pg_identifier(ops_schema))
    out = []
    for chunk in _SPLIT.split(text + "\n"):
        body = "\n".join(ln for ln in chunk.splitlines() if not ln.strip().startswith("--")).strip()
        if body:
            out.append(body.rstrip(";"))
    return out


def exec_sql_file(conn: psycopg.Connection, path: str | Path = DDL_FILE, ops_schema: str = OPS_SCHEMA) -> int:
    """Execute every statement in one transaction; returns the number executed."""
    stmts = render_sql_file(path, ops_schema)
    with conn.transaction(), conn.cursor() as cur:
        for i, stmt in enumerate(stmts, 1):
            try:
                cur.execute(stmt)
            except psycopg.Error as exc:
                raise RuntimeError(f"{path}: statement {i}/{len(stmts)} failed:\n{stmt[:400]}") from exc
    return len(stmts)


def _adapt(value):
    if isinstance(value, (dict, list)) and not isinstance(value, str):
        return Jsonb(value)
    return value


def upsert_rows(conn: psycopg.Connection, table: str, rows: Iterable[dict], key: Sequence[str],
                schema: str = OPS_SCHEMA, array_cols: Sequence[str] = ()) -> int:
    """INSERT ... ON CONFLICT (key) DO UPDATE for a list of dicts with the same keys. dict/list -> jsonb,
    except columns named in `array_cols` (Postgres arrays such as ask_cache.variants)."""
    rows = list(rows)
    if not rows:
        return 0
    cols = list(rows[0].keys())
    updates = [c for c in cols if c not in key]
    query = sql.SQL("INSERT INTO {t} ({cols}) VALUES ({vals}) ON CONFLICT ({key}) DO {action}").format(
        t=ident(table, schema),
        cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
        vals=sql.SQL(", ").join(sql.Placeholder() * len(cols)),
        key=sql.SQL(", ").join(map(sql.Identifier, key)),
        action=(sql.SQL("UPDATE SET ") + sql.SQL(", ").join(
            sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in updates))
        if updates else sql.SQL("NOTHING"),
    )
    params = [[r[c] if c in array_cols else _adapt(r[c]) for c in cols] for r in rows]
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(query, params)
    return len(rows)


def fetch_all(conn: psycopg.Connection, query, params=None) -> list[dict]:
    from psycopg.rows import dict_row

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, params)
        return cur.fetchall()


def clock_t0(cfg: Config) -> dt.datetime:
    hh, mm = (int(x) for x in planted.CLOCK["t0"].split(":"))
    day = dt.date.fromisoformat(cfg.game_date)
    return dt.datetime(day.year, day.month, day.day, hh, mm, tzinfo=ZoneInfo(planted.TZ))


def reset_t0(conn: psycopg.Connection, cfg: Config, *, actor: str = "notebooks/91_reset_t0",
             schema: str = OPS_SCHEMA) -> dict:
    """Same logic as POST /api/admin/reset: clear approvals, notes and the support queue; clock to T0.
    audit_log is append-only; a reset_t0 event is added."""
    t0 = clock_t0(cfg)
    counts = {}
    with conn.transaction(), conn.cursor() as cur:
        for table in ("offer_approvals", "traffic_shift_actions", "incident_notes", "support_context_queue"):
            cur.execute(sql.SQL("DELETE FROM {t}").format(t=ident(table, schema)))
            counts[table] = cur.rowcount
        cur.execute(sql.SQL(
            "INSERT INTO {t} (session_id, mode, t_sim, speed, paused, t0, updated_at) "
            "VALUES (%s, NULL, %s, %s, true, %s, now()) ON CONFLICT (session_id) DO UPDATE SET "
            "t_sim = EXCLUDED.t_sim, speed = EXCLUDED.speed, paused = true, t0 = EXCLUDED.t0, "
            "updated_at = EXCLUDED.updated_at").format(t=ident("replay_state", schema)),
            (SESSION_ID, t0, DEFAULT_SPEED, t0))
        cur.execute(sql.SQL("INSERT INTO {t} (event_id, event_type, entity_id, actor, payload, created_at) "
                            "VALUES (%s, 'reset_t0', %s, %s, %s, now())").format(t=ident("audit_log", schema)),
                    (uuid.uuid4(), SESSION_ID, actor, Jsonb({"deleted": counts, "source": "notebook"})))
    return {"t0": t0.isoformat(), "deleted": counts}


def to_json(value) -> str:
    return json.dumps(value, default=str, separators=(",", ":"))
