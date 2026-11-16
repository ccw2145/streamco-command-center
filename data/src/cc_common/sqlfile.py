"""Run a .sql file from the bundle with validated {catalog}/{schema} placeholders. Owner: Stream 0 (lead).

SQL files use three literal tokens for identifiers only: {catalog}, {schema}, {gateway_model}.
Other braces (JSON, YAML, maps) are left untouched. Statements end with ';' at end of line, so keep
';' off line ends inside $$ ... $$ function bodies. User input is never
formatted into SQL; values that come from users go through parameter markers in the app.
"""
from __future__ import annotations

import re
from pathlib import Path

from .config import Config, validate_identifier

_SPLIT = re.compile(r";\s*\n")


def render(path: str | Path, cfg: Config) -> str:
    text = Path(path).read_text()
    tokens = {
        "{catalog}": validate_identifier(cfg.catalog, "catalog"),
        "{schema}": validate_identifier(cfg.schema, "schema"),
        "{gateway_model}": validate_identifier(cfg.gateway_model, "gateway_model"),
    }
    for token, value in tokens.items():
        text = text.replace(token, value)
    return text


def statements(sql: str) -> list[str]:
    """Split on statement terminators, dropping blank and comment-only chunks."""
    out = []
    for chunk in _SPLIT.split(sql + "\n"):
        body = "\n".join(line for line in chunk.splitlines() if not line.strip().startswith("--")).strip()
        if body:
            out.append(chunk.strip().rstrip(";"))
    return out


def run(spark, path: str | Path, cfg: Config) -> int:
    """Execute every statement in order; returns the number executed. Fails fast on the first error."""
    stmts = statements(render(path, cfg))
    for i, stmt in enumerate(stmts, 1):
        try:
            spark.sql(stmt)
        except Exception as exc:  # noqa: BLE001 - re-raise with the failing statement for the job log
            raise RuntimeError(f"{path}: statement {i}/{len(stmts)} failed:\n{stmt[:500]}") from exc
    return len(stmts)
