"""AI Search client: retrieve passages from the knowledge index (policy docs and masked support transcripts).

The index is {catalog}.{schema}.knowledge_idx (KNOWLEDGE_INDEX overrides it), built by notebooks/07_knowledge with
Unity Gateway embeddings (EMBEDDING_MODEL, default system.ai.gte-large-en); queries are embedded the same way.
Its source table, knowledge_chunks, holds masked transcript text only (kids-only accounts excluded), so nothing
returned here carries personal data; quote_of() also drops any sentence with a mask placeholder. Blocking calls;
callers run them through fallback.call() with a timeout.
"""
from __future__ import annotations

import json
import os
import re

import requests

from .config import get_settings
from .databricks import run_sql, workspace

COLUMNS = ["chunk_id", "doc_type", "doc_title", "section", "text", "reason_code", "tier"]
_PLACEHOLDER = re.compile(r"\[(EMAIL|PHONE|CARD|ADDRESS|NAME)\]")
_IDENT = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")
SQL_COUNTS = ("SELECT reason_code, tier, count(*) AS n_conversations FROM knowledge_chunks "
              "WHERE doc_type = 'transcript' AND reason_code = :reason_code AND tier = :tier GROUP BY reason_code, tier")


def index_name() -> str:
    s = get_settings()
    name = os.getenv("KNOWLEDGE_INDEX", "").strip() or f"{s.fq}.knowledge_idx"
    if not _IDENT.match(name):
        raise ValueError(f"KNOWLEDGE_INDEX has an invalid value: {name!r}")
    return name


def quote_of(hit: dict, max_chars: int = 220) -> str:
    """A short quote: a policy section as written; for a transcript, the customer's first clean sentence."""
    text = str(hit.get("text") or "")
    if hit.get("doc_type") == "transcript":
        lines = [ln.split(":", 1)[1].strip() for ln in text.splitlines() if ln.lower().startswith("customer:")]
        sentences = [x for ln in lines for x in re.split(r"(?<=[.!?])\s+", ln) if x and not _PLACEHOLDER.search(x)]
        text = sentences[0] if sentences else ""
    text = _PLACEHOLDER.sub("", text).strip()
    return text if len(text) <= max_chars else text[: max_chars - 3].rsplit(" ", 1)[0] + "..."


def embedding_model() -> str:
    name = os.getenv("EMBEDDING_MODEL", "").strip() or "system.ai.gte-large-en"
    if not re.match(r"^[A-Za-z0-9_.\-]+$", name):
        raise ValueError(f"EMBEDDING_MODEL has an invalid value: {name!r}")
    return name


def embed(text: str) -> list[float]:
    """The query vector, from the same Unity Gateway embedding model that embedded the chunks (07_knowledge)."""
    w = workspace()
    r = requests.post(w.config.host.rstrip("/") + "/ai-gateway/mlflow/v1/embeddings",
                      headers={**w.config.authenticate(), "Content-Type": "application/json"},
                      json={"model": embedding_model(), "input": [text]}, timeout=get_settings().llm_timeout_s)
    r.raise_for_status()
    v = r.json()["data"][0]["embedding"]
    n = sum(x * x for x in v) ** 0.5 or 1.0      # the chunks are stored at unit length (07_knowledge)
    return [x / n for x in v]


def search(query: str, *, doc_type: str | None = None, tier: str | None = None, k: int = 3) -> list[dict]:
    """Top-k passages: {doc_type, doc_title, section, quote, reason_code, tier, score}."""
    filters = {key: v for key, v in (("doc_type", doc_type), ("tier", tier)) if v}
    r = workspace().vector_search_indexes.query_index(
        index_name=index_name(), columns=COLUMNS, query_vector=embed(query), num_results=k,
        filters_json=json.dumps(filters) if filters else None)
    names = [c.name for c in (r.manifest.columns or [])] if r.manifest else []
    hits = [dict(zip(names, row)) for row in ((r.result.data_array if r.result else None) or [])]
    return [{"doc_type": h.get("doc_type"), "doc_title": h.get("doc_title"), "section": h.get("section"),
             "quote": quote_of(h), "reason_code": h.get("reason_code"), "tier": h.get("tier"),
             "score": round(float(h.get("score") or 0), 4)} for h in hits]


def transcript_count(reason_code: str, tier: str) -> int:
    """How many masked support conversations carry this reason code for this tier (governed count, not retrieval)."""
    rows = run_sql(SQL_COUNTS, {"reason_code": reason_code, "tier": tier})
    return int(rows[0]["n_conversations"]) if rows else 0
