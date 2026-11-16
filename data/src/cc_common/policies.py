"""Read cited passages from the static policy docs in seed/docs. Owner: Stream 0 (lead).

Policies are short markdown files. A citation is (doc, section heading); the passage is the first
paragraph under that `## ` heading. Used by notebooks/10 (traffic-shift rule), notebooks/18 (plan
policy checks) and scripts/make_frozen_fixture.py, so every screen quotes the same text.
"""
from __future__ import annotations

from pathlib import Path

DOCS_DIR = Path(__file__).resolve().parents[2] / "seed" / "docs"


def passage(doc: str, section: str, docs_dir: Path = DOCS_DIR) -> str:
    """Return the first paragraph under `## <section>` in seed/docs/<doc>. Raises KeyError if absent."""
    lines = (docs_dir / doc).read_text().splitlines()
    try:
        start = lines.index(f"## {section}") + 1
    except ValueError as exc:
        raise KeyError(f"{doc}: no section '## {section}'") from exc
    para: list[str] = []
    for line in lines[start:]:
        if line.startswith("#"):
            break
        if line.strip():
            para.append(line.strip())
        elif para:
            break
    if not para:
        raise KeyError(f"{doc}: section '{section}' is empty")
    return " ".join(para)


def citation(doc: str, section: str) -> dict:
    return {"doc": doc, "section": section, "passage": passage(doc, section)}
