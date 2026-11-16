from conftest import sse_events

from backend import databricks as dbx
from backend import knowledge
from backend.routers import ask

QUESTION = "Why did Sports cancellations jump after the game?"


def test_recorded_run_streams_lanes_then_answer(client):
    r = client.post("/api/ask", json={"question": QUESTION})
    events = sse_events(r.text)
    names = [e for e, _ in events]
    assert names.count("lane_start") == names.count("lane_done") >= 5
    assert names[-2:] == ["answer", "done"]
    answer = events[-2][1]
    assert [round(d["pct"], 2) for d in answer["drivers"]] == [0.34, 0.29, 0.30, 0.07]
    assert events[-1][1] == {"recorded": True}


def test_unknown_question_declines_in_recorded_mode(client):
    events = sse_events(client.post("/api/ask", json={"question": "What is the weather?"}).text)
    assert events[0][0] == "answer" and events[0][1]["declined"] is True


def test_live_failure_replays_the_recording(live_client, monkeypatch):
    def down(*a, **k):
        raise ConnectionError("warehouse unreachable")

    monkeypatch.setattr(dbx, "run_sql", down)
    monkeypatch.setattr(dbx, "genie_ask", down)
    events = sse_events(live_client.post("/api/ask", json={"question": QUESTION}).text)
    names = [e for e, _ in events]
    assert "fallback" in names and names[-2:] == ["answer", "done"]
    assert events[-1][1] == {"recorded": True}


def test_followups_match_recorded_answers(client):
    r = client.post("/api/ask/followup", json={"question": "What about Originals?"}).json()
    assert r["declined"] is True and r["recorded"] is True and "Originals" in r["text"]
    r = client.post("/api/ask/followup", json={"question": "Show cancels by state"}).json()
    assert r["declined"] is False and r["columns"] == ["state", "cancels"]


def test_live_followup_goes_to_genie(live_client, monkeypatch):
    monkeypatch.setattr(dbx, "genie_ask", lambda q, conv=None, **k: {
        "conversation_id": "c1", "message_id": "m1", "text": "Texas led.", "sql": "SELECT 1",
        "columns": ["state", "cancels"], "rows": [["Texas", 10]], "duration_ms": 5})
    r = live_client.post("/api/ask/followup", json={"question": "Show cancels by state"}).json()
    assert r["recorded"] is False and r["conversation_id"] == "c1"


def test_summary_rejects_invented_numbers():
    facts = {"total": {"observed": 15800, "expected": 9000}, "drivers": [{"pct": 0.34}]}
    allowed = ask._allowed(facts)
    assert ask._nums("15,800 vs 9,000, 34% of it") <= allowed
    assert not ask._nums("about 12,345 more") <= allowed


def _stub_live(monkeypatch, *, recon_status="within_tolerance", agent=None):
    import datetime as dt

    def run_sql(stmt, params, **k):
        if "gold_anomaly_alerts" in stmt:
            return [{"alert_id": "A1", "detected_ts": dt.datetime(2027, 2, 15, 1, 19, tzinfo=dt.UTC), "cdn": "CDN-C",
                     "metro": "DFW", "device_family": "Roku", "app_version": "7.2.0", "observed": 0.105,
                     "baseline": 0.01, "persistence_min": 3, "affected_sessions": 210000, "resolved_ts": None}]
        if "gold_playback_failures_by_device" in stmt:
            return [{"cdn": "CDN-C", "metro": "DFW", "device_family": "Roku", "app_version": "7.2.0",
                     "sessions": 1, "rebuffer_ratio": 0.03}]
        if "gold_billing_failures" in stmt:
            return [{"acquirer": "B", "window_rate": 0.17, "normal_rate": 0.03, "top_decline_code": "processor_timeout",
                     "retry_rate": 0.0}]
        if "gold_contact_reasons" in stmt:
            return [{"reason_code": "stream_froze_halftime", "n_excess": 1364, "n_contacts": 2000},
                    {"reason_code": "charged_declined_cancelled", "n_excess": 1085, "n_contacts": 1500}]
        if "fn_cancel_drivers" in stmt:
            return [{"driver": d, "n_cancels": n, "pct": p, "evidence_type": "observed", "confidence": "high",
                     "evidence": "rule"} for d, n, p in (("playback", 2300, 0.34), ("payment", 2000, 0.29),
                                                         ("event_only", 2050, 0.30), ("unexplained", 450, 0.07))]
        if "gold_cancel_expected" in stmt:
            return [{"actual": 15800, "yhat": 9000.0, "lo": 8400.0, "hi": 9700.0}]
        if "gold_reconciliation_daily" in stmt:
            return [{"subscriber_live_cancels": 15800, "ledger_cancels": 15760, "pct_diff": 0.0025,
                     "status": recon_status}]
        raise AssertionError(stmt)

    monkeypatch.setattr(dbx, "run_sql", run_sql)

    def search(query, *, doc_type=None, tier=None, k=3):
        if doc_type == "policy":
            return [{"doc_type": "policy", "doc_title": "Payment retry policy", "section": "2. Decline codes without a retry",
                     "quote": "The decline code processor_timeout is not on the retry list.", "reason_code": None,
                     "tier": None, "score": 0.8}]
        return [{"doc_type": "transcript", "doc_title": "Support chat transcript",
                 "section": "charged, card declined, account cancelled", "quote": "Signed up for the game, got a payment error.",
                 "reason_code": "charged_declined_cancelled", "tier": "Sports", "score": 0.7}]

    monkeypatch.setattr(knowledge, "search", search)
    monkeypatch.setattr(knowledge, "transcript_count", lambda reason, tier: 965)
    monkeypatch.setattr(dbx, "genie_ask", lambda q, conv=None, **k: {
        "conversation_id": "c", "message_id": "m", "text": "Sports cancellations were 15,800.", "sql": "SELECT 1",
        "columns": ["cancels"], "rows": [[15800]], "duration_ms": 1})
    monkeypatch.setattr(dbx, "llm_json", lambda m, **k: {"headline": "Halftime explains most of the 6,800 extra.",
                                                        "narrative": "15,800 vs 9,000 expected; 34% and 29% of it."})
    if agent is None:
        def no_agent(*a, **k):
            raise RuntimeError("agent not deployed")
        monkeypatch.setattr(dbx, "agent_answer", no_agent)
    else:
        monkeypatch.setattr(dbx, "agent_answer", lambda q, **k: agent)


def test_live_run_uses_the_agents_words_and_the_queries_numbers(live_client, monkeypatch):
    _stub_live(monkeypatch, agent={"headline": "From the agent.", "narrative": "Agent narrative.", "declined": False})
    events = sse_events(live_client.post("/api/ask", json={"question": QUESTION}).text)
    lanes = [d for e, d in events if e == "lane_done"]
    assert len(lanes) == 6 and all(x["status"] == "ok" for x in lanes)
    answer = next(d for e, d in events if e == "answer")
    assert [c["doc_title"] for c in answer["citations"]] == ["Payment retry policy", "Support chat transcript"]
    assert answer["citations"][1]["n_conversations"] == 965
    assert answer["narrative"] == "Agent narrative." and answer["source"] == "agent"
    assert answer["headline"].startswith("Nearly two-thirds")          # computed from the drivers, not written
    assert answer["total"]["excess"] == 6800 and [d["pct"] for d in answer["drivers"]] == [0.34, 0.29, 0.30, 0.07]
    assert events[-1] == ("done", {"recorded": False})


def test_live_run_without_the_agent_uses_one_gateway_call(live_client, monkeypatch):
    _stub_live(monkeypatch)
    answer = next(d for e, d in sse_events(live_client.post("/api/ask", json={"question": QUESTION}).text) if e == "answer")
    assert answer["source"] == "live_llm" and answer["narrative"].startswith("15,800 vs 9,000")


def test_live_run_without_the_index_still_answers(live_client, monkeypatch):
    _stub_live(monkeypatch)

    def down(*a, **k):
        raise RuntimeError("index not found")

    monkeypatch.setattr(knowledge, "search", down)
    events = sse_events(live_client.post("/api/ask", json={"question": QUESTION}).text)
    answer = next(d for e, d in events if e == "answer")
    assert answer["citations"] == [] and [d["pct"] for d in answer["drivers"]] == [0.34, 0.29, 0.30, 0.07]
    assert next(d for e, d in events if e == "lane_done" and d["lane"] == "knowledge")["status"] == "error"


def test_live_run_declines_when_sources_disagree(live_client, monkeypatch):
    _stub_live(monkeypatch, recon_status="sources_disagree")
    answer = next(d for e, d in sse_events(live_client.post("/api/ask", json={"question": QUESTION}).text) if e == "answer")
    assert answer["declined"] is True and "can't give a confident" in answer["headline"]
