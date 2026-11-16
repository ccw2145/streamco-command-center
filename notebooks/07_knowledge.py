# Databricks notebook source
# StreamCo Command Center 07: the knowledge index the GenAI layer retrieves and cites.
# knowledge_chunks (Delta, change data feed on) holds
#   - doc_type 'policy':     one chunk per section of the four policy docs in data/seed/docs (doc_title, section, text)
#   - doc_type 'transcript': one chunk per support conversation, masked text only (fn_redact_pii),
#                            kids-only accounts excluded, with the classified reason code and the tier.
# knowledge_idx is a Delta Sync index on the existing AI Search endpoint (no endpoint is created here). Embeddings
# are computed through Unity Gateway (ai_query on the system.ai embedding model service) into the `embedding`
# column and synced as a self-managed vector column: this workspace serves models only through Unity Gateway, and
# managed-embedding indexes don't accept Gateway-governed models yet. Queries embed the question the same way. knowledge_citations is the offer-policy passage retrieved for each save-plan card, used by
# 05_personalize (prompt input) and the Save screen.
# Fails on PII patterns, brand-term leaks or kids-only rows in the chunks.

# COMMAND ----------

import datetime as dt
import json
import os
import re
import time

from databricks.sdk import WorkspaceClient

for k, v in {"catalog": "cindy_wu_catalog", "schema": "streamco", "src_schema": "streamco",
             "endpoint": "telco-vs-endpoint", "embedding_model": "system.ai.gte-large-en",
             "docs_dir": "../data/seed/docs"}.items():
    dbutils.widgets.text(k, v)  # noqa: F821
W = {k: dbutils.widgets.get(k).strip() for k in ("catalog", "schema", "src_schema", "endpoint", "embedding_model", "docs_dir")}  # noqa: F821
for k in ("catalog", "schema", "src_schema"):
    assert re.fullmatch(r"[A-Za-z0-9_]+", W[k]), f"invalid {k}"
assert re.fullmatch(r"[A-Za-z0-9_\-]+", W["endpoint"]) and re.fullmatch(r"[A-Za-z0-9_.\-]+", W["embedding_model"])
EMBED_DIM = 1024  # gte-large-en
# The Gateway returns unnormalized vectors; unit length makes the index's L2 ranking equal cosine ranking.
unit = lambda e: f"transform({e}, x -> CAST(x / sqrt(aggregate({e}, 0D, (a, y) -> a + y * y)) AS FLOAT))"  # noqa: E731
sqnorm = lambda e: f"aggregate({e}, 0D, (a, y) -> a + y * y)"  # noqa: E731
CATALOG, SCHEMA, SRC = W["catalog"], W["schema"], W["src_schema"]
fq = lambda t: f"{CATALOG}.{SCHEMA}.{t}"  # noqa: E731
src = lambda t: f"{CATALOG}.{SRC}.{t}"  # noqa: E731
CHUNKS, INDEX, CITATIONS = fq("knowledge_chunks"), fq("knowledge_idx"), fq("knowledge_citations")
w = WorkspaceClient()

# COMMAND ----------
# Policy docs: one chunk per "## n. Section". The HTML owner comment and "(synthetic)" are dropped.

docs_dir = os.path.abspath(W["docs_dir"])
policy_rows = []
for name in sorted(f for f in os.listdir(docs_dir) if f.endswith(".md")):
    raw = re.sub(r"<!--.*?-->", "", open(os.path.join(docs_dir, name)).read(), flags=re.DOTALL).strip()
    title = re.search(r"^# (.+)$", raw, re.M).group(1).replace("(synthetic)", "").strip()
    title = title[0].upper() + title[1:]
    for m in re.finditer(r"^## (.+?)\n(.*?)(?=^## |\Z)", raw, re.M | re.DOTALL):
        section, text = m.group(1).strip(), " ".join(m.group(2).split())
        if text:
            policy_rows.append((f"policy:{name[:-3]}:{section.split('.')[0]}", "policy", title, section, text,
                                f"{title}. {section}. {text}", None, None, None, None, name))
print(f"policy chunks: {len(policy_rows)} from {len({r[2] for r in policy_rows})} docs")
assert len({r[2] for r in policy_rows}) == 4, "expected the four policy docs"

spark.createDataFrame(policy_rows, "chunk_id STRING, doc_type STRING, doc_title STRING, section STRING, text STRING, "  # noqa: F821
                      "embed_text STRING, reason_code STRING, tier STRING, contact_day DATE, channel STRING, "
                      "source_file STRING").createOrReplaceTempView("k_policy")

# COMMAND ----------
# Support transcripts: masked text only, kids-only accounts out (v_subscribers' row filter, plus an explicit check).

REASON_TITLES = {"charged_declined_cancelled": "charged, card declined, account cancelled",
                 "stream_froze_halftime": "stream froze at halftime", "cancel_request_other": "cancel request",
                 "billing_question": "billing question", "login_issue": "login issue", "content_question": "content question",
                 "playback_general": "playback problem", "device_setup": "device setup", "account_update": "account update",
                 "promo_question": "promotion question"}
reason_case = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in REASON_TITLES.items())
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW k_transcripts AS
SELECT concat('transcript:', CAST(c.contact_id AS STRING)) AS chunk_id, 'transcript' AS doc_type,
       concat('Support ', c.channel, ' transcript') AS doc_title,
       CASE r.reason_code {reason_case} ELSE r.reason_code END AS section,
       m.text, concat('Support conversation (', s.tier, ', ', r.reason_code, '). ', m.text) AS embed_text,
       r.reason_code, s.tier, to_date(c.contact_ts) AS contact_day, c.channel, CAST(NULL AS STRING) AS source_file
FROM {src('fact_support_contacts')} c
JOIN {src('silver_contact_reasons')} r USING (contact_id)
JOIN {fq('v_subscribers')} s USING (subscriber_id)
JOIN {src('dim_subscribers')} d USING (subscriber_id)
JOIN (SELECT contact_id, {src('fn_redact_pii')}(transcript_text) AS text
      FROM {src('fact_support_contacts')} WHERE transcript_text IS NOT NULL) m USING (contact_id)
WHERE NOT d.is_kids_only_account
""")  # noqa: F821
spark.sql("CREATE OR REPLACE TEMP VIEW k_all AS SELECT * FROM k_policy UNION ALL SELECT * FROM k_transcripts")  # noqa: F821

# Guards before anything is written.
bad = spark.sql("""SELECT
  count_if(text RLIKE '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}') AS emails,
  count_if(text RLIKE '\\\\(?[0-9]{3}\\\\)?[ .-][0-9]{3}[ .-][0-9]{4}') AS phones,
  count_if(text RLIKE '[0-9]{4}[ -]?[0-9]{4}[ -]?[0-9]{4}') AS cards,
  count(*) AS n FROM k_all""").first()  # noqa: F821
print(bad.asDict())
assert bad.emails == bad.phones == bad.cards == 0, f"PII in the chunks: {bad.asDict()}"

# COMMAND ----------
# knowledge_chunks: MERGE (not overwrite) so the Delta Sync index sees row-level changes through the change feed.

spark.sql(f"""CREATE TABLE IF NOT EXISTS {CHUNKS} (
  chunk_id STRING NOT NULL, doc_type STRING, doc_title STRING, section STRING, text STRING, embed_text STRING,
  reason_code STRING, tier STRING, contact_day DATE, channel STRING, source_file STRING, embedding ARRAY<FLOAT>)
TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'Knowledge for the GenAI layer: policy-doc sections and masked support transcripts (kids-only accounts excluded, no PII). Source of the AI Search index knowledge_idx.'""")  # noqa: F821
spark.sql(f"ALTER TABLE {CHUNKS} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")  # noqa: F821
if "embedding" not in spark.table(CHUNKS).columns:  # noqa: F821 - tables from before the embedding column
    spark.sql(f"ALTER TABLE {CHUNKS} ADD COLUMNS (embedding ARRAY<FLOAT>)")  # noqa: F821

# Embed only new or changed chunks (one Unity Gateway call per chunk, batched by ai_query); reuse the rest.
started = time.monotonic()
spark.sql(f"""CREATE OR REPLACE TABLE {fq('tmp_knowledge_stage')} AS
SELECT s.*, t.embedding
FROM k_all s JOIN {CHUNKS} t ON s.chunk_id = t.chunk_id AND s.embed_text = t.embed_text
WHERE t.embedding IS NOT NULL
UNION ALL
SELECT s.*, ai_query('{W["embedding_model"]}', s.embed_text, returnType => 'ARRAY<FLOAT>') AS embedding
FROM k_all s LEFT ANTI JOIN (SELECT * FROM {CHUNKS} WHERE embedding IS NOT NULL) t
  ON s.chunk_id = t.chunk_id AND s.embed_text = t.embed_text""")  # noqa: F821
st = spark.sql(f"""SELECT count(*) AS n, count_if(embedding IS NULL OR size(embedding) <> {EMBED_DIM}) AS bad
                   FROM {fq('tmp_knowledge_stage')}""").first()  # noqa: F821
print(f"staged {st.n:,} chunks in {(time.monotonic() - started) / 60:.1f} min")
assert st.bad == 0, f"{st.bad} chunks have no {EMBED_DIM}-d embedding"
# Normalized from the materialized stage, so each chunk costs exactly one embedding call.
spark.sql(f"""MERGE INTO {CHUNKS} t
USING (SELECT * EXCEPT (embedding), {unit('embedding')} AS embedding FROM {fq('tmp_knowledge_stage')}) s
ON t.chunk_id = s.chunk_id
WHEN MATCHED AND (t.text <> s.text OR t.doc_title <> s.doc_title OR t.section <> s.section
                  OR NOT (t.reason_code <=> s.reason_code) OR NOT (t.tier <=> s.tier) OR t.embed_text <> s.embed_text
                  OR t.embedding IS NULL OR abs({sqnorm('t.embedding')} - 1) > 1e-3)
  THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE""")  # noqa: F821
spark.sql(f"DROP TABLE IF EXISTS {fq('tmp_knowledge_stage')}")  # noqa: F821
counts = {r.doc_type: r.n for r in spark.sql(f"SELECT doc_type, count(*) AS n FROM {CHUNKS} GROUP BY 1").collect()}  # noqa: F821
print("knowledge_chunks:", counts)
kids = spark.sql(f"""SELECT count(*) AS n FROM {CHUNKS} k JOIN {src('fact_support_contacts')} c
                     ON k.chunk_id = concat('transcript:', CAST(c.contact_id AS STRING))
                     JOIN {src('dim_subscribers')} d USING (subscriber_id) WHERE d.is_kids_only_account""").first().n  # noqa: F821
assert kids == 0, "a kids-only account's transcript reached knowledge_chunks"

# COMMAND ----------
# The index: Delta Sync, managed embeddings, on the existing endpoint. Created once; later runs trigger a sync.

ep = w.vector_search_endpoints.get_endpoint(W["endpoint"])
assert ep.endpoint_status and ep.endpoint_status.state.value == "ONLINE", f"endpoint {W['endpoint']} is not ONLINE"
try:
    w.vector_search_indexes.get_index(INDEX)
    exists = True
except Exception as exc:  # noqa: BLE001 - NotFound
    if "not found" not in str(exc).lower() and "does not exist" not in str(exc).lower():
        raise
    exists = False
if not exists:  # REST body, so the notebook doesn't depend on the runtime's SDK version
    w.api_client.do("POST", "/api/2.0/vector-search/indexes", body={
        "name": INDEX, "endpoint_name": W["endpoint"], "primary_key": "chunk_id", "index_type": "DELTA_SYNC",
        "delta_sync_index_spec": {
            "source_table": CHUNKS, "pipeline_type": "TRIGGERED",
            "columns_to_sync": ["chunk_id", "doc_type", "doc_title", "section", "text", "reason_code", "tier",
                                "contact_day", "channel", "embedding"],
            "embedding_vector_columns": [{"name": "embedding", "embedding_dimension": EMBED_DIM}]}})
    print(f"created {INDEX} on {W['endpoint']} ({W['embedding_model']})")
synced_after = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=5)
if exists:
    for _ in range(60):  # a sync can't start while the first build is still running
        try:
            w.vector_search_indexes.sync_index(INDEX)
            print(f"sync triggered for {INDEX}")
            break
        except Exception as exc:  # noqa: BLE001
            print(f"sync not started yet: {str(exc)[:160]}")
            time.sleep(30)

# Wait for the sync pipeline's latest update to finish: the row count alone is already full while a sync rewrites
# vectors, and querying mid-sync returns a mix of old and new embeddings.
total = sum(counts.values())
deadline = time.monotonic() + 90 * 60
while True:  # REST, so the loop doesn't depend on the runtime's SDK version
    idx = w.api_client.do("GET", f"/api/2.0/vector-search/indexes/{INDEX}")
    st = idx.get("status") or {}
    n = st.get("indexed_row_count") or 0
    pid = (idx.get("delta_sync_index_spec") or {}).get("pipeline_id")
    updates = (w.api_client.do("GET", f"/api/2.0/pipelines/{pid}").get("latest_updates") or []) if pid else []
    fresh = [u for u in updates if u.get("creation_time")
             and dt.datetime.fromisoformat(u["creation_time"].replace("Z", "+00:00")) >= synced_after]
    update = fresh[0].get("state", "UNKNOWN") if fresh else ("WAITING" if exists else "NONE")
    print(f"{dt.datetime.now():%H:%M:%S} ready={st.get('ready')} indexed={n:,}/{total:,} sync={update}")
    if st.get("ready") and n >= total and update in ("COMPLETED", "NONE"):  # NONE: a new index's first build
        break
    if update in ("FAILED", "CANCELED"):
        raise RuntimeError(f"index sync {update}: {(st.get('message') or '')[:300]}")
    if time.monotonic() > deadline:
        raise TimeoutError(f"{INDEX} not fully indexed after 90 min: {n}/{total}")
    time.sleep(30)

# COMMAND ----------
# Retrieval checks, then the offer-policy passage for each save-plan card.

COLS = ["chunk_id", "doc_type", "doc_title", "section", "text", "reason_code", "tier"]


def embed(text: str) -> list[float]:
    """The query embedding, through Unity Gateway (the same model service the chunks were embedded with)."""
    r = w.api_client.do("POST", "/ai-gateway/mlflow/v1/embeddings", body={"model": W["embedding_model"], "input": [text]})
    v = r["data"][0]["embedding"]
    n = sum(x * x for x in v) ** 0.5 or 1.0
    return [x / n for x in v]


def search(query: str, filters: dict, k: int = 3) -> list[dict]:
    r = w.vector_search_indexes.query_index(index_name=INDEX, columns=COLS, query_vector=embed(query), num_results=k,
                                            filters_json=json.dumps(filters))
    names = [c.name for c in r.manifest.columns]
    return [dict(zip(names, row)) for row in (r.result.data_array or [])]


for q, f in [("sign-up payment declined processor timeout retry grace window", {"doc_type": "policy"}),
             ("I was charged, my card was declined and my account was cancelled", {"doc_type": "transcript", "tier": "Sports"}),
             ("stream froze during halftime", {"doc_type": "transcript"})]:
    hits = search(q, f)
    print(f"\n{q}")
    for h in hits:
        print(f"  {h['score']:.3f} {h['doc_title']} / {h['section']}: {h['text'][:110]}")
    assert hits, f"no hits for {q!r}"

CARD_QUERIES = {
    "payment": "offer for subscribers who cancelled because a payment failed (involuntary cancellation)",
    "playback": "offer for subscribers with a verified service failure such as buffering",
    "onboarding": "offer for new event sign-ups in their first days",
}
plan_cards = [r.card_id for r in spark.table(fq("save_plan")).select("card_id").collect()]  # noqa: F821
cites = []
for card in plan_cards:
    if card not in CARD_QUERIES:
        print(f"no retrieval query for card {card!r}; skipped")
        continue
    hits = search(CARD_QUERIES[card], {"doc_type": "policy", "doc_title": "Save-offer policy"}, k=1)
    assert hits, f"no offer-policy passage for {card}"
    h = hits[0]
    cites.append((card, h["doc_title"], f"{h['section']}: {h['text']}", float(h["score"])))
    print(f"{card:<11} {h['score']:.3f} {h['doc_title']} / {h['section']}: {h['text']}")
assert len({c[2] for c in cites}) == len(cites), f"two cards retrieved the same passage: {cites}"
spark.createDataFrame(cites, "card_id STRING, doc_title STRING, passage STRING, score DOUBLE").write.mode(  # noqa: F821
    "overwrite").option("overwriteSchema", "true").saveAsTable(CITATIONS)
spark.sql(f"COMMENT ON TABLE {CITATIONS} IS 'The Save-offer policy passage retrieved from knowledge_idx for each "  # noqa: F821
          "save-plan card. 05_personalize puts it in every message prompt.'")

# COMMAND ----------

summary = {"index": INDEX, "endpoint": W["endpoint"], "embedding_model": W["embedding_model"], "chunks": counts,
           "citations": {c[0]: f"{c[1]} {c[2][:60]}" for c in cites}}
print(json.dumps(summary, indent=1))
display(spark.sql(f"SELECT doc_type, tier, reason_code, count(*) AS n FROM {CHUNKS} GROUP BY ALL ORDER BY n DESC"))  # noqa: F821
dbutils.notebook.exit(json.dumps(summary))  # noqa: F821
