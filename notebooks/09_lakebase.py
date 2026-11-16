# Databricks notebook source
# MAGIC %md
# MAGIC # 09 Lakebase: operational serving
# MAGIC Syncs the per-subscriber outputs of 03 (churn scores) and 05 (personalized messages) from Unity Catalog into
# MAGIC the Lakebase project `streamco`, then checks the copy: row counts against the Delta source and a timed
# MAGIC one-subscriber lookup, the read the app's Save screen and a support tool make.
# MAGIC
# MAGIC Synced tables (snapshot mode) live in Postgres schema `streamco`; their Unity Catalog entries are
# MAGIC `cindy_wu_catalog.streamco.*_serving`. The app writes approvals to its own schema `streamco_app`.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.2" "databricks-sdk>=0.81"
# MAGIC %restart_python

# COMMAND ----------

import re
import time

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import SyncedTable, SyncedTableSyncedTableSpec

for k, v in {"catalog": "cindy_wu_catalog", "schema": "streamco", "project": "streamco",
             "branch": "production", "endpoint": "primary", "app_sp": ""}.items():
    dbutils.widgets.text(k, v)  # noqa: F821
W = {k: dbutils.widgets.get(k).strip() for k in ("catalog", "schema", "project", "branch", "endpoint", "app_sp")}  # noqa: F821
assert all(re.fullmatch(r"[A-Za-z0-9_\-]*", v) for v in W.values()), "invalid identifier"
BRANCH = f"projects/{W['project']}/branches/{W['branch']}"
ENDPOINT = f"{BRANCH}/endpoints/{W['endpoint']}"
TABLES = {"churn_scores": ["subscriber_id"], "personalized_messages": ["subscriber_id"]}
w = WorkspaceClient()

# COMMAND ----------
# 1. Create each synced table if it's missing; otherwise refresh its snapshot.

pipelines = {}
for source, pk in TABLES.items():
    name = f"{W['catalog']}.{W['schema']}.{source}_serving"
    try:
        st = w.postgres.get_synced_table(name=f"synced_tables/{name}")
        pipeline_id = st.status.pipeline_id if st.status else None
        if pipeline_id:
            w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=True)
        print(f"{name}: exists, snapshot refresh started (pipeline {pipeline_id})")
    except Exception as exc:  # noqa: BLE001 - not found: create it
        if "not found" not in str(exc).lower() and "does not exist" not in str(exc).lower():
            raise
        spec = SyncedTableSyncedTableSpec.from_dict({
            "source_table_full_name": f"{W['catalog']}.{W['schema']}.{source}", "primary_key_columns": pk,
            "scheduling_policy": "SNAPSHOT", "branch": BRANCH, "postgres_database": "databricks_postgres",
            "create_database_objects_if_missing": True,
            "new_pipeline_spec": {"storage_catalog": W["catalog"], "storage_schema": W["schema"]}})
        w.postgres.create_synced_table(synced_table=SyncedTable(spec=spec), synced_table_id=name).wait()
        st = w.postgres.get_synced_table(name=f"synced_tables/{name}")
        print(f"{name}: created")
    pipelines[name] = st.status.pipeline_id if st.status else None

# COMMAND ----------
# 2. Wait until every snapshot is online.

deadline = time.time() + 1800
for name in pipelines:
    while True:
        state = str(w.postgres.get_synced_table(name=f"synced_tables/{name}").status.detailed_state)
        if "ONLINE" in state or time.time() > deadline:
            break
        time.sleep(20)
    print(f"{name}: {state}")
    assert "ONLINE" in state, f"{name} did not come online"

# COMMAND ----------
# 3. Check the Postgres copy against the Delta source, then time one subscriber lookup.

host = w.postgres.get_endpoint(name=ENDPOINT).status.hosts.host
token = w.postgres.generate_database_credential(endpoint=ENDPOINT).token
user = w.current_user.me().user_name
with psycopg.connect(host=host, dbname="databricks_postgres", user=user, password=token, sslmode="require") as conn:
    for source in TABLES:
        delta = spark.table(f"{W['catalog']}.{W['schema']}.{source}").count()  # noqa: F821
        pg = conn.execute(f"SELECT count(*) FROM {W['schema']}.{source}_serving").fetchone()[0]
        print(f"{source}: Delta {delta:,} rows, Lakebase {pg:,} rows, match={delta == pg}")
        assert delta == pg, f"{source}: row counts differ"
    sample = spark.sql(f"SELECT first_name, subscriber_id FROM {W['catalog']}.{W['schema']}.ref_sample_subscribers")  # noqa: F821
    for row in sample.collect():
        t0 = time.perf_counter()
        r = conn.execute(
            f"SELECT s.churn_prob, s.risk_decile, s.card_id, m.subject FROM {W['schema']}.churn_scores_serving s "
            f"LEFT JOIN {W['schema']}.personalized_messages_serving m USING (subscriber_id) WHERE s.subscriber_id = %s",
            (row.subscriber_id,)).fetchone()
        ms = (time.perf_counter() - t0) * 1000
        print(f"{row.first_name}: churn {r[0]:.2f}, decile {r[1]}, card {r[2]}, message '{r[3]}' ({ms:.1f} ms)")
    # A snapshot refresh can recreate the Postgres tables, so re-apply the app's read access each run (approved grant;
    # the app's service principal id is the widget app_sp, from `databricks apps get streamco-command-center`).
    if W["app_sp"]:
        conn.execute(f'GRANT USAGE ON SCHEMA {W["schema"]} TO "{W["app_sp"]}"')
        conn.execute(f'GRANT SELECT ON {W["schema"]}.churn_scores_serving, {W["schema"]}.personalized_messages_serving '
                     f'TO "{W["app_sp"]}"')
        print(f"read access granted to the app role {W['app_sp']}")
