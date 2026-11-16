# Grants pending approval

None of these have been run. Each one needs Cindy's approval. Until they are granted, the fallbacks hold: the app
serves `app/fixture.json` and the recorded Ask run in `ask_recorded_run`, and the Ask investigation and Genie
follow-ups run in-process as the presenter.

- Catalog `cindy_wu_catalog`, schema `streamco`.
- Warehouse `<warehouse-id>`.
- Genie Agent "StreamCo Subscriber Insights": `01f1dbf923701cb3b3ec3d2fcbee9e87`, from `notebooks/04_genie.py`.

## 1. Ask agent on Agent Runtime: app `agent-bricks-streamco-ask`

Deployed 2026-11-09. Service principal (application id): `<agent-service-principal-id>`.
Status: the deployed agent answers, and its model call works. Both tools fail with access denied, so it declines.

```sql
GRANT USE CATALOG ON CATALOG cindy_wu_catalog TO `<agent-service-principal-id>`;
GRANT USE SCHEMA ON SCHEMA cindy_wu_catalog.streamco TO `<agent-service-principal-id>`;
GRANT EXECUTE ON FUNCTION cindy_wu_catalog.streamco.fn_cancel_drivers TO `<agent-service-principal-id>`;
-- Tables the function and the Genie Agent read:
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.gold_reconciliation_daily TO `<agent-service-principal-id>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.gold_cancel_expected TO `<agent-service-principal-id>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.gold_cancel_decomposition TO `<agent-service-principal-id>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.gold_team_cancel_counts TO `<agent-service-principal-id>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.gold_cancellations_daily TO `<agent-service-principal-id>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.gold_billing_failures TO `<agent-service-principal-id>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.churn_scores TO `<agent-service-principal-id>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.save_plan TO `<agent-service-principal-id>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.mv_streamco_metrics TO `<agent-service-principal-id>`;
```

```bash
databricks --profile my-workspace api patch /api/2.0/permissions/genie/01f1dbf923701cb3b3ec3d2fcbee9e87 \
  --json '{"access_control_list":[{"service_principal_name":"<agent-service-principal-id>","permission_level":"CAN_RUN"}]}'
databricks --profile my-workspace api patch /api/2.0/permissions/sql/warehouses/<warehouse-id> \
  --json '{"access_control_list":[{"service_principal_name":"<agent-service-principal-id>","permission_level":"CAN_USE"}]}'
```

The model service `system.ai.gpt-5-6-sol` already answers for this service principal, so no model grant is needed.

Already done by the tool: `agentbricks deploy` ran its built-in step "Granting the agent runtime access to its trace
experiment", even though agent.toml binds no tracing. No other grant was made. To review it, check the permissions
on the agent's trace experiment.

## 2. Command Center app (`resources/app.yml`, `<APP_SP>` from `databricks apps get <app-name>`)

Run the `USE CATALOG`, `USE SCHEMA`, `EXECUTE` and `SELECT` grants from section 1 for `<APP_SP>`, plus the Genie
`CAN_RUN` and warehouse `CAN_USE` commands. Then add:

```sql
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.ref_source_systems TO `<APP_SP>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.gold_anomaly_alerts TO `<APP_SP>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.gold_cdn_region_minute TO `<APP_SP>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.gold_playback_failures_by_device TO `<APP_SP>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.gold_contact_reasons TO `<APP_SP>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.ref_sample_subscribers TO `<APP_SP>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.forecast_viewers TO `<APP_SP>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.forecast_flags TO `<APP_SP>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.churn_model_card TO `<APP_SP>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.personalized_messages TO `<APP_SP>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.personalization_run TO `<APP_SP>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.ask_recorded_run TO `<APP_SP>`;
GRANT SELECT ON TABLE cindy_wu_catalog.streamco.v_support_showcase TO `<APP_SP>`;
-- The app writes approvals (it creates the table on first Approve, or create it first and grant MODIFY):
GRANT CREATE TABLE ON SCHEMA cindy_wu_catalog.streamco TO `<APP_SP>`;
-- Live Gateway usage tile in the drawer:
GRANT USE CATALOG ON CATALOG system TO `<APP_SP>`;
GRANT USE SCHEMA ON SCHEMA system.ai_gateway TO `<APP_SP>`;
GRANT SELECT ON TABLE system.ai_gateway.usage TO `<APP_SP>`;
```

The app also needs to call the deployed agent:

```bash
databricks --profile my-workspace api patch /api/2.0/permissions/apps/agent-bricks-streamco-ask \
  --json '{"access_control_list":[{"service_principal_name":"<APP_SP>","permission_level":"CAN_USE"}]}'
```

The Regenerate button calls `system.ai.gpt-5-6-sol`, so `<APP_SP>` needs the same model-service access as the agent.

If reading `v_support_showcase` fails with a function permission error, also grant `EXECUTE` on
`cindy_wu_catalog.streamco.fn_redact_pii` and `fn_count_redactions`.

## 3. Account group `cc_privileged`

Create the group in the account console. The views mask PII for everyone outside it. Leave it empty unless
someone needs to see clear values in `v_subscribers` and `v_support_showcase`.

## 4. Bundle deployment

`databricks bundle deploy` was not run. The pipelines `streamco-data-stream` and `streamco-data-batch` were created
through the Pipelines API from workspace files under `/Workspace/Users/<user>/streamco-build/`.
Before the first `bundle deploy`, bind them so the bundle doesn't create duplicates:

```bash
databricks bundle deployment bind streamco_data_stream <stream_pipeline_id>
databricks bundle deployment bind streamco_data_batch <batch_pipeline_id>
```

## 5. Command Center app: additions from the app builder

These add to section 2 for `<APP_SP>` (app `streamco-command-center`). Not run.

- The Ask lanes also read `gold_cancel_decomposition` (fallback when `fn_cancel_drivers` errors); it is in section 1.
- Approve runs `CREATE TABLE IF NOT EXISTS approvals` then `INSERT`. With `CREATE TABLE` on the schema (section 2)
  the app owns the table it creates. If you create `approvals` yourself instead, grant:
  ```sql
  GRANT MODIFY, SELECT ON TABLE cindy_wu_catalog.streamco.approvals TO `<APP_SP>`;
  ```
  (It already exists: created locally by the app running as cindy.wu on its first live Approve.)
- The app calls the Ask agent by name, `agent-bricks-streamco-ask` (`AGENT_NAME`), so it needs the apps `CAN_USE`
  grant in section 2.
- The drawer doesn't read `system.ai_gateway.usage` at runtime (its figures come from the fixture), so the three
  `system` grants in section 2 are optional.
