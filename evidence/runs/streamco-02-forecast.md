# streamco-02-forecast

- Run id: 400489213566361
- Result: SUCCESS
- Started: 2026-11-09 04:28 UTC
- Duration: 130 s

## Task `t` (SUCCESS)

Notebook: `/Workspace/Users/<user>/streamco-build/notebooks/02_forecast`  
Task run id: 837378004785351

#### Cell 1: `# StreamCo Command Center 02: game-night viewership forecast by metro x CDN, issued at kickoff.`

_(no output)_

#### Cell 2: `%pip install -q lightgbm>=4.5 mlflow>=3.4`

```
ERROR: pip's dependency resolver does not currently take into account all the packages that are installed. This behaviour is the source of the following dependency conflicts.
jupyter-server 1.23.4 requires anyio<4,>=3.1.0, but you have anyio 4.16.0 which is incompatible.
googleapis-common-protos 1.62.0 requires protobuf!=3.20.0,!=3.20.1,!=4.21.1,!=4.21.2,!=4.21.3,!=4.21.4,!=4.21.5,<5.0.0.dev0,>=3.19.5, but you have protobuf 7.36.2 which is incompatible.
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

#### Cell 3: `dbutils.library.restartPython()  # noqa: F821`

```
Warning: statements after `dbutils.library.restartPython()` will execute before Python is restarted.
```

#### Cell 4: `import json`

```
2026/11/09 04:29:55 INFO mlflow.tracking.fluent: Experiment with name '/Users/<user>/streamco-command-center' does not exist. Creating a new experiment.
If you are using MLflow Tracing, consider storing your traces in Unity Catalog for unlimited storage (no 100,000 trace limit), fine-grained access controls, and queryability from notebooks, SQL, and dashboards. Learn more: https://docs.databricks.com/aws/en/mlflow3/genai/tracing/trace-unity-catalog
<Experiment: artifact_location='dbfs:/databricks/mlflow-tracking/881368839201646', creation_time=1794198595162, effective_trace_archival_retention=None, experiment_id='881368839201646', last_update_time=1794198595162, lifecycle_stage='active', name='/Users/<user>/streamco-command-center', tags={'mlflow.experiment.sourceName': '/Users/<user>/streamco-command-center',
 'mlflow.experimentType': 'MLFLOW_EXPERIMENT',
 'mlflow.ownerEmail': '<user>',
 'mlflow.ownerId': '7635338147052150'}, trace_location=None, workspace='default'>
```

#### Cell 5: `# Training data: past big events at 5-minute grain, each cell's ordinary prime-time level as a size feature.`

```
training rows=20,736 events=8  holdout EVT-20270207 peak error -10.1%
```

#### Cell 6: `# Game-night inputs available at kickoff: each cell's 6:00-6:29 PM ET telemetry.`

_(no output)_

#### Cell 7: `# Train on all past events, register, forecast.`

```
/local_disk0/.ephemeral_nfs/envs/pythonEnv-20200ffa-14fb-4128-b7b9-9e8082554a34/lib/python3.10/site-packages/mlflow/types/utils.py:440: UserWarning: Hint: Inferred schema contains integer column(s). Integer columns in Python cannot represent missing values. If your input data contains missing values at inference time, it will be encoded as floats and will cause a schema enforcement error. The best way to avoid this problem is to infer the model schema based on a realistic data sample (training dataset) that includes missing values. Alternatively, you can declare integer columns as doubles (float64) whenever these columns may have missing values. See `Handling Integers With Missing Values <https://www.mlflow.org/docs/latest/models.html#handling-integers-with-missing-values>`_ for more details.
  warnings.warn(
🔗 View Logged Model at: https://<workspace-host>/ml/experiments/881368839201646/models/m-1e8aee6dc83b4b829883a3098fccc194?o=<workspace-id>
2026/11/09 04:30:10 INFO mlflow.pyfunc: Validating input example against model signature
Successfully registered model 'cindy_wu_catalog.streamco.viewer_forecast'.
🔗 Created version '1' of model 'cindy_wu_catalog.streamco.viewer_forecast': https://<workspace-host>/explore/data/models/cindy_wu_catalog/streamco/viewer_forecast/version/1?o=<workspace-id>
<RegisteredModel: aliases={}, creation_timestamp=1794198612360, deployment_job_id='', deployment_job_state='DEPLOYMENT_JOB_CONNECTION_STATE_UNSPECIFIED', description=('Game-night viewership forecast per metro x CDN (LightGBM quantile '
 'p10/p50/p90 relative to the pre-kickoff level). Issued at kickoff.'), last_updated_timestamp=1794198616134, latest_versions=None, name='cindy_wu_catalog.streamco.viewer_forecast', tags={}, workspace='default'>
```

#### Cell 8: `# forecast_viewers and forecast_flags`

```
DataFrame[num_affected_rows: bigint, num_inserted_rows: bigint]
```

#### Cell 9: `# Validation (numbers reported; only structural failures stop the task)`

```
{"model": "cindy_wu_catalog.streamco.viewer_forecast", "version": "1", "rows": 2592, "national_peak_forecast": 9599588, "national_peak_actual": 9603491, "peak_err": -0.0004, "dallas_cdn_c_util": 0.968, "flagged": [["DFW", "CDN-C", 0.968]], "max_other_cell": [["DFW", "CDN-B", 0.8012]], "holdout_peak_err": -0.1012}
```
