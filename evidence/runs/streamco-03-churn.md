# streamco-03-churn

- Run id: 555881085293319
- Result: SUCCESS
- Started: 2026-11-09 04:37 UTC
- Duration: 664 s

## Task `t` (SUCCESS)

Notebook: `/Workspace/Users/<user>/streamco-build/notebooks/03_churn`  
Task run id: 279782325334247

#### Cell 1: `# StreamCo Command Center 03: churn model, scores, save plan and model card.`

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
If you are using MLflow Tracing, you can migrate your traces to Unity Catalog for unlimited storage, fine-grained access controls, and queryability from notebooks, SQL, and dashboards. Learn more: https://docs.databricks.com/aws/en/mlflow3/genai/tracing/migrate-traces-to-uc
```

#### Cell 5: `# 1. Train and register`

```
/local_disk0/.ephemeral_nfs/envs/pythonEnv-326f0b1b-489b-420d-9762-b02e757f8702/lib/python3.10/site-packages/mlflow/types/utils.py:440: UserWarning: Hint: Inferred schema contains integer column(s). Integer columns in Python cannot represent missing values. If your input data contains missing values at inference time, it will be encoded as floats and will cause a schema enforcement error. The best way to avoid this problem is to infer the model schema based on a realistic data sample (training dataset) that includes missing values. Alternatively, you can declare integer columns as doubles (float64) whenever these columns may have missing values. See `Handling Integers With Missing Values <https://www.mlflow.org/docs/latest/models.html#handling-integers-with-missing-values>`_ for more details.
  warnings.warn(
🔗 View Logged Model at: https://<workspace-host>/ml/experiments/881368839201646/models/m-b5489bd4e2674d15a619c778b485a7a3?o=<workspace-id>
2026/11/09 04:40:27 INFO mlflow.pyfunc: Validating input example against model signature
Successfully registered model 'cindy_wu_catalog.streamco.churn_propensity'.
🔗 Created version '1' of model 'cindy_wu_catalog.streamco.churn_propensity': https://<workspace-host>/explore/data/models/cindy_wu_catalog/streamco/churn_propensity/version/1?o=<workspace-id>
cindy_wu_catalog.streamco.churn_propensity v1 @champion  test AUC 0.8033  rows train/calib/test 839,748/119,910/240,342
```

#### Cell 6: `# 2. Score the eligible population with the champion`

```
DataFrame[]
```

#### Cell 7: `# 3. Save plan`

_(no output)_

#### Cell 8: `# 4. Model card`

_(no output)_

#### Cell 9: `# Validation: structural checks fail the task; numbers are reported against the generator's targets.`

```
{"model": "cindy_wu_catalog.streamco.churn_propensity v1", "auc": 0.8033, "auc_target": 0.8, "n_scored": 299624, "at_risk": 29963, "at_risk_target": 31000, "projected_cancels_30d": 6696, "projected_target": 4900, "at_risk_without_card": 1694, "cards": {"payment": {"n": 4556, "target": 5000, "expected": 1247, "saved": 624, "cost": 1822}, "playback": {"n": 13165, "target": 17000, "expected": 3217, "saved": 804, "cost": 39462}, "onboarding": {"n": 10548, "target": 9000, "expected": 1921, "saved": 672, "cost": 527}}, "totals": {"saved": 2100, "saved_target": 1700, "cost": 41812, "cost_target": 53408, "revenue": 302150, "revenue_target": 245000}, "samples": [{"first_name": "Marcus", "planted_card": "playback", "card_id": "playback", "risk_decile": 10, "churn_prob": 0.418}, {"first_name": "Priya", "planted_card": "payment", "card_id": "payment", "risk_decile": 10, "churn_prob": 0.373}, {"first_name": "Jordan", "planted_card": "onboarding", "card_id": "onboarding", "risk_decile": 10, "churn_prob": 0.606}], "top_drivers": ["Days since last watched", "Worst buffering in one session", "Watching less", "Low viewing hours", "Failed payment, never retried"]}
```
