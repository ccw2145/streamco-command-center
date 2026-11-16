# streamco-10

- Run id: 731353196876301
- Result: SUCCESS
- Started: 2026-11-09 04:31 UTC
- Duration: 98 s

## Task `t` (SUCCESS)

Notebook: `/Workspace/Users/<user>/streamco-build/data/notebooks/10_anomaly_engine_sim`  
Task run id: 480425668050703

#### Cell 1: `# Owner: Stream A (Data + Pipelines). Stand-in for the customer's Anomaly Engine (same output contract).`

_(no output)_

#### Cell 2: `%pip install -q -r ../requirements.txt`

```
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

#### Cell 3: `dbutils.library.restartPython()  # noqa: F821`

```
Warning: statements after `dbutils.library.restartPython()` will execute before Python is restarted.
```

#### Cell 4: `import os`

_(no output)_

#### Cell 5: `ALERT_SCHEMA = ("cdn STRING, metro STRING, device_family STRING, app_version STRING, detected_ts TIMESTAMP, "`

```
cdn metro device_family  ...  baseline     z_score       max_z
0  CDN-C   DFW          Roku  ...  0.009978  113.060002  648.781191

[1 rows x 10 columns]
```

#### Cell 6: `# Alert rows. The planted incident gets planted.INCIDENT["alert_id"]; any other alert gets a derived id.`

_(no output)_

#### Cell 7: `# Traffic-shift recommendation for each high alert (rules only; the LLM just phrases it later).`

_(no output)_

#### Cell 8: `# Self-asserts`

```
| alert_id | detected_ts | cdn | metro | device_family | app_version | metric | observed | baseline | z_score | persistence_min | affected_sessions | severity | status | resolved_ts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ALR-20270214-DFW-C-ROKU720 | 2027-02-15T01:19:00.000Z | CDN-C | DFW | Roku | 7.2.0 | rebuffer_ratio | 0.10550875556387683 | 0.009978043081624439 | 113.06000242131208 | 3 | 208498 | high | resolved | 2027-02-15T01:36:00.000Z |
10_anomaly_engine_sim: all self-asserts passed
```
