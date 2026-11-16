# streamco-05-personalize-preview

- Run id: 236648554263877
- Result: SUCCESS
- Started: 2026-11-09 18:26 UTC
- Duration: 84 s

## Task `t` (SUCCESS)

Notebook: `/Workspace/Users/<user>/streamco-build/notebooks/05_personalize`  
Task run id: 425407066275066

#### Cell 1: `# StreamCo Command Center 05: personalized save messages with batch ai_query through Unity Gateway.`

_(no output)_

#### Cell 2: `import datetime as dt`

_(no output)_

#### Cell 3: `# Inputs: at-risk subscribers with a card + the three samples (with first names).`

```
payment     Save-offer policy / 1. Payment failures: Involuntary (payment) cancellations receive a free payment retry. No discount or credit is offered. (score 0.003)
playback    Save-offer policy / 2. Service failures: Subscribers with a verified service failure may receive one one-month credit per 12 months. (score 0.004)
onboarding  Save-offer policy / 3. New event sign-ups: Event sign-ups in their first 30 days receive onboarding messages only. No discount is offered. (score 0.004)
inputs: 6 (samples 3)
```

#### Cell 4: `# Batch ai_query (failOnError => false: errors come back per row).`

```
ai_query: 6 rows in 0.4 min
```

#### Cell 5: `# Guards, row by row. Failures get the card's template.`

```
DataFrame[]
```

#### Cell 6: `n_llm = sum(1 for r in rows if r[6] == MODEL)`

```
{"table": "cindy_wu_catalog.streamco.personalized_messages_preview", "n_messages": 6, "from_model": 6, "policy": {"payment": "Save-offer policy: 1. Payment failures: Involuntary (payment) cancellations receive a free payment retry. No discount or credit is offered.", "playback": "Save-offer policy: 2. Service failures: Subscribers with a verified service failure may receive one one-month credit per 12 months.", "onboarding": "Save-offer policy: 3. New event sign-ups: Event sign-ups in their first 30 days receive onboarding messages only. No discount is offered."}, "template": 0, "fail_reasons": {}, "cost_usd_estimated": 0.01, "minutes": 0.4}
```
