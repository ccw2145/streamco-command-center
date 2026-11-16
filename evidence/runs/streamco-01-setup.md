# streamco-01-setup

- Run id: 999195199975515
- Result: SUCCESS
- Started: 2026-11-09 04:34 UTC
- Duration: 74 s

## Task `t` (SUCCESS)

Notebook: `/Workspace/Users/<user>/streamco-build/notebooks/01_setup`  
Task run id: 706327176592392

#### Cell 1: `# StreamCo Command Center 01: demo-layer views, the Ask tool function, the governed metric view, and checks.`

_(no output)_

#### Cell 2: `import datetime as dt`

```
DataFrame[]
```

#### Cell 3: `def run_sql_file(path: str) -> int:`

```
views.sql: 6 statements
metric_view.sql: 1 statements
```

#### Cell 4: `# Structural checks.`

```
OK  v_subscribers             3,859,513
OK  v_support_showcase                1
OK  v_training_features       1,200,000
OK  v_scoring_features          299,624
PASS  every view returns rows  
PASS  no brand terms in screen text  
PASS  tiers are StreamCo names  
PASS  kids-only accounts filtered from v_subscribers  
PASS  scoring population is eligible only  
PASS  emails masked for this caller  
PASS  showcase transcript has redactions
```

#### Cell 5: `# Numbers (reported, not asserted: the app numbers are illustrative).`

```
ok governed=15800
```
