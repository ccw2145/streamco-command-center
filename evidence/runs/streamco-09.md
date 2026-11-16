# streamco-09

- Run id: 201666735234326
- Result: SUCCESS
- Started: 2026-11-09 04:31 UTC
- Duration: 77 s

## Task `t` (SUCCESS)

Notebook: `/Workspace/Users/<user>/streamco-build/data/notebooks/00_run_sql`  
Task run id: 1029718605362341

#### Cell 1: `# Owner: Stream 0 (lead). Generic SQL-file runner used by build_all tasks`

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

```
Executed 2 statements from ../sql/09_metric_views.sql
```
