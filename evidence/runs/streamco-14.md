# streamco-14

- Run id: 987953182964477
- Result: SUCCESS
- Started: 2026-11-09 04:33 UTC
- Duration: 82 s

## Task `t` (SUCCESS)

Notebook: `/Workspace/Users/<user>/streamco-build/data/notebooks/00_run_sql`  
Task run id: 1083539144884596

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
Executed 1 statements from ../sql/14_cancel_decomposition.sql
```
