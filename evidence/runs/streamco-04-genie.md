# streamco-04-genie

- Run id: 143908382889491
- Result: SUCCESS
- Started: 2026-11-09 04:49 UTC
- Duration: 102 s

## Task `t` (SUCCESS)

Notebook: `/Workspace/Users/<user>/streamco-build/notebooks/04_genie`  
Task run id: 889094051153246

#### Cell 1: `# StreamCo Command Center 04: Genie Agent "StreamCo Subscriber Insights" (Genie Agents API).`

_(no output)_

#### Cell 2: `%pip install -q "databricks-sdk>=0.124"`

```
ERROR: pip's dependency resolver does not currently take into account all the packages that are installed. This behaviour is the source of the following dependency conflicts.
googleapis-common-protos 1.62.0 requires protobuf!=3.20.0,!=3.20.1,!=4.21.1,!=4.21.2,!=4.21.3,!=4.21.4,!=4.21.5,<5.0.0.dev0,>=3.19.5, but you have protobuf 7.36.2 which is incompatible.
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

#### Cell 3: `dbutils.library.restartPython()  # noqa: F821`

```
Warning: statements after `dbutils.library.restartPython()` will execute before Python is restarted.
```

#### Cell 4: `import datetime as dt`

_(no output)_

#### Cell 5: `TABLES = {  # identifier -> column notes (description, synonyms)`

_(no output)_

#### Cell 6: `# Create or update, found by title.`

```
created StreamCo Subscriber Insights 01f1dbf923701cb3b3ec3d2fcbee9e87 measures as snippets: True
```

#### Cell 7: `# Pre-warm with the scripted follow-ups (starts the warehouse, caches plans). A slow answer never fails the build.`

```
{"genie_agent_id": "01f1dbf923701cb3b3ec3d2fcbee9e87", "prewarm": {"What about Originals?": {"s": 13.0, "status": "MessageStatus.COMPLETED", "text": "For **Originals** on **2027-02-15**, I **can\u2019t give a confident cancellation count** because reconciliation status is **sources_disagree**. The two reported figures are **4,017** cancellations in **Subscriber Live** and **4,268** cancellations in the **Finance ledger**, a difference of **251** cance", "sql": "SELECT `tier`, `subscriber_live_cancels`, `ledger_cancels`, `pct_diff`, `status` FROM `cindy_wu_catalog`.`streamco`.`gold_reconciliation_daily` WHERE `day` = DATE'2027-02-15' AND `tier` ILIKE '%Origin"}, "Show cancels by state": {"s": 13.1, "status": "MessageStatus.COMPLETED", "text": "For **Sports** on **2027-02-15**, cancellations were highest in **Texas** and lowest in **Wyoming** across **51 states/jurisdictions**. Examples include:\n- **Texas:** **3,488**\n- **California:** **1,606**\n- **Florida:** **894**\n- **New York:** **761**\n- **Pennsylvania:** **543**\n\nThe spread is wide,", "sql": "SELECT\n  `state`,\n  MEASURE(`cancels`) AS `cancels`\nFROM `cindy_wu_catalog`.`streamco`.`mv_streamco_metrics`\nWHERE `day` = DATE'2027-02-15'\n  AND `tier` = 'Sports'\n  AND `state` IS NOT NULL\nGROUP BY A"}, "Why do Finance and Product show different cancel numbers?": {"s": 13.8, "status": "MessageStatus.COMPLETED", "text": "Finance and Product show different cancel numbers because they are counting **different events**. **Product analytics** shows **17,108** based on cancel button clicks in the app, including people who were later saved or changed their mind, while **Finance** shows **15,760** from the billing ledger w", "sql": "SELECT `team`, `count`, `definition`, `governed_count`, `governed_definition`\nFROM `cindy_wu_catalog`.`streamco`.`gold_team_cancel_counts`\nWHERE `team` IS NOT NULL AND `count` IS NOT NULL\nORDER BY `co"}}}
```
