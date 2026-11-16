# Databricks notebook source
# Owner: Stream 0 (lead). Generic SQL-file runner used by build_all tasks
# build_metric_views (sql/09), build_decomposition (sql/14) and create_uc_functions (sql/20).
# Parameter `sql_file` is a path relative to this notebook. Placeholders: {catalog}, {schema}, {gateway_model}.

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from cc_common import config, sqlfile  # noqa: E402

cfg = config.load(dbutils)  # noqa: F821
dbutils.widgets.text("sql_file", "")  # noqa: F821
sql_file = dbutils.widgets.get("sql_file")  # noqa: F821
if not sql_file.endswith(".sql"):
    raise ValueError(f"sql_file must be a .sql path relative to notebooks/, got {sql_file!r}")

spark.sql(f"USE CATALOG {cfg.catalog}")  # noqa: F821
spark.sql(f"USE SCHEMA {cfg.schema}")  # noqa: F821
n = sqlfile.run(spark, os.path.abspath(sql_file), cfg)  # noqa: F821
print(f"Executed {n} statements from {sql_file}")
