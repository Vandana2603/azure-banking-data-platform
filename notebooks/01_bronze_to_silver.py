# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze to Silver
# MAGIC This notebook is the Databricks entry point invoked by Azure Data Factory's
# MAGIC **"Notebook" activity** in `pipelines/pl_daily_banking_pipeline.json`.
# MAGIC
# MAGIC It clones/reads the repo (synced via Databricks Repos in a real workspace)
# MAGIC and executes the reusable `pyspark/jobs/bronze_to_silver.py` job so the
# MAGIC transformation logic lives in one tested, version-controlled place instead
# MAGIC of being duplicated inside the notebook.

# COMMAND ----------

dbutils.widgets.text("config_path", "/dbfs/FileStore/bank_platform/config/config.yaml")
config_path = dbutils.widgets.get("config_path")

# COMMAND ----------

import sys
sys.path.append("/dbfs/FileStore/bank_platform")   # repo synced to DBFS / Repos folder

from pyspark.jobs import bronze_to_silver

# COMMAND ----------

# Reuse the same main() used for spark-submit, just force the Databricks path mode
import argparse
sys.argv = ["bronze_to_silver", "--config", config_path, "--running-on-databricks"]
bronze_to_silver.main()

# COMMAND ----------

# MAGIC %md
# MAGIC Job finished. Silver-layer Parquet datasets are now available under
# MAGIC `abfss://silver@bankdatalakedev.dfs.core.windows.net/`.
# MAGIC Notify downstream: ADF proceeds to the `silver_to_gold` notebook activity.
