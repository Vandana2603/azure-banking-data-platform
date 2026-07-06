# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver to Gold (Star Schema Build)
# MAGIC Builds dimension and fact tables from the cleaned silver layer.
# MAGIC Triggered by Azure Data Factory immediately after `01_bronze_to_silver`
# MAGIC succeeds.

# COMMAND ----------

dbutils.widgets.text("config_path", "/dbfs/FileStore/bank_platform/config/config.yaml")
config_path = dbutils.widgets.get("config_path")

# COMMAND ----------

import sys
sys.path.append("/dbfs/FileStore/bank_platform")

from pyspark.jobs import silver_to_gold

# COMMAND ----------

sys.argv = ["silver_to_gold", "--config", config_path, "--running-on-databricks"]
silver_to_gold.main()

# COMMAND ----------

# MAGIC %md
# MAGIC Gold layer (star schema) is ready. Next: ADF triggers the
# MAGIC **Copy Data** activity to load these Parquet tables into Azure SQL
# MAGIC Database, followed by the DBT job that runs SQL-side transformations
# MAGIC and tests.
