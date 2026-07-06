#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh
# Runs the entire local pipeline end-to-end: data generation -> bronze/silver
# -> silver/gold -> (optional) warehouse load -> dbt.
# Used as the default Docker CMD, and can also be run directly on a host
# machine that has Python + Spark installed.
# =============================================================================
set -e

echo "=============================================="
echo " Azure Banking Data Platform - Local Pipeline"
echo "=============================================="

echo "[1/4] Generating synthetic raw banking data..."
python data/generate_data.py --config config/config.yaml

echo "[2/4] Running bronze -> silver PySpark job..."
python pyspark/jobs/bronze_to_silver.py --config config/config.yaml

echo "[3/4] Running silver -> gold PySpark job (star schema build)..."
python pyspark/jobs/silver_to_gold.py --config config/config.yaml

echo "[4/4] Pipeline complete."
echo "Gold layer output available under: data/gold/"
echo "To load into Azure SQL Database, run sql/ddl.sql then sql/warehouse_load.sql,"
echo "then 'dbt run && dbt test' from the dbt/ directory."
