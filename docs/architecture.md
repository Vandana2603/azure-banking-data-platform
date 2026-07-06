# Architecture

## End-to-End Flow

```
CSV / JSON Banking Data (data/generate_data.py)
        │
        ▼
Azure Data Lake Storage Gen2 - RAW layer
        │  (Azure Data Factory: Copy Data activity)
        ▼
Azure Data Factory - pl_daily_banking_pipeline
        │
        ├─► Databricks Notebook: 01_bronze_to_silver
        │       - schema validation, cleaning, dedup, business-rule validation
        │       - writes ADLS Gen2 SILVER layer (Parquet)
        │
        ├─► Databricks Notebook: 02_silver_to_gold
        │       - builds star schema: dims + facts with surrogate keys
        │       - writes ADLS Gen2 GOLD layer (Parquet, partitioned)
        │
        ├─► Copy Data: GOLD Parquet -> Azure SQL staging tables (stg schema)
        │
        ├─► Stored Procedure: usp_run_warehouse_load
        │       - runs sql/warehouse_load.sql MERGE statements
        │       - idempotent upserts into dw.* star schema tables
        │
        ├─► Custom Activity: dbt run / dbt test
        │       - staging -> intermediate -> marts models
        │       - schema + custom data tests
        │
        └─► Stored Procedure: usp_write_pipeline_audit_log
                - writes row counts / status to dw.pipeline_audit_log

Azure SQL Database (BankAnalyticsDW)
        │
        ▼
Power BI (Executive / Customer Analytics / Operations dashboards)
```

## Layered Storage Strategy (Medallion Architecture)

| Layer  | Format  | Purpose                                             |
|--------|---------|------------------------------------------------------|
| Raw    | CSV/JSON | Exact copy of source system extracts, immutable      |
| Bronze | (raw, same as landed) | Ingested as-is, partitioned by ingestion date |
| Silver | Parquet | Cleaned, validated, deduplicated, business rules applied |
| Gold   | Parquet | Star schema (dimensions + facts) with surrogate keys |

## Why this design

- **ADLS Gen2 + Databricks** gives cheap, scalable storage and distributed
  compute for the messy, high-volume raw data (25k-50k+ records here, but the
  same code scales to billions of rows).
- **PySpark modules are decoupled from notebooks.** Notebooks are thin
  wrappers that call tested, reusable `pyspark/jobs/*.py` and
  `pyspark/transformations/*.py` code - this mirrors how mature data teams
  avoid "notebook spaghetti" and keep logic unit-testable (see `tests/`).
- **DBT** owns SQL-layer transformations, testing, and documentation once
  data lands in the warehouse - giving analysts a version-controlled,
  testable semantic layer instead of ad hoc SQL scripts.
- **Star schema** in Azure SQL Database keeps Power BI models simple, fast,
  and easy to explain to business stakeholders.
- **Idempotent MERGE loads + watermark tracking** mean the daily pipeline is
  safe to re-run without creating duplicate facts.
