# Data Dictionary

## Raw Datasets

| Dataset | Format | Key Column(s) | Description |
|---|---|---|---|
| customers.csv | CSV | customer_id | Customer master data |
| accounts.csv | CSV | account_id | Bank accounts linked to customers/branches/products |
| branches.csv | CSV | branch_id | Physical/virtual branch reference data |
| products.csv | CSV | product_id | Banking product catalog |
| transaction_channels.csv | CSV | channel_id | Channels used for transactions (ATM, UPI, etc.) |
| payments.json | NDJSON | payment_id | Payment transactions between accounts |
| savings_transactions.json | NDJSON | transaction_id | Deposits/withdrawals/interest/fees on savings accounts |

## Naming Standards

- Snake_case for all column names (`customer_id`, not `CustomerID` or `Customer Id`).
- Business/natural keys use the pattern `<entity>_id` (e.g. `customer_id`, `account_id`).
- Surrogate keys in the warehouse use the pattern `<entity>_sk`.
- Boolean validity flags use the pattern `<column>_is_valid`.
- Audit/lineage metadata columns are prefixed with an underscore
  (`_source_system`, `_batch_id`, `_ingested_at`).
- Dimension tables are prefixed `Dim`, fact tables `Fact` (SQL Server) /
  `dim_` and `fact_` (dbt models, following dbt naming conventions).

## Data Lineage (Summary)

```
data/generate_data.py
    -> data/raw/*.csv, *.json                              [RAW]
    -> ADLS Gen2 raw container                              [RAW]
    -> pyspark/jobs/bronze_to_silver.py                     [BRONZE -> SILVER]
    -> ADLS Gen2 silver container (Parquet)                 [SILVER]
    -> pyspark/jobs/silver_to_gold.py                       [SILVER -> GOLD]
    -> ADLS Gen2 gold container (Parquet, star schema)       [GOLD]
    -> ADF Copy Data -> Azure SQL stg schema                 [STAGING]
    -> sql/warehouse_load.sql (MERGE)                        [DW]
    -> dbt staging/intermediate/marts models                 [DW / semantic layer]
    -> Power BI dashboards                                   [REPORTING]
```

Full column-level lineage for each table is captured in the corresponding
dbt model's `description` and `columns` blocks (see `dbt/models/**/*.yml`),
which `dbt docs generate` turns into a browsable, searchable lineage graph.
