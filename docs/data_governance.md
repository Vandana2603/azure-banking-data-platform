# Data Governance

## Data Quality Validation

- Schema validation (`validation.validate_schema`) enforced at the bronze->silver
  boundary - any unexpected column drop/type mismatch fails the job loudly.
- Null-rate thresholds (`validation.validate_not_null`) checked per critical
  column, configurable in `config/config.yaml` (`data_quality.null_threshold_pct`).
- Duplicate-rate checks (`validation.duplicate_rate`) reported per dataset.
- Referential integrity checks (`validation.validate_referential_integrity`)
  between facts and their parent dimensions before loading the warehouse.
- Invalid records are never silently dropped - they are written to a
  `_rejected/<dataset>` path for investigation (see `bronze_to_silver.py`).

## Metadata & Documentation

- Every dataset and column is documented in dbt `schema.yml` files
  (`dbt/models/**/*.yml`), which `dbt docs generate` renders into a searchable
  data catalog with lineage graphs.
- `docs/data_dictionary.md` documents raw source datasets and naming standards.

## Naming Standards

See `docs/data_dictionary.md#naming-standards` for the full convention list
(snake_case columns, `_id`/`_sk` key suffixes, `_is_valid` flags, `_`-prefixed
audit columns).

## Data Lineage

- Column and table-level lineage is captured via dbt `ref()`/`source()`
  functions, which dbt turns into a browsable DAG (`dbt docs generate && dbt docs serve`).
- Pipeline-level lineage (raw -> bronze -> silver -> gold -> warehouse -> BI)
  is documented in `docs/architecture.md` and `docs/data_dictionary.md`.
- Every record carries `_source_system`, `_batch_id`, and `_ingested_at`
  metadata columns from bronze onward for traceability.

## Audit Logs

- `dw.pipeline_audit_log` (see `sql/ddl.sql`) records every pipeline step's
  start/end time, status, and row counts, written by the
  `Write_Audit_Log` activity in the ADF pipeline.
- Application-level logs are written by `pyspark/utils/logger.py` to both
  console (visible in Databricks job run output) and `logs/pipeline.log`.

## Access & Security Notes (for a real deployment)

- Secrets (SQL passwords, storage keys, Databricks tokens) are never stored
  in code or `config.yaml` - they are read from environment variables locally,
  and from **Azure Key Vault** (via Databricks secret scopes and ADF Key
  Vault-backed linked services) in Azure.
- ADLS Gen2 containers should use **RBAC + ACLs** scoped to the Databricks
  service principal and ADF managed identity, following least-privilege.
- Azure SQL Database should enable **Transparent Data Encryption** (on by
  default) and restrict access via firewall rules / Private Endpoint.
