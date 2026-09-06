# Architecture Notes — Finance Medallion Project

## 1. Layer Responsibilities

| Layer  | Responsibility | Storage | Trigger |
|--------|-----------------|---------|---------|
| Bronze | Raw, immutable, append-only copy of source data + audit columns | Delta, Autoloader-managed | `availableNow` micro-batch (or continuous) |
| Silver | Cleansed, conformed, deduplicated, quality-checked, business-key grain | Delta, MERGE (upsert) | Batch, after Bronze |
| Gold   | Dimensional model (star schema) + pre-aggregated KPIs for BI tools | Delta, CTAS (`CREATE OR REPLACE`) | Batch, after Silver |

## 2. Data Model (Gold / Star Schema)

```
                  dim_customer
                       │
                       │ customer_id
                       ▼
dim_branch ──branch_id──► fact_transactions ◄──account_id── dim_account
```

- **fact_transactions**: grain = one row per transaction_id.
- **dim_customer**, **dim_branch**, **dim_account**: SCD Type 1 (overwrite) in this
  version. For SCD Type 2 (historical tracking of e.g. customer_segment changes),
  extend `merge_to_silver` to add `effective_date`/`end_date`/`is_current` columns
  and use `whenMatchedUpdate` conditioned on hash-diff instead of `UpdateAll`.

## 3. Metadata-Driven Design — Why It Matters

Traditional pipelines: 1 notebook per table × 3 layers = N×3 notebooks to maintain.
This pattern: 3 generic notebooks total, regardless of how many sources you add.

Adding source #5 (e.g., `loan_repayments`) requires:
1. One new row in `pipeline_config.json` (path/format/target tables/PK)
2. Optional rows in `column_mapping.json` if raw column names need renaming
3. Optional rows in `dq_rules.json` if the column needs validation
4. Re-run `01_setup_metadata_tables.py`, then the orchestrator — **zero new PySpark code**

## 4. Data Quality Pattern

Every Silver run splits incoming rows into:
- **clean_df** → MERGEd into the Silver table
- **bad_df** → appended to `{source}_quarantine` table, tagged with `_failed_rule`

This means bad data is never silently dropped — it's always inspectable, and the
`07_data_quality_dashboard.py` notebook surfaces quarantine volume trends.

## 5. Incremental vs Full Load

`pipeline_config.load_type` controls behavior:
- `full`: Bronze reload is fine to overwrite-replace source data each run (e.g. small
  reference tables like `branches`, `customers`).
- `incremental`: Autoloader's checkpoint ensures only new files are picked up;
  Silver MERGE ensures only changed/new primary keys are touched.

## 6. Extending to Gold KPIs

`gold_config.sql_text` is a raw SQL string executed via `spark.sql()`. This keeps
Gold logic in SQL (accessible to analysts) while still being metadata-driven.
`table_type` (`dimension` / `fact` / `kpi`) controls execution order so dependent
KPI tables always see fresh dimension/fact tables.

## 7. Suggested Next Steps for Production Hardening

- Add **Unity Catalog row/column-level security** on `gold` schema for PII fields
  (email, date_of_birth) restricted to authorized finance roles.
- Add **Delta Live Tables (DLT)** as an alternative execution engine for Silver,
  using `EXPECT` clauses generated dynamically from `data_quality_rules` rows.
- Add **SCD Type 2** support to `merge_to_silver` for dimension history.
- Add **great_expectations** or **Databricks Lakehouse Monitoring** for statistical
  drift detection on `transaction_amount` distributions.
- Parameterize `catalog` per environment (dev/staging/prod) via Databricks Asset
  Bundles (DABs) instead of manual widget entry.
