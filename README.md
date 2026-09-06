# Finance Medallion Architecture — Metadata-Driven Databricks Project

End-to-end, config-driven Bronze → Silver → Gold pipeline for a retail banking
use case (customers, accounts, branches, transactions).

## Folder Structure

```
finance_medallion_project/
│
├── README.md                          # This file
│
├── config/                             # All metadata / control definitions (JSON)
│   ├── pipeline_config.json            # One row per source: paths, tables, load type, PK
│   ├── column_mapping.json             # Source col -> target col + datatype, per source
│   ├── dq_rules.json                   # Data quality rules per source/column
│   └── gold_config.json                # SQL templates that build Gold tables/KPIs
│
├── notebooks/                          # Databricks notebooks (import as .py, run top-down)
│   ├── 00_setup_catalog_schemas.py     # Creates catalog, schemas, volumes
│   ├── 01_setup_metadata_tables.py     # Loads config/*.json into Delta control tables
│   ├── 02_generate_sample_data.py      # Generates synthetic finance data into landing volume
│   ├── 03_bronze_generic_loader.py     # Generic Autoloader-based Bronze ingestion
│   ├── 04_silver_generic_transform.py  # Generic cleansing/DQ/MERGE into Silver
│   ├── 05_gold_generic_aggregator.py   # Generic SQL-template-driven Gold builder
│   ├── 06_orchestrator_driver.py       # Master notebook: calls 03->04->05 in sequence
│   └── 07_data_quality_dashboard.py    # Reads audit_log + quarantine tables, reports health
│
├── utils/                              # Shared Python modules (imported via %run or as .whl)
│   ├── audit_logger.py                 # log_audit() helper, writes to control.audit_log
│   └── common_functions.py             # get_config(), get_column_map(), get_dq_rules(), merge_to_silver()
│
├── jobs/
│   └── workflow_job.json               # Databricks Workflows job definition (3 chained tasks)
│
├── tests/
│   └── test_data_quality.py            # Pytest-style unit tests for DQ rule functions
│
└── docs/
    └── architecture.md                 # Design notes, data model, lineage diagram (text)
```

## Run Order

1. `00_setup_catalog_schemas.py`
2. `01_setup_metadata_tables.py`
3. `02_generate_sample_data.py`  (skip in production — replace with real source drops)
4. `06_orchestrator_driver.py`  (runs Bronze → Silver → Gold for every active source)
5. `07_data_quality_dashboard.py` (optional — check quarantine/audit health)

In production, steps 3–5 above are what a Databricks Workflow job (`jobs/workflow_job.json`)
runs on a schedule instead of manual notebook execution.

## Deploy to Databricks

1. Import or clone this folder as `/Repos/finance_medallion_project`.
2. Run `notebooks/00_setup_catalog_schemas.py` once with the target catalog.
3. Run `notebooks/01_setup_metadata_tables.py`, setting `config_repo_path` to the
    repository's `config` directory in the workspace.
4. Upload `jobs/workflow_job.json` in **Workflows > Jobs > Create or edit JSON**.
    Set the notebook paths and `catalog` parameter for the target workspace.
5. Run `notebooks/02_generate_sample_data.py` only for a demonstration environment.

The workflow JSON includes the catalog and metadata setup tasks so a fresh target
can run end to end. It intentionally does not generate sample data on its schedule.

## Adding a New Source (the whole point of this framework)

To onboard a new source table, **you do not write a new notebook**. You only add:
1. A row to `pipeline_config.json` (path, format, target tables, load type, PK)
2. Rows to `column_mapping.json` (if renaming/casting is needed)
3. Rows to `dq_rules.json` (if validation is needed)
4. Re-run `01_setup_metadata_tables.py` to reload control tables, then run the orchestrator.
