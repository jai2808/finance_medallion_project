# Finance Medallion Architecture — Community Edition

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
│   ├── transformation_rules.json       # Ordered expressions and handler selections
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
│   ├── audit_logger.py                 # Pipeline and step audit helpers
│   ├── common_functions.py             # Metadata-driven mappings, DQ, transforms, and merges
│   └── transformation_registry.py     # Reviewed handlers for complex domain logic
│
├── jobs/
│   └── workflow_job.json               # Paid-workspace workflow definition (not used in CE)
│
├── tests/
│   └── test_data_quality.py            # Pytest-style unit tests for DQ rule functions
│
└── docs/
    └── architecture.md                 # Design notes, data model, lineage diagram (text)
```

## Community Edition Run Order

1. Upload the four `config/*.json` files to `/FileStore/finance_medallion_project/config/`.
2. Run `00_setup_catalog_schemas.py`.
3. Run `01_setup_metadata_tables.py` with its default `config_repo_path`.
4. Run `02_generate_sample_data.py` for the demonstration data.
5. Run `06_orchestrator_driver.py` for Bronze -> Silver -> Gold.
6. Run `07_data_quality_dashboard.py` to inspect audit and quarantine results.

The Community Edition version uses one default-metastore database, flat table names,
`/FileStore` paths, batch Bronze reads, and manual notebook execution. The
`jobs/workflow_job.json` file is retained only for a paid Databricks workspace.

## Complex Transformations

Use `transformation_rules.json` for ordered `derive`, `filter`, and
`drop_duplicates` operations. Expressions are evaluated by Spark SQL and can
contain `CASE`, date functions, conditional logic, and other supported Spark SQL
functions. Each rule has a `config_version`; only the highest active version per
source is executed.

For logic that needs multiple DataFrame steps, windows, joins, lookups, or an
external service, add a reviewed function to `utils/transformation_registry.py`,
register its stable `handler_name`, and add an `operation: "handler"` rule. The
metadata selects the handler; it cannot execute arbitrary Python. This creates a
controlled extension point that can be code-reviewed, unit-tested, and promoted
through environments independently of the pipeline notebook.

## Upload to Community Edition

1. Create a folder in your Community Edition workspace.
2. Upload the files under `notebooks/` and `utils/` as notebooks/source files.
3. Upload the JSON files to `/FileStore/finance_medallion_project/config/`.
4. Run the notebooks in the order above. Set `database` to `finance_project`.

## Adding a New Source (the whole point of this framework)

To onboard a new source table, **you do not write a new notebook**. You only add:
1. A row to `pipeline_config.json` (path, format, target tables, load type, PK)
2. Rows to `column_mapping.json` (if renaming/casting is needed)
3. Rows to `dq_rules.json` (if validation is needed)
4. Re-run `01_setup_metadata_tables.py` to reload control tables, then run the orchestrator.
