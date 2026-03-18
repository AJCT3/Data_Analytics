# Databricks Pipelines

This folder contains Databricks-based PySpark pipelines for the UEC Team.

## Structure
- `reference_data/` → Scripts to build and maintain reference Delta/Parquet tables.
- `MH_Referrals_with_Care_Contacts_London.py` → Main pipeline for building the MH Referrals with Care Contacts dataset.

## Running the Pipelines
- Open in Databricks Workspace.
- Set `RUN_MODE` widget to `"full"` for initial build (back to 2019-01-01) or `"update"` for monthly incremental.
- Pipelines write to Delta tables in ADLS Gen2.

## Notes
- Uses Spark optimisations: dynamic partition pruning, adaptive execution.
- External reference data paths are hardcoded — ensure access is configured before running.
