# Smart Electricity Consumption Analytics (India)

End-to-end data engineering project using **Databricks Free Edition, PySpark, Delta Lake and Power BI**.

## What is included
- Databricks notebook source files for ingestion, Bronze, Silver and Gold layers.
- Schema-inspection and configurable column mapping.
- Data-quality checks and analytics tables.
- Power BI dashboard plan, DAX measures and relationship guidance.
- Small synthetic CSVs so the pipeline can be tested immediately.
- Setup instructions and project architecture.

> The sample data is synthetic and is only for testing. Replace it with the real CEA/India electricity dataset before presenting project findings. Do not describe synthetic results as real-world findings.

## Architecture

CSV files -> Databricks / PySpark -> Bronze Delta -> Silver Delta -> Gold Delta -> Power BI

## 1. Databricks setup

1. Create a Databricks Free Edition workspace and start available compute.
2. In Catalog Explorer, create catalog `workspace` (if it does not already exist).
3. Create schema `energy_analytics`.
4. Create a volume named `energy_raw` in that schema.
5. Upload the three source CSV files into the volume:
   - `corrected_daily_energy_met_MU.csv`
   - `corrected_peak_met_MW.csv`
   - `peak_unmet_MW.csv`
6. Create a Python notebook and import/run the source files from `databricks/` in order, or paste each file into a separate notebook cell.

Volume path:
`/Volumes/workspace/energy_analytics/energy_raw/`

If your catalog is not named `workspace`, change `CATALOG` in `00_config_and_inspection.py`.

## 2. Run order

| Order | File | Purpose |
|---|---|---|
| 1 | `00_config_and_inspection.py` | Set paths, read CSVs, inspect schemas |
| 2 | `01_bronze_ingestion.py` | Save source data as Bronze Delta tables |
| 3 | `02_silver_transform.py` | Standardize, clean and join the data |
| 4 | `03_gold_analytics.py` | Build state, monthly, peak and shortage aggregates |
| 5 | `04_validation_and_export.py` | Validate outputs and export Gold tables |

**Important:** The source dataset may use different column names from the aliases in this project. Run notebook 00 first. Edit the mapping in notebook 02 (`COLUMN_MAP`) to match the actual printed schema. Do not guess column names or units.

## 3. Data mapping

The Silver layer expects a combined record with:
- `date` — observation date
- `state` — state/UT name
- `energy_met_mu` — energy supplied/met, in MU
- `peak_met_mw` — peak demand met, in MW
- `peak_unmet_mw` — unmet peak demand, in MW

The three input files are joined on `date` and `state`. If your source has different join keys or a different meaning for a measurement, update the mapping and transformation logic accordingly.

## 4. Power BI

Export Gold tables as CSV from Databricks (notebook 04), then use Power BI Desktop:
1. Get Data -> Text/CSV -> import each Gold CSV.
2. Check data types: dates as Date, measures as Decimal Number.
3. Add slicers for State, Year and Month.
4. Build visuals using `power_bi/dashboard_guide.md`.
5. Add measures from `power_bi/measures.dax`.

Recommended pages:
- Executive Overview
- State-wise Analysis
- Monthly Trends
- Peak Demand & Shortage

## 5. Sample data

`sample_data/` contains a tiny **synthetic** dataset with matching column names. It can be uploaded to the Databricks volume using the three expected filenames. It is intended to test the pipeline and verify table creation.

## 6. Troubleshooting

- `PATH_NOT_FOUND`: check the volume path and uploaded filenames.
- `TABLE_OR_VIEW_NOT_FOUND`: run notebook 01 before notebook 02.
- Join produces no records: inspect date formats and state naming in all three files. Normalize names and date types before joining.
- Wrong numeric values: inspect source units and decimal separators; never silently assume units.
- Null metrics: some datasets may not contain a metric for every state/date. Preserve nulls and investigate rather than replacing all nulls with zero.
- Power BI can't connect directly: export Gold CSVs from Databricks and import them into Power BI Desktop.

## 7. Project deliverables
- Raw source files stored in a Databricks Volume.
- Bronze, Silver and Gold Delta tables.
- Data-quality validation output.
- Power BI dashboard with filters, KPIs and trend analysis.
- Short project report describing source, transformations, limitations and insights.

## Responsible reporting
Only report insights calculated from the actual selected dataset. Document missing dates, missing states, null values and source coverage. Sample data is not evidence of India's actual electricity consumption.
