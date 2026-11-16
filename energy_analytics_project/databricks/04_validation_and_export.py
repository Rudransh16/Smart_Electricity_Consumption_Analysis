# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Validation and Power BI export
# MAGIC Validates Gold tables and writes CSV exports to the Volume.

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "energy_analytics"
BASE = f"{CATALOG}.{SCHEMA}"
EXPORT_PATH = "/Volumes/workspace/energy_analytics/energy_raw/powerbi_exports"

gold_tables = [
    "gold_state_consumption",
    "gold_monthly_trends",
    "gold_peak_demand",
    "gold_power_shortage"
]

for table_name in gold_tables:
    df = spark.table(f"{BASE}.{table_name}")
    print(f"{table_name}: {df.count()} rows")
    if df.count() == 0:
        raise ValueError(f"{table_name} is empty")
    display(df.limit(5))

# Export each table as a folder containing part-*.csv.
# In Databricks, download the generated CSV part file(s) for Power BI.
for table_name in gold_tables:
    output_path = f"{EXPORT_PATH}/{table_name}"
    (spark.table(f"{BASE}.{table_name}")
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(output_path))
    print("Exported:", output_path)

print("Validation and export completed.")
