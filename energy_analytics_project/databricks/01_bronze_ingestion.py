# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze ingestion
# MAGIC Bronze preserves source columns and adds ingestion metadata.

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "energy_analytics"
BASE = f"{CATALOG}.{SCHEMA}"

# If running in a fresh notebook, run/import notebook 00 first.
# The three DataFrames should be named energy_raw, peak_raw, shortage_raw.

def write_bronze(df, table_name):
    output = df.withColumn("_ingested_at", F.current_timestamp()) \
               .withColumn("_source_system", F.lit("India electricity CSV dataset"))
    (output.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{BASE}.{table_name}"))
    print(f"Saved {BASE}.{table_name}: {output.count()} rows")

write_bronze(energy_raw, "bronze_energy")
write_bronze(peak_raw, "bronze_peak")
write_bronze(shortage_raw, "bronze_shortage")

display(spark.table(f"{BASE}.bronze_energy").limit(10))
