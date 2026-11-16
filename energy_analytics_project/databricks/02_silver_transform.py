# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver cleaning, standardization and joining
# MAGIC IMPORTANT: Run notebook 00 and 01 first.
# MAGIC Inspect the schemas from notebook 00 and edit COLUMN_MAP below.
# MAGIC Map each logical field to the exact column name in its source DataFrame.

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "energy_analytics"
BASE = f"{CATALOG}.{SCHEMA}"

# EDIT THESE VALUES after inspecting the source schemas.
# The default values match the included synthetic sample CSVs.
COLUMN_MAP = {
    "energy": {
        "date": "Date",
        "state": "State",
        "value": "Energy_Met_MU"
    },
    "peak": {
        "date": "Date",
        "state": "State",
        "value": "Peak_Met_MW"
    },
    "shortage": {
        "date": "Date",
        "state": "State",
        "value": "Peak_Unmet_MW"
    }
}

def standardize_source(df, date_col, state_col, value_col, metric_name):
    required = [date_col, state_col, value_col]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"{metric_name}: columns not found: {missing}. "
            f"Available columns: {df.columns}. Update COLUMN_MAP."
        )

    result = df.select(
        F.to_date(F.col(date_col)).alias("date"),
        F.initcap(F.trim(F.col(state_col).cast("string"))).alias("state"),
        F.regexp_replace(
            F.trim(F.col(value_col).cast("string")), ",", ""
        ).cast("double").alias(metric_name)
    )
    return result.filter(
        F.col("date").isNotNull() &
        F.col("state").isNotNull() &
        (F.col("state") != "")
    )

energy_silver = standardize_source(
    energy_raw,
    COLUMN_MAP["energy"]["date"],
    COLUMN_MAP["energy"]["state"],
    COLUMN_MAP["energy"]["value"],
    "energy_met_mu"
)

peak_silver = standardize_source(
    peak_raw,
    COLUMN_MAP["peak"]["date"],
    COLUMN_MAP["peak"]["state"],
    COLUMN_MAP["peak"]["value"],
    "peak_met_mw"
)

shortage_silver = standardize_source(
    shortage_raw,
    COLUMN_MAP["shortage"]["date"],
    COLUMN_MAP["shortage"]["state"],
    COLUMN_MAP["shortage"]["value"],
    "peak_unmet_mw"
)

# Remove duplicate keys within each source. If duplicates have a business
# meaning, aggregate them instead of arbitrarily selecting a record.
energy_silver = energy_silver.dropDuplicates(["date", "state"])
peak_silver = peak_silver.dropDuplicates(["date", "state"])
shortage_silver = shortage_silver.dropDuplicates(["date", "state"])

# Full outer joins preserve records that occur in only one source.
silver = (
    energy_silver
    .join(peak_silver, ["date", "state"], "full")
    .join(shortage_silver, ["date", "state"], "full")
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("region", F.lit("Unknown"))
    .withColumn("_processed_at", F.current_timestamp())
)

# Do not replace missing energy/demand values with zero: null may mean
# unavailable data, not zero consumption.
(silver.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{BASE}.silver_electricity"))

print("Silver table created:", silver.count(), "rows")
display(silver.orderBy("date", "state").limit(20))

# Data quality summary
display(silver.select(
    F.count("*").alias("row_count"),
    F.countDistinct("state").alias("state_count"),
    F.min("date").alias("start_date"),
    F.max("date").alias("end_date"),
    F.sum(F.col("energy_met_mu").isNull().cast("int")).alias("missing_energy"),
    F.sum(F.col("peak_met_mw").isNull().cast("int")).alias("missing_peak"),
    F.sum(F.col("peak_unmet_mw").isNull().cast("int")).alias("missing_shortage")
))
