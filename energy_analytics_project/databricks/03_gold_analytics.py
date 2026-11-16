# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold analytics tables
# MAGIC Run after Silver. These tables are designed for Power BI.

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "energy_analytics"
BASE = f"{CATALOG}.{SCHEMA}"

silver = spark.table(f"{BASE}.silver_electricity")

# 1. State-level KPIs
gold_state = (
    silver.groupBy("state")
    .agg(
        F.sum("energy_met_mu").alias("total_energy_met_mu"),
        F.avg("energy_met_mu").alias("avg_daily_energy_met_mu"),
        F.max("energy_met_mu").alias("max_daily_energy_met_mu"),
        F.avg("peak_met_mw").alias("avg_peak_met_mw"),
        F.max("peak_met_mw").alias("max_peak_met_mw"),
        F.sum("peak_unmet_mw").alias("total_peak_unmet_mw"),
        F.countDistinct("date").alias("days_recorded")
    )
)
gold_state.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{BASE}.gold_state_consumption")

# 2. Monthly trends
gold_monthly = (
    silver.groupBy("year", "month", "month_name")
    .agg(
        F.sum("energy_met_mu").alias("total_energy_met_mu"),
        F.avg("energy_met_mu").alias("avg_daily_energy_met_mu"),
        F.max("peak_met_mw").alias("max_peak_met_mw"),
        F.avg("peak_unmet_mw").alias("avg_daily_peak_unmet_mw")
    )
)
gold_monthly.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{BASE}.gold_monthly_trends")

# 3. Daily peak demand by state
gold_peak = (
    silver.groupBy("date", "year", "month", "state")
    .agg(
        F.max("peak_met_mw").alias("peak_met_mw"),
        F.max("peak_unmet_mw").alias("peak_unmet_mw")
    )
)
gold_peak.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{BASE}.gold_peak_demand")

# 4. Shortage analysis
gold_shortage = (
    silver.groupBy("state", "year", "month")
    .agg(
        F.sum("peak_unmet_mw").alias("total_peak_unmet_mw"),
        F.avg("peak_unmet_mw").alias("avg_peak_unmet_mw"),
        F.max("peak_unmet_mw").alias("max_peak_unmet_mw"),
        F.count(F.when(F.col("peak_unmet_mw") > 0, True)).alias("shortage_days")
    )
)
gold_shortage.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{BASE}.gold_power_shortage")

for table in [
    "gold_state_consumption",
    "gold_monthly_trends",
    "gold_peak_demand",
    "gold_power_shortage"
]:
    print(table)
    display(spark.table(f"{BASE}.{table}").limit(20))
