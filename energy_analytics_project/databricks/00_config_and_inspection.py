# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Configuration and source inspection
# MAGIC Run this first. Review the printed schemas and confirm file names.

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "energy_analytics"
VOLUME = "energy_raw"
BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

ENERGY_FILE = f"{BASE_PATH}/corrected_daily_energy_met_MU.csv"
PEAK_FILE = f"{BASE_PATH}/corrected_peak_met_MW.csv"
SHORTAGE_FILE = f"{BASE_PATH}/peak_unmet_MW.csv"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

def read_source(path):
    return (spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("mode", "PERMISSIVE")
            .csv(path))

energy_raw = read_source(ENERGY_FILE)
peak_raw = read_source(PEAK_FILE)
shortage_raw = read_source(SHORTAGE_FILE)

print("ENERGY SOURCE")
energy_raw.printSchema()
display(energy_raw.limit(10))

print("PEAK SOURCE")
peak_raw.printSchema()
display(peak_raw.limit(10))

print("SHORTAGE SOURCE")
shortage_raw.printSchema()
display(shortage_raw.limit(10))

print("Rows:", energy_raw.count(), peak_raw.count(), shortage_raw.count())
