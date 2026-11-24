# Databricks notebook source
"""Run once (or safely rerun) to set up the portfolio demonstration."""
import csv
import json
import os
import re

from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# COMMAND ----------
dbutils.widgets.text("catalog", "", "Unity Catalog name (blank = current catalog)")
catalog = dbutils.widgets.get("catalog").strip() or spark.sql("SELECT current_catalog()").first()[0]
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError("Use a simple existing Unity Catalog name (letters, numbers, underscores).")
if catalog.lower() == "hive_metastore":
    raise ValueError("Choose a Unity Catalog-enabled catalog rather than hive_metastore.")
schema = "md_framework"
space = f"`{catalog}`.`{schema}`"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {space}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {space}.`landing`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {space}.`state`")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {space}.pipeline_config (
    pipeline_id STRING,
    file_format STRING,
    input_subdir STRING,
    bronze_table STRING,
    silver_table STRING,
    rule_set STRING,
    active BOOLEAN
) USING DELTA
""")

# COMMAND ----------
configs = [
    ("customers", "csv", "customers", "bronze_customers", "silver_customers", "customers", True),
    ("transactions", "json", "transactions", "bronze_transactions", "silver_transactions", "transactions", True),
]
config_schema = StructType([
    StructField("pipeline_id", StringType()),
    StructField("file_format", StringType()),
    StructField("input_subdir", StringType()),
    StructField("bronze_table", StringType()),
    StructField("silver_table", StringType()),
    StructField("rule_set", StringType()),
    StructField("active", BooleanType()),
])
spark.createDataFrame(configs, config_schema).createOrReplaceTempView("seed_config")
spark.sql(f"""
MERGE INTO {space}.pipeline_config AS t
USING seed_config AS s
ON t.pipeline_id = s.pipeline_id
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------
# Only synthetic data is generated. Keep existing files on reruns so
# Auto Loader file discovery does not need to reinterpret overwritten files.
base = f"/Volumes/{catalog}/{schema}/landing"
os.makedirs(f"{base}/customers", exist_ok=True)
os.makedirs(f"{base}/transactions", exist_ok=True)
customer_file = f"{base}/customers/customers_batch_001.csv"
if not os.path.exists(customer_file):
    with open(customer_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "name", "region"])
        writer.writerows([
            ("C001", "Alice", "East"),
            ("C002", "Bob", "West"),
            ("C003", "Clara", "East"),
            ("", "Unknown", "West"),  # deliberately invalid
            ("C001", "Alice", "East"), # deliberately duplicate
        ])
transaction_file = f"{base}/transactions/transactions_batch_001.json"
if not os.path.exists(transaction_file):
    transactions = [
        {"transaction_id":"T001","customer_id":"C001","amount":"120.00","transaction_date":"2026-11-01"},
        {"transaction_id":"T002","customer_id":"C002","amount":"50.00","transaction_date":"2026-11-02"},
        {"transaction_id":"T003","customer_id":"C003","amount":"-9.00","transaction_date":"2026-11-02"},
        {"transaction_id":"T004","customer_id":"C999","amount":"17.00","transaction_date":"2026-11-02"},
        {"transaction_id":"T005","customer_id":None,"amount":"42.00","transaction_date":"2026-11-03"},
        {"transaction_id":"T001","customer_id":"C001","amount":"120.00","transaction_date":"2026-11-01"},
    ]
    with open(transaction_file, "w", encoding="utf-8") as f:
        for transaction in transactions:
            f.write(json.dumps(transaction) + "\n")
print(f"Setup complete. Catalog: {catalog}. Schema: {schema}.")
print(f"Metadata: {catalog}.{schema}.pipeline_config")
print(f"Synthetic files: {base}/customers and {base}/transactions")
display(spark.table(f"{catalog}.{schema}.pipeline_config"))
