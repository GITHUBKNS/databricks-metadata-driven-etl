# Databricks notebook source
"""Extension: add a third feed by changing metadata, not the processor code."""
import json
import os
import re
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

dbutils.widgets.text("catalog", "", "Unity Catalog name (blank = current catalog)")
catalog = dbutils.widgets.get("catalog").strip() or spark.sql("SELECT current_catalog()").first()[0]
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError("Invalid catalog")
schema = "md_framework"
file_dir = f"/Volumes/{catalog}/{schema}/landing/partner_transactions"
os.makedirs(file_dir, exist_ok=True)
file_path = f"{file_dir}/transactions_batch_001.json"
if not os.path.exists(file_path):
    with open(file_path, "w", encoding="utf-8") as f:
        for item in [
            {"transaction_id":"T006","customer_id":"C003","amount":"35.00","transaction_date":"2026-11-04"},
            {"transaction_id":"T007","customer_id":"C002","amount":"60.00","transaction_date":"2026-11-05"},
        ]:
            f.write(json.dumps(item) + "\n")

row = [("partner_transactions", "json", "partner_transactions",
        "bronze_partner_transactions", "silver_partner_transactions", "transactions", True)]
sch = StructType([
    StructField("pipeline_id", StringType()), StructField("file_format", StringType()),
    StructField("input_subdir", StringType()), StructField("bronze_table", StringType()),
    StructField("silver_table", StringType()), StructField("rule_set", StringType()),
    StructField("active", BooleanType()),
])
spark.createDataFrame(row, sch).createOrReplaceTempView("new_config")
spark.sql(f"""
MERGE INTO `{catalog}`.`{schema}`.pipeline_config AS t
USING new_config AS s
ON t.pipeline_id = s.pipeline_id
WHEN NOT MATCHED THEN INSERT *
""")
print("Added partner_transactions. Now run 01_process_source with pipeline_id=partner_transactions;"
      " then rerun 02_build_gold and 03_check_results. No changes to ingestion code.")
