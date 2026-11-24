# Databricks notebook source
"""Build customer-region transaction totals from all active transaction sources."""
import re
from functools import reduce
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "", "Unity Catalog name (blank = current catalog)")
catalog = dbutils.widgets.get("catalog").strip() or spark.sql("SELECT current_catalog()").first()[0]
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError("Invalid catalog")
schema = "md_framework"
settings = spark.table(f"`{catalog}`.`{schema}`.pipeline_config").filter(F.col("active") == True).collect()
customers = [r for r in settings if r["rule_set"] == "customers"]
transactions = [r for r in settings if r["rule_set"] == "transactions"]
if len(customers) != 1 or not transactions:
    raise ValueError("The demo requires exactly one customer feed and at least one transaction feed")

def tbl(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError("Invalid table name")
    return spark.table(f"`{catalog}`.`{schema}`.`{name}`")

customer_df = tbl(customers[0]["silver_table"]).select("customer_id", "region")
transaction_frames = [tbl(r["silver_table"]).select(
    "transaction_id", "customer_id", "amount", "transaction_date"
) for r in transactions]
all_transactions = reduce(lambda a, b: a.unionByName(b), transaction_frames)
# A transaction ID must be unique across sources in this toy example.
# Real cross-source deduplication requires source-specific IDs or a composite key.
all_transactions = all_transactions.dropDuplicates(["transaction_id"])
with_region = all_transactions.join(customer_df, "customer_id", "left")
orphans = with_region.filter(F.col("region").isNull())
valid = with_region.filter(F.col("region").isNotNull())
gold = valid.groupBy("region").agg(
    F.countDistinct("transaction_id").alias("transaction_count"),
    F.sum("amount").cast("decimal(14,2)").alias("total_amount"),
)

gold.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(
    f"`{catalog}`.`{schema}`.`gold_transactions_by_region`"
)
orphans.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(
    f"`{catalog}`.`{schema}`.`quarantine_orphan_transactions`"
)
print("Gold summary:")
display(spark.table(f"`{catalog}`.`{schema}`.`gold_transactions_by_region`"))
print(f"Unknown customer references found: {orphans.count()}")
