# Databricks notebook source
"""Simple acceptance checks; run after Gold publishing."""
import re
from decimal import Decimal

dbutils.widgets.text("catalog", "", "Unity Catalog name (blank = current catalog)")
catalog = dbutils.widgets.get("catalog").strip() or spark.sql("SELECT current_catalog()").first()[0]
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError("Invalid catalog")
prefix = f"`{catalog}`.`md_framework`"
def count(table):
    return spark.table(f"{prefix}.`{table}`").count()

assert count("bronze_customers") == 5
assert count("silver_customers") == 3
assert count("quarantine_customers") == 1
assert count("bronze_transactions") == 6
assert count("silver_transactions") == 3
assert count("quarantine_transactions") == 2
assert count("quarantine_orphan_transactions") == 1
partner_active = spark.sql(f"SELECT count(*) AS n FROM {prefix}.pipeline_config "
                           "WHERE pipeline_id = 'partner_transactions' AND active").first()[0] > 0
if partner_active:
    assert count("silver_partner_transactions") == 2
    expected = Decimal("265.00")
else:
    expected = Decimal("170.00")
result = spark.table(f"{prefix}.`gold_transactions_by_region`")
actual = result.agg({"total_amount": "sum"}).first()[0]
assert actual == expected, f"Expected {expected}, found {actual}"
print(f"PASS: All validation checks passed. Gold total = {actual}")
