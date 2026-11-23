# Databricks notebook source
"""A single generic notebook handles every configured source."""
import re
from pyspark.sql import functions as F

# COMMAND ----------
dbutils.widgets.text("catalog", "", "Unity Catalog name (blank = current catalog)")
dbutils.widgets.text("pipeline_id", "customers", "Pipeline ID")
catalog = dbutils.widgets.get("catalog").strip() or spark.sql("SELECT current_catalog()").first()[0]
pipeline_id = dbutils.widgets.get("pipeline_id").strip()
pattern = r"[A-Za-z_][A-Za-z0-9_]*"
if not re.fullmatch(pattern, catalog) or not re.fullmatch(pattern, pipeline_id):
    raise ValueError("Invalid catalog or pipeline ID")
schema = "md_framework"
rows = (
    spark.table(f"`{catalog}`.`{schema}`.pipeline_config")
    .filter((F.col("pipeline_id") == pipeline_id) & (F.col("active") == True))
    .limit(2).collect()
)
if len(rows) != 1:
    raise ValueError(f"Expected exactly one active config row for {pipeline_id}")
cfg = rows[0].asDict()
for key in ["input_subdir", "bronze_table", "silver_table", "rule_set"]:
    if not re.fullmatch(pattern, cfg[key]):
        raise ValueError(f"Invalid control-table value: {key}")
if cfg["file_format"] not in {"csv", "json", "parquet"}:
    raise ValueError("Only CSV, JSON and Parquet are supported")
if cfg["rule_set"] not in {"customers", "transactions"}:
    raise ValueError("Unknown validation rule_set; add an explicit handler first")

volumes = f"/Volumes/{catalog}/{schema}"
source_path = f"{volumes}/landing/{cfg['input_subdir']}"
state_path = f"{volumes}/state/{pipeline_id}"
bronze = f"`{catalog}`.`{schema}`.`{cfg['bronze_table']}`"
silver = f"`{catalog}`.`{schema}`.`{cfg['silver_table']}`"
quarantine = f"`{catalog}`.`{schema}`.`quarantine_{pipeline_id}`"

# COMMAND ----------
# Auto Loader remembers which files were ingested through its per-source checkpoint.
reader = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", cfg["file_format"])
    .option("cloudFiles.schemaLocation", f"{state_path}/schema")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
)
if cfg["file_format"] == "csv":
    reader = reader.option("header", "true")
raw = reader.load(source_path)
raw = (
    raw.withColumn("_source_file", F.col("_metadata.file_path"))
       .withColumn("_ingested_at", F.current_timestamp())
)
query = (
    raw.writeStream.format("delta")
    .option("checkpointLocation", f"{state_path}/stream_checkpoint")
    .trigger(availableNow=True)
    .toTable(bronze)
)
query.awaitTermination()

# COMMAND ----------
# For a small portfolio dataset, rebuild the Silver snapshot from Bronze.
# This is idempotent for unchanged input. For large production tables, use
# incremental MERGE / change processing instead of full snapshot overwrites.
bronze_df = spark.table(bronze)
if cfg["rule_set"] == "customers":
    data = bronze_df.select(
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.trim(F.col("name")).alias("name"),
        F.trim(F.col("region")).alias("region"),
        "_source_file", "_ingested_at",
    )
    data = data.withColumn("error_reason", F.when(
        F.col("customer_id").isNull() | (F.col("customer_id") == ""),
        F.lit("missing customer_id")
    ).when(
        F.col("region").isNull() | (F.col("region") == ""),
        F.lit("missing region")
    ))
    dedupe_key = ["customer_id"]
else:
    data = bronze_df.select(
        F.trim(F.col("transaction_id")).alias("transaction_id"),
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.col("amount").cast("decimal(12,2)").alias("amount"),
        F.to_date(F.col("transaction_date")).alias("transaction_date"),
        "_source_file", "_ingested_at",
    )
    data = data.withColumn("error_reason", F.when(
        F.col("transaction_id").isNull() | (F.col("transaction_id") == ""),
        F.lit("missing transaction_id")
    ).when(
        F.col("customer_id").isNull() | (F.col("customer_id") == ""),
        F.lit("missing customer_id")
    ).when(
        F.col("amount").isNull() | (F.col("amount") < 0),
        F.lit("invalid amount")
    ).when(
        F.col("transaction_date").isNull(),
        F.lit("invalid transaction_date")
    ))
    dedupe_key = ["transaction_id"]

good = data.filter(F.col("error_reason").isNull()).drop("error_reason")
bad = data.filter(F.col("error_reason").isNotNull())
# The seeded duplicate rows are identical. Conflicting duplicate keys require
# a real deterministic business resolution policy (e.g., a source update timestamp).
good = good.dropDuplicates(dedupe_key)
good.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver)
bad.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(quarantine)

print(f"Processed {pipeline_id}: bronze={spark.table(bronze).count()}, "
      f"silver={spark.table(silver).count()}, quarantine={spark.table(quarantine).count()}")
