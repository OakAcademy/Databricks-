# Databricks notebook source
# ==========================================
# Spark Structured Streaming - Introduction
# Independent demo (not related to Olist pipeline)
# Catalog : course_training_catalog
# Schema  : streaming_demo
# ==========================================
catalog_name = "course_training_catalog"
schema_name = "streaming_demo"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print("Using:", f"{catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS course_training_catalog.streaming_demo.streaming_volume;

# COMMAND ----------

# ==========================================
# Streaming folders using Unity Catalog Volume
# ==========================================

base_path = "/Volumes/course_training_catalog/streaming_demo/streaming_volume"

source_dir = f"{base_path}/price_updates_in"
checkpoint_dir = f"{base_path}/checkpoints/price_updates"


# COMMAND ----------

dbutils.fs.rm(source_dir, True)
dbutils.fs.rm(checkpoint_dir, True)

dbutils.fs.mkdirs(source_dir)
dbutils.fs.mkdirs(checkpoint_dir)

print("Source Directory:", source_dir)
print("Checkpoint Directory:", checkpoint_dir)

# COMMAND ----------

# ==========================================
# 1) Define schema and read the folder as a stream
# ==========================================

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, current_timestamp, input_file_name, to_timestamp

schema = StructType(
    [
    StructField("update_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("update_time", StringType(), True)
    ]
)

raw_stream_df = (
    spark.readStream
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(source_dir)
)

print("Is streaming:", raw_stream_df.isStreaming)

# COMMAND ----------

# ==========================================
# 2) Transform: parse update_time and add ingestion metadata
# NOTE: In Unity Catalog, use _metadata.file_path instead of input_file_name()
# ==========================================

from pyspark.sql.functions import col, current_timestamp, to_timestamp

transformed_df = (
    raw_stream_df
        .withColumn("update_time_ts", to_timestamp(col("update_time"), "yyyy-MM-dd HH:mm:ss"))
        .drop("update_time")
        .withColumn("ingestion_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
)

print("Is streaming:", transformed_df.isStreaming)

# COMMAND ----------

# ==========================================
# Continuous streaming: keep the query running
# New files dropped into source_dir will be processed automatically
# ==========================================

catalog_name = "course_training_catalog"
schema_name = "streaming_demo"
table_name = f"{catalog_name}.{schema_name}.product_price_updates_bronze"

# COMMAND ----------

query = (
    transformed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(availableNow=True)
        .toTable(table_name)
)

query.awaitTermination()
print("AvailableNow finished. New files were ingested.")

# COMMAND ----------

# ==========================================
# 4) Validate: run a normal SQL query against the Delta table
# ==========================================

display(spark.sql(f"SELECT * FROM {table_name} ORDER BY ingestion_time DESC"))

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS row_count FROM {table_name}"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM course_training_catalog.streaming_demo.product_price_updates_bronze;

# COMMAND ----------

query = (
    transformed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(availableNow=True)
        .toTable(table_name)
)

query.awaitTermination()

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {table_name} ORDER BY ingestion_time DESC"))

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS row_count FROM {table_name}"))