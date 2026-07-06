# Databricks notebook source
catalog_name = "course_training_catalog"
schema_name  = "streaming_demo"
volume_name  = "streaming_volume"

base_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/autoloader_lab"

source_dir     = f"{base_path}/source/orders_raw"
checkpoint_dir = f"{base_path}/checkpoints/orders_autoloader"
schema_dir     = f"{checkpoint_dir}/schemas"
sink_path      = f"{base_path}/sink/orders_delta"

print("source_dir    :", source_dir)
print("checkpoint_dir:", checkpoint_dir)
print("schema_dir    :", schema_dir)
print("sink_path     :", sink_path)

# COMMAND ----------

dbutils.fs.mkdirs(source_dir)
dbutils.fs.mkdirs(checkpoint_dir)
dbutils.fs.mkdirs(schema_dir)
dbutils.fs.mkdirs(sink_path)

dbutils.fs.rm(checkpoint_dir, True)
dbutils.fs.rm(sink_path, True)
dbutils.fs.rm(source_dir, True)

dbutils.fs.mkdirs(source_dir)
dbutils.fs.mkdirs(checkpoint_dir)
dbutils.fs.mkdirs(schema_dir)
dbutils.fs.mkdirs(sink_path)

print("✅ Fresh lab folders ready.")

# COMMAND ----------

df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemalocation", schema_dir)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("header", "true")
        .load(source_dir)
)

df.printSchema()

# COMMAND ----------

query = (
    df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(once=True)
        .start(sink_path)
)

display(query)

# COMMAND ----------

query.awaitTermination()
print("Auto Loader ingestion finished.")

# COMMAND ----------

display(dbutils.fs.ls(sink_path))

# COMMAND ----------

out_df = spark.read.format("delta").load(sink_path)
display(out_df.orderBy("order_ts", ascending=False))

# COMMAND ----------

query = (
    df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(once=True)
        .start(sink_path)
)

display(query)

# COMMAND ----------

query.awaitTermination()

# COMMAND ----------

out_df = spark.read.format("delta").load(sink_path)
display(out_df.orderBy("order_ts", ascending=False))

# COMMAND ----------

query = (
    df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(once=True)
        .start(sink_path)
)

display(query)

# COMMAND ----------

query.awaitTermination()

# COMMAND ----------

out_df = spark.read.format("delta").load(sink_path)
display(out_df.orderBy("order_ts", ascending=False))

# COMMAND ----------

display(dbutils.fs.ls(checkpoint_dir))
display(dbutils.fs.ls(f"{checkpoint_dir}/sources"))
display(dbutils.fs.ls(f"{checkpoint_dir}/offsets"))
display(dbutils.fs.ls(f"{checkpoint_dir}/commits"))
display(dbutils.fs.ls(schema_dir))

# COMMAND ----------

