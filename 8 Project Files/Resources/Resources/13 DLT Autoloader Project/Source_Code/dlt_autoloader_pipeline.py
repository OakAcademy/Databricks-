import dlt
from pyspark.sql.functions import col, current_timestamp, when, count, sum, avg

source_path = "/Volumes/course_training_catalog/streaming_demo/streaming_volume/autoloader_lab/source/orders_raw/"

@dlt.table(
    name = "orders_raw_stg",
    comment = "Streaming table that ingests raw order files using Auto Loader"
)

def orders_raw_stg():
    return(
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(source_path)
    )

@dlt.table(
    name="orders_autoloader_enriched",
    comment="Enriched orders table created from Auto Loader streaming table"
)
def orders_autoloader_enriched():
    df = spark.read.table("orders_raw_stg")

    df = (
        df.withColumn("amount", col("amount").cast("double"))
          .withColumn("quantity", col("quantity").cast("int"))
          .withColumn("order_ts", col("order_ts").cast("timestamp"))
          .withColumn(
              "amount_segment",
              when(col("amount") >= 500, "high")
              .when(col("amount") >= 100, "medium")
              .otherwise("low")
          )
          .withColumn("processed_at", current_timestamp())
    )

    return df

@dlt.table(
    name="orders_autoloader_summary",
    comment="Summary table by amount segment and order status"
)
def orders_autoloader_summary():
    df = spark.read.table("orders_autoloader_enriched")

    df = (
        df.groupBy("amount_segment", "order_status")
          .agg(
              count("*").alias("total_orders"),
              sum("amount").alias("total_amount"),
              avg("amount").alias("avg_amount")
          )
    )

    return df












    






