import dlt
from pyspark.sql.functions import col

@dlt.table(
    name = "order_items_stg",
    comment = "Streaming staging table built from bronze order items"
)

def order_items_stg():
    df = spark.readStream.table("course_training_catalog.bronze_olist.order_items")

    df = df.withColumn(
        "total_amount",
        col("price") + col("freight_value")
    )

    return df