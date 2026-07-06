import dlt
from pyspark.sql.functions import col, when

@dlt.view(
    name="order_items_view"
)
def order_items_view():

    df = spark.read.table("order_items_stg")

    df = df.withColumn(
        "price_segment",
        when(col("total_amount") < 50, "low")
        .when(col("total_amount") < 150, "medium")
        .otherwise("high")
    )

    df = df.withColumn(
        "has_freight",
        when(col("freight_value") > 0, True).otherwise(False)
    )

    return df