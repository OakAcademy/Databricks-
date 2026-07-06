import dlt
from pyspark.sql.functions import count, sum, avg

@dlt.table(
    name="order_items_summary",
    comment="Aggregated summary table built from the view"
)
def order_items_summary():

    df = spark.read.table("order_items_view")

    df = df.groupBy("price_segment").agg(
        count("*").alias("total_orders"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_order_value")
    )

    return df