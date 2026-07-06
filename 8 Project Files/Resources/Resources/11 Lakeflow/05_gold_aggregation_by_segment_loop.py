# Databricks notebook source
from pyspark.sql import functions as F

segment = dbutils.widgets.get("segment")

table_name = f"course_training_catalog.bronze_ecommerce_orders.gold_state_segment_sales_summary_{segment}"

query = f"""
CREATE OR REPLACE TABLE {table_name} AS
SELECT
    state,
    customer_segment,
    COUNT(*) AS total_orders,
    ROUND(SUM(net_amount), 2) AS total_sales_amount,
    ROUND(AVG(delivery_days), 2) AS avg_delivery_days
FROM course_training_catalog.bronze_ecommerce_orders.silver_us_ecommerce_orders
WHERE customer_segment = '{segment}'
GROUP BY state, customer_segment
"""

spark.sql(query)

print(f"Created table: {table_name}")
display(spark.table(table_name))