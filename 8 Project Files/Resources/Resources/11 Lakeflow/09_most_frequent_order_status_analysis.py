# Databricks notebook source

dbutils.widgets.text("order_status", "")
dbutils.widgets.text("status_count", "")

order_status = dbutils.widgets.get("order_status")
status_count = dbutils.widgets.get("status_count")

print(f"Most frequent order status: {order_status}")
print(f"Frequency: {status_count}")

query = f"""
SELECT
    state,
    order_status,
    COUNT(*) AS total_orders,
    ROUND(SUM(net_amount), 2) AS total_sales_amount,
    ROUND(AVG(delivery_days), 2) AS avg_delivery_days
FROM course_training_catalog.bronze_ecommerce_orders.silver_us_ecommerce_orders
WHERE order_status = '{order_status}'
GROUP BY state, order_status
ORDER BY total_orders DESC
"""

result_df = spark.sql(query)

display(result_df)

table_name = f"course_training_catalog.bronze_ecommerce_orders.gold_most_frequent_order_status_{order_status}"

result_df.write.mode("overwrite").saveAsTable(table_name)

print(f"Created table: {table_name}")
display(spark.table(table_name))