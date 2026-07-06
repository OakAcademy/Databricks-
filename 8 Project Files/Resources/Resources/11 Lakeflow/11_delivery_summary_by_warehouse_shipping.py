# Databricks notebook source

dbutils.widgets.text("warehouse_code", "")
dbutils.widgets.text("shipping_type", "")

warehouse_code = dbutils.widgets.get("warehouse_code")
shipping_type = dbutils.widgets.get("shipping_type")

print(f"Warehouse: {warehouse_code}")
print(f"Shipping Type: {shipping_type}")

query = f"""
SELECT
    warehouse_code,
    shipping_type,
    COUNT(*) AS total_orders,
    ROUND(SUM(net_amount), 2) AS total_sales_amount,
    ROUND(AVG(delivery_days), 2) AS avg_delivery_days,
    SUM(CASE WHEN delivery_status = 'late' THEN 1 ELSE 0 END) AS late_orders,
    SUM(CASE WHEN delivery_status = 'on_time' THEN 1 ELSE 0 END) AS on_time_orders,
    ROUND(
        100.0 * SUM(CASE WHEN delivery_status = 'late' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS late_order_percentage
FROM course_training_catalog.bronze_ecommerce_orders.silver_us_ecommerce_orders
WHERE warehouse_code = '{warehouse_code}'
  AND shipping_type = '{shipping_type}'
GROUP BY warehouse_code, shipping_type
"""

result_df = spark.sql(query)

display(result_df)

target_table = "course_training_catalog.bronze_ecommerce_orders.gold_delivery_summary_by_warehouse_shipping"

result_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(target_table)

print(f"Appended results to: {target_table}")