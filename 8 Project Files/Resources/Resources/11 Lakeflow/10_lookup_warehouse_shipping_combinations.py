# Databricks notebook source

query = """
SELECT DISTINCT
    warehouse_code,
    shipping_type
FROM course_training_catalog.bronze_ecommerce_orders.silver_us_ecommerce_orders
WHERE warehouse_code IS NOT NULL
  AND shipping_type IS NOT NULL
ORDER BY warehouse_code, shipping_type
"""

df = spark.sql(query)

rows = df.collect()

combination_array = [
    {
        "warehouse_code": row["warehouse_code"],
        "shipping_type": row["shipping_type"]
    }
    for row in rows
]

array_length = len(combination_array)

print("Generated combinations:")
print(combination_array[:10])
print(f"Total combinations: {array_length}")

dbutils.jobs.taskValues.set(key="combination_array", value=combination_array)
dbutils.jobs.taskValues.set(key="array_length", value=array_length)