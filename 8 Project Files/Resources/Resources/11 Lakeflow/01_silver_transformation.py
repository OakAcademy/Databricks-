# Databricks notebook source
# Read data from Bronze table
df = spark.table("course_training_catalog.bronze_ecommerce_orders.lakeflow_connect_orders_us")

display(df)

# COMMAND ----------

# Inspect schema to understand structure and data types

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# Convert all column names to lowercase for consistency

df = df.select([col(c).alias(c.lower()) for c in df.columns])

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col

# Convert string columns into proper data types

df = df.withColumn("order_timestamp", to_timestamp("order_timestamp")) \
       .withColumn("delivered_timestamp", to_timestamp("delivered_timestamp")) \
       .withColumn("unit_price", col("unit_price").cast("double")) \
       .withColumn("quantity", col("quantity").cast("int")) \
       .withColumn("net_amount", col("net_amount").cast("double")) \
       .withColumn("delivery_days", col("delivery_days").cast("int"))

# COMMAND ----------

from pyspark.sql.functions import when

# Replace null values in selected columns

df = df.fillna({
    "coupon_code": "NO_COUPON",
    "campaign_name": "UNKNOWN"
})

# Handle invalid values (e.g., negative revenue)

df = df.withColumn(
    "net_amount",
    when(col("net_amount") < 0, 0).otherwise(col("net_amount"))
)

# COMMAND ----------

from pyspark.sql.functions import when

# Create a new column to classify delivery performance

df = df.withColumn(
    "delivery_status",
    when(col("is_late_delivery") == True, "late")
    .otherwise("on_time")
)

# COMMAND ----------

# Preview transformed dataset

display(df)

# COMMAND ----------

# Save transformed data into Silver layer

df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("course_training_catalog.bronze_ecommerce_orders.silver_us_ecommerce_orders")

# COMMAND ----------

# Read and validate the Silver table

df_silver = spark.table("course_training_catalog.bronze_ecommerce_orders.silver_us_ecommerce_orders")

display(df_silver)