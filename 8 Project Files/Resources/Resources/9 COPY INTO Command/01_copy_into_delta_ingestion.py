# Databricks notebook source
catalog = "course_training_catalog"
schema  = "streaming_demo"
volume  = "streaming_volume"

base_path = f"/Volumes/{catalog}/{schema}/{volume}/copy_into_lab"
incoming_csv = f"{base_path}/incoming_csv"

dbutils.fs.mkdirs(incoming_csv)

print("Upload your CSV files here:")
print(incoming_csv)

# COMMAND ----------

display(dbutils.fs.ls(incoming_csv))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files(
# MAGIC   "/Volumes/course_training_catalog/streaming_demo/streaming_volume/copy_into_lab/incoming_csv/",
# MAGIC   format => "csv",
# MAGIC   header => true
# MAGIC )
# MAGIC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE course_training_catalog.streaming_demo.bronze_orders_copyinto (
# MAGIC   order_id        STRING,
# MAGIC   customer_id     STRING,
# MAGIC   product_id      STRING,
# MAGIC   category        STRING,
# MAGIC   amount          STRING,
# MAGIC   quantity        STRING,
# MAGIC   payment_method  STRING,
# MAGIC   order_status    STRING,
# MAGIC   order_ts        STRING,
# MAGIC   coupon_code     STRING,
# MAGIC   shipping_city   STRING,
# MAGIC   discount_rate   STRING,
# MAGIC   _rescued_data   STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO course_training_catalog.streaming_demo.bronze_orders_copyinto
# MAGIC FROM "/Volumes/course_training_catalog/streaming_demo/streaming_volume/copy_into_lab/incoming_csv/"
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS (
# MAGIC   "header" = "true",
# MAGIC   "mode" = "PERMESSIVE"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM course_training_catalog.streaming_demo.bronze_orders_copyinto;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE course_training_catalog.streaming_demo.silver_orders_copyinto AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   product_id,
# MAGIC   category,
# MAGIC
# MAGIC   CAST(REPLACE(NULLIF(amount, "null"), "i", ".") AS DOUBLE) AS amount,
# MAGIC   CAST(NULLIF(quantity, "null") AS INT) AS quantity,
# MAGIC
# MAGIC   NULLIF(payment_method, "null") AS payment_method,
# MAGIC   NULLIF(order_status, "null") AS order_status,
# MAGIC  
# MAGIC   TO_TIMESTAMP(NULLIF(order_ts, "null")) AS order_ts,
# MAGIC
# MAGIC   NULLIF(coupon_code, "null") AS coupon_code,
# MAGIC   NULLIF(shipping_city, "null") AS shipping_city,
# MAGIC   
# MAGIC   CAST(REPLACE(NULLIF(discount_rate, 'null'), ',', '.') AS DOUBLE) AS discount_rate,
# MAGIC   NULLIF(_rescued_data, "null") AS _rescued_data
# MAGIC
# MAGIC FROM course_training_catalog.streaming_demo.bronze_orders_copyinto;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE course_training_catalog.streaming_demo.silver_orders_copyinto;