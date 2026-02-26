# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM course_training_catalog.bronze_olist.orders LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 'orders' AS table_name, COUNT(*) AS row_cnt FROM course_training_catalog.bronze_olist.orders
# MAGIC UNION ALL
# MAGIC SELECT 'order_items', COUNT(*) FROM course_training_catalog.bronze_olist.order_items;
# MAGIC

# COMMAND ----------

# PySpark
df = spark.table("course_training_catalog.bronze_olist.order_items")
display(df.select("price", "freight_value").summary("count","mean","min","max"))

# COMMAND ----------

