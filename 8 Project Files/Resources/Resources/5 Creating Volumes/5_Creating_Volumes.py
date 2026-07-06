# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM csv.`/Volumes/course_training_catalog/bronze_olist/olist_volumes/orders/orders.csv`
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_orders
# MAGIC FROM csv.`/Volumes/course_training_catalog/bronze_olist/olist_volumes/orders/orders.csv`

# COMMAND ----------

# MAGIC %fs ls /Volumes/course_training_catalog/bronze_olist/olist_volumes

# COMMAND ----------

from PIL import Image
import matplotlib.pyplot as plt

path = "/Volumes/course_training_catalog/bronze_olist/olist_volumes/images/olist_data_schema.png"

img = Image.open(path)
plt.imshow(img)
plt.axis("off")
plt.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM binaryFile.`/Volumes/course_training_catalog/bronze_olist/olist_volumes/images/*.png`