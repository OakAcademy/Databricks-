# Databricks notebook source
# MAGIC %fs ls /

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/

# COMMAND ----------

# MAGIC %sh ps

# COMMAND ----------

# MAGIC %sh whoami

# COMMAND ----------

# MAGIC %pip list

# COMMAND ----------

from wordcloud import WordCloud

# COMMAND ----------

# MAGIC %pip install wordcloud 

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from wordcloud import WordCloud
import matplotlib.pyplot as plt

# Sample text
text = """
Databricks notebooks are powerful tools for data analysis and machine learning.
Data engineers use Databricks every day for ETL, big data processing, and SQL queries.
Databricks helps with data pipelines, AI, and collaboration in the cloud.
"""

# Create a WordCloud object
wordcloud = WordCloud(width=800, height=400, background_color="white").generate(text)

# Visualize the WordCloud
plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.show()

# COMMAND ----------

# MAGIC %run "/Workspace/Resources/2 Essential Notebook Commands in Databricks/Helper_Notebook"

# COMMAND ----------

print(APP_MODE)

# COMMAND ----------

welcome_message("Joseph")

# COMMAND ----------

subtract_numbers(10, 5)

# COMMAND ----------

system_summary()