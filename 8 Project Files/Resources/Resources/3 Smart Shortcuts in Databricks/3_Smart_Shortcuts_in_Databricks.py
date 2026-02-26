# Databricks notebook source
# MAGIC %fs ls /

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/")

# COMMAND ----------

entries = dbutils.fs.ls("/databricks-datasets/")

# COMMAND ----------

largest_entries = sorted(entries, key=lambda x: x.size, reverse=True)

# COMMAND ----------

# Print top 5 largest entries
print("📦 Top 5 largest files/folders in /databricks-datasets:")
for entry in largest_entries[:5]:
    print(f"{entry.path} --> {entry.size/1024:.2f} KB")