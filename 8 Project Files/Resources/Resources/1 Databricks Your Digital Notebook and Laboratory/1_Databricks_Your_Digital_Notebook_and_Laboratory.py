# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Your Digital Notebook and Laboratory 

# COMMAND ----------

print("Hello world")

# COMMAND ----------

# MAGIC %md
# MAGIC ## For example;
# MAGIC - 1
# MAGIC - 2

# COMMAND ----------

x = 7
y = 10
print(x)
print(y)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW students AS
# MAGIC SELECT 1 AS id, "Alice" AS name
# MAGIC UNION ALL
# MAGIC SELECT 2, "Bob"
# MAGIC UNION ALL
# MAGIC SELECT 3, "Charlie";
# MAGIC
# MAGIC SELECT * FROM students;

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns