# Databricks notebook source
# Global variables
APP_MODE = "testing"
CREATED_BY = "Databricks Academy"
RELEASE_ID = "2025.1"

# COMMAND ----------

# Function 1: Welcome message
def welcome_message(user):
    return f"Hi {user}, you are now working in {APP_MODE} mode."

# COMMAND ----------

# Function 2: Subtract two numbers
def subtract_numbers(x, y):
    return x - y

# COMMAND ----------

# Function 3: System summary
def system_summary():
    return {
        "mode": APP_MODE,
        "creator": CREATED_BY,
        "release": RELEASE_ID
    }

# COMMAND ----------

# Function 4: Average of a list
def average_list(values):
    if not values:
        return 0
    return sum(values) / len(values)