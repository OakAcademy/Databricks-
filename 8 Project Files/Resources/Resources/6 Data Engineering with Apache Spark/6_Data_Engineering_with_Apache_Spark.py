# Databricks notebook source
# MAGIC %md
# MAGIC # **BRONZE LAYER**

# COMMAND ----------

# MAGIC %md
# MAGIC ## First ETL Steps(Extract) 

# COMMAND ----------

BASE = "/Volumes/course_training_catalog/bronze_olist/olist_volumes/orders/"
display(dbutils.fs.ls(BASE))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM csv.`dbfs:/Volumes/course_training_catalog/bronze_olist/olist_volumes/orders/orders.csv`
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _metadata
# MAGIC FROM csv.`dbfs:/Volumes/course_training_catalog/bronze_olist/olist_volumes/orders/orders.csv`
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _metadata.file_path, *
# MAGIC FROM csv.`dbfs:/Volumes/course_training_catalog/bronze_olist/olist_volumes/orders/orders.csv`
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   _c0 as order_id,
# MAGIC   _c1 as customer_id,
# MAGIC   _c2 AS order_status,
# MAGIC   _c3 AS order_purchase_timestamp,
# MAGIC   _c4 AS order_approved_at,
# MAGIC   _c5 AS order_delivered_carrier_date,
# MAGIC   _c6 AS order_delivered_customer_date,
# MAGIC   _c7 AS order_estimated_delivery_date
# MAGIC
# MAGIC FROM csv.`dbfs:/Volumes/course_training_catalog/bronze_olist/olist_volumes/orders/orders.csv`
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   t._c0 as order_id,
# MAGIC   t._c1 as customer_id,
# MAGIC   t._c2 AS order_status,
# MAGIC   t._c3 AS order_purchase_timestamp,
# MAGIC   t._c4 AS order_approved_at,
# MAGIC   t._c5 AS order_delivered_carrier_date,
# MAGIC   t._c6 AS order_delivered_customer_date,
# MAGIC   t._c7 AS order_estimated_delivery_date
# MAGIC
# MAGIC FROM 
# MAGIC   (
# MAGIC   SELECT *
# MAGIC   FROM  
# MAGIC   csv.`dbfs:/Volumes/course_training_catalog/bronze_olist/olist_volumes/orders/orders.csv`
# MAGIC   ) AS t
# MAGIC
# MAGIC WHERE t._c0 <> "order_id"
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM read_files("/Volumes/course_training_catalog/bronze_olist/olist_volumes/orders/orders.csv", format => "csv");

# COMMAND ----------

# MAGIC %md
# MAGIC read_file documentation link: https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files

# COMMAND ----------

df_orders = spark.read.format("csv").option("inferSchema", True).option("header", True).load("dbfs:/Volumes/course_training_catalog/bronze_olist/olist_volumes/orders/orders.csv")

# COMMAND ----------

display(df_orders)

# COMMAND ----------

base_path = "/Volumes/course_training_catalog/bronze_olist/olist_volumes"

# Customers
df_customers = (spark.read.format("csv")
                .option("header", True)
                .option("inferSchema", True)
                .load(f"{base_path}/customers/customers.csv"))

# Geolocation
df_geolocation = (spark.read.format("csv")
                  .option("header", True)
                  .option("inferSchema", True)
                  .load(f"{base_path}/customers/geolocation.csv"))


# Order Items
df_items = (spark.read.format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .load(f"{base_path}/orders/order_items.csv"))

# Order Payments
df_payments = (spark.read.format("csv")
               .option("header", True)
               .option("inferSchema", True)
               .load(f"{base_path}/orders/order_payments.csv"))

# Order Reviews
df_reviews = (spark.read.format("csv")
              .option("header", True)
              .option("inferSchema", True)
              .load(f"{base_path}/orders/order_reviews.csv"))

# Products
df_products = (spark.read.format("csv")
               .option("header", True)
               .option("inferSchema", True)
               .load(f"{base_path}/products/products.csv"))

# Product Category Translation
df_categories = (spark.read.format("csv")
                 .option("header", True)
                 .option("inferSchema", True)
                 .load(f"{base_path}/products/product_category_name_translation.csv"))

# Sellers
df_sellers = (spark.read.format("csv")
              .option("header", True)
              .option("inferSchema", True)
              .load(f"{base_path}/sellers/sellers.csv"))

# COMMAND ----------

display(df_customers)

# COMMAND ----------

display(df_categories)

# COMMAND ----------

df_orders.printSchema()

# COMMAND ----------

df_table_products = spark.table("course_training_catalog.bronze_olist.products")

# COMMAND ----------

display(df_table_products)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Schema & Sample Data Checks

# COMMAND ----------

# Put all Dataframes in a dictionary
dataframes = {
    "orders": df_orders,
    "customers": df_customers,
    "sellers": df_sellers,
    "products": df_products,
    "product_category": df_categories,
    "order_items": df_items,
    "order_payments": df_payments,
    "order_reviews": df_reviews,
    "geolocation": df_geolocation
}

for name, df in dataframes.items():
  print(f"\n===={name.upper()}====")
  df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Quick Notes
# MAGIC
# MAGIC ### **Orders**
# MAGIC
# MAGIC * 8 columns; `order_id` and `customer_id` are strings, all date columns correctly inferred as *timestamp*.
# MAGIC * **Key:** `order_id` should be the primary key → check for duplicates.
# MAGIC * **Critical column:** `order_status` is categorical (delivered, shipped, canceled…). Distribution should be inspected.
# MAGIC * Some timestamp columns (`order_approved_at`, `order_delivered_customer_date`) may contain null values → handle later in Silver.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Customers**
# MAGIC
# MAGIC * 5 columns; `customer_id` is string, `customer_zip_code_prefix` is integer.
# MAGIC * **Key:** `customer_id` should be unique, but the same `customer_unique_id` can map to multiple `customer_id`s.
# MAGIC * **Critical columns:** `customer_city`, `customer_state` → distributions worth checking.
# MAGIC * Zip codes stored as integers → useful for address analysis.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Sellers**
# MAGIC
# MAGIC * 4 columns; `seller_id` string, `seller_zip_code_prefix` integer.
# MAGIC * **Key:** `seller_id` should be unique.
# MAGIC * City/state fields can be used for distribution checks.
# MAGIC * Zip code info can be cross-checked with Customers for address consistency in Silver.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Products**
# MAGIC
# MAGIC * 9 columns; product dimensions, weight, description length, photo qty all inferred as integers.
# MAGIC * **Key:** `product_id` should be unique → check duplicates.
# MAGIC * **Critical column:** `product_category_name`. Number of categories and distribution should be noted.
# MAGIC * Weight/size fields may contain zero or extreme values → clean later in Silver.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Product Category Translation**
# MAGIC
# MAGIC * 2 columns; Portuguese and English category names.
# MAGIC * Acts as a lookup table to standardize category names.
# MAGIC * Just check structure in Bronze; join with Products in Silver.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Order Items**
# MAGIC
# MAGIC * Contains line-item details; links to Orders, Products, and Sellers.
# MAGIC * **Key:** combination of `order_id` + `order_item_id` should be unique.
# MAGIC * `price` and `freight_value` are doubles → check for nulls/zeros.
# MAGIC * `shipping_limit_date` correctly inferred as timestamp.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Order Payments**
# MAGIC
# MAGIC * Payment details; linked to Orders via `order_id`.
# MAGIC * **Key:** multiple rows per order possible → `payment_sequential` used as ordering field.
# MAGIC * **Critical column:** `payment_type` (credit_card, boleto, voucher…). Distribution analysis recommended.
# MAGIC * `payment_value` is double → later check consistency with total order value in Silver.
# MAGIC * `payment_installments` important for installment analysis.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Order Reviews**
# MAGIC
# MAGIC * Customer reviews; linked to Orders via `order_id`.
# MAGIC * **Key:** `review_id` should be unique.
# MAGIC * **Critical column:** `review_score` → important for quality analysis but inferred as string; convert to integer in Silver.
# MAGIC * `review_creation_date` and `review_answer_timestamp` are strings → convert to timestamp in Silver.
# MAGIC * Review texts (`review_comment_message`) should be inspected for length/null issues.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Geolocation**
# MAGIC
# MAGIC * Contains geocoordinates linked to zip prefixes.
# MAGIC * No unique key; multiple coordinates may exist per prefix.
# MAGIC * `geolocation_lat`, `geolocation_lng` are doubles → check for nulls.
# MAGIC * City/state fields should be cross-checked with Customers/Sellers in Silver.
# MAGIC * Functions as a lookup table for address/location analysis.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detecting Duplicate Keys in the Bronze Layer

# COMMAND ----------

dataframes = {
    "orders": (df_orders, ["order_id"]),
    "customers": (df_customers, ["customer_id"]),
    "sellers": (df_sellers, ["seller_id"]),
    "products": (df_products, ["product_id"]),
    "product_category": (df_categories, ["product_category_name"]),
    "order_items": (df_items, ["order_id", "order_item_id"]),
    "order_payments": (df_payments, ["order_id", "payment_sequential"]),
    "order_reviews": (df_reviews, ["review_id"])
}

# Simple dublicate check
for name, (df, keys) in dataframes.items():
    total = df.count()
    distinct = df.select(*keys).distinct().count()
    print(f"{name.upper()}: total = {total}, distinct = {distinct}, dublicates = {total - distinct}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Bronze Layer Notes – Order Reviews
# MAGIC
# MAGIC * **1204 duplicates** detected in the `review_id` column.
# MAGIC * This means the same `review_id` appears in more than one row.
# MAGIC * Possible reasons:
# MAGIC
# MAGIC   * Records might have been written multiple times during data loading,
# MAGIC   * Or different variations of the same review may have been stored.
# MAGIC * In the **Bronze layer**, we only **record this issue**; no cleaning is performed yet.
# MAGIC * In the **Silver layer**, the duplicates must be addressed by:
# MAGIC
# MAGIC   * Removing exact duplicate records,
# MAGIC   * Considering the combination of `review_id` + `order_id` as the key,
# MAGIC   * Or keeping only the most recent/consistent version of the review.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Missing Value Audit

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import(DoubleType, FloatType, LongType, ShortType, DecimalType)

dataframes = {
    "orders": df_orders,
    "customers": df_customers,
    "sellers": df_sellers,
    "products": df_products,
    "product_category": df_categories,
    "order_items": df_items,
    "order_payments": df_payments,
    "order_reviews": df_reviews,
    "geolocation": df_geolocation
}

# COMMAND ----------

def safe_missing_report(df):
    """
    Missing = NULL + trimmed empty string ("") + NaN (only for numeric columns)
    Returns a Spark DF with columns: ["column", "missing_count", "missing_ratio"]
    """
    total = df.count()
    rows = []

    for field in df.schema.fields:
        c = field.name
        dt = field.dataType

        # NULL and empty string checks for all columns
        is_null = F.col(c).isNull()
        is_empty = (F.trim(F.col(c).cast("string")) == "")

        # NaN check only for float/double types
        if isinstance(dt, (DoubleType, FloatType)):
            is_nan = F.isnan(F.col(c))

        else:
            # No NaN concept for Integer/Long/Short/Decimal/String
            is_nan = F.lit(False)

        cond = is_null | is_empty | is_nan
        missing = df.filter(cond).count()
        ratio = (missing / total) if total > 0 else 0.0
        rows.append((c, int(missing), float(ratio)))

    return spark.createDataFrame(rows, ["column", "missing_count", "missing_ratio"])


# Run for all tables
for name, df in dataframes.items():
    print(f"\n===={name.upper()}====")
    safe_missing_report(df).orderBy("missing_count", ascending = False).show(truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Olist — Bronze Missing Value Profile
# MAGIC
# MAGIC ## Goal & Method
# MAGIC
# MAGIC * **Goal:** In the Bronze layer, profile missing values **without modifying data** to plan precise cleanup/standardization in Silver.
# MAGIC * **“Missing” definition:**
# MAGIC
# MAGIC   * **NULL**
# MAGIC   * **Trimmed empty string** (`""` or whitespace-only)
# MAGIC   * **NaN** *(only for Float/Double columns)*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Quick Summary
# MAGIC
# MAGIC * **Orders:** Expected gaps on delivery timestamps (to-customer ≈ **2.98%**, to-carrier ≈ **1.79%**).
# MAGIC * **Customers / Sellers / Geolocation / Order Items / Order Payments / Category Translation:** No missing values (or negligible).
# MAGIC * **Products:** Category/summary fields ≈ **1.85%** missing; physical dimensions missing in **~0.006%** of rows (2 products).
# MAGIC * **Order Reviews:** Text fields very sparse (title ≈ **88.48%**, message ≈ **60.57%** missing). Also `order_id` ≈ **2.15%** and `review_score` ≈ **2.28%** missing.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Table-by-Table Details
# MAGIC
# MAGIC ### 1) `orders`
# MAGIC
# MAGIC | Column                          | Missing (count) | Ratio      |
# MAGIC | ------------------------------- | --------------- | ---------- |
# MAGIC | `order_delivered_customer_date` | 2965            | ~**2.98%** |
# MAGIC | `order_delivered_carrier_date`  | 1783            | ~**1.79%** |
# MAGIC | `order_approved_at`             | 160             | ~**0.16%** |
# MAGIC | *(others)*                      | 0               | 0.00%      |
# MAGIC
# MAGIC **Interpretation:** Delivery gaps typically come from **canceled/not-delivered** orders.
# MAGIC **Silver note:** Compute delivery/latency metrics **only for relevant statuses** (e.g., `delivered`) and **non-null** timestamps. Cast time fields to proper `timestamp`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2) `customers`, `sellers`, `geolocation`, `order_items`, `order_payments`, `product_category_name_translation`
# MAGIC
# MAGIC * **Missing:** None / negligible.
# MAGIC * **Interpretation:** Strong join backbone; dimensional references look clean.
# MAGIC * **Silver note:** `geolocation` can be multi-row per prefix; if you need a single city/state per prefix, standardize with a clear rule (e.g., most-frequent).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3) `products`
# MAGIC
# MAGIC | Column(s)                                                                              | Missing (count) | Ratio       |
# MAGIC | -------------------------------------------------------------------------------------- | --------------- | ----------- |
# MAGIC | `product_category_name`, `*_name_lenght`, `*_description_lenght`, `product_photos_qty` | 610             | ~**1.85%**  |
# MAGIC | `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm`       | 2               | ~**0.006%** |
# MAGIC
# MAGIC **Interpretation:** Category/summary fields have minor gaps; physical measurements are almost fully populated.
# MAGIC **Silver note:**
# MAGIC
# MAGIC * Fix naming consistency: `*_lenght` → `*_length` (preserve lineage to original Bronze names).
# MAGIC * Cast numeric fields appropriately. Keep NULLs for measurements; optional category-level median imputation can be tested separately.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4) `order_reviews`
# MAGIC
# MAGIC | Column                    | Missing (count) | Ratio       |
# MAGIC | ------------------------- | --------------- | ----------- |
# MAGIC | `review_comment_title`    | 92159           | ~**88.48%** |
# MAGIC | `review_comment_message`  | 63088           | ~**60.57%** |
# MAGIC | `review_answer_timestamp` | 8785            | ~**8.43%**  |
# MAGIC | `review_creation_date`    | 8764            | ~**8.41%**  |
# MAGIC | `review_score`            | 2380            | ~**2.28%**  |
# MAGIC | `order_id`                | 2240            | ~**2.15%**  |
# MAGIC | `review_id`               | 1               | ~**0.001%** |
# MAGIC
# MAGIC **Interpretation:** Real-world pattern—many users leave a **score only** with no text. Missing `order_id` harms joins; missing `review_score` impacts rating metrics.
# MAGIC **Silver note:**
# MAGIC
# MAGIC * Park rows with NULL `order_id` outside core analytics; exclude NULL `review_score` from rating KPIs (avoid imputing scores).
# MAGIC * Check potential duplicate `review_id`; if found, keep the most recent `review_creation_date`.
# MAGIC * Run text analytics (sentiment/topic) **only** on non-empty messages.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Silver Layer Action Plan
# MAGIC
# MAGIC 1. **Types & Naming**
# MAGIC
# MAGIC    * Cast timestamps/numerics to correct types across all tables.
# MAGIC    * Standardize product name fields (`*_length`), while maintaining lineage to Bronze names.
# MAGIC 2. **Missing-Data Strategy**
# MAGIC
# MAGIC    * **Orders:** Calculate delivery KPIs only on relevant statuses and complete timestamps.
# MAGIC    * **Reviews:** Exclude rows with NULL `order_id`/`review_score` from respective analyses; limit NLP to non-empty text.
# MAGIC    * **Products:** Leave measurement NULLs; optionally test median-by-category imputation (report impact).
# MAGIC 3. **Data Quality (DQ) Checks / Expectations**
# MAGIC
# MAGIC    * `orders.order_id` unique; `order_items.order_id` all exist in `orders`.
# MAGIC    * `order_reviews.review_id` unique; track `% NULL order_id` and `% NULL review_score`.
# MAGIC    * Persist the missing-value profile as a Delta table for monitoring over time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Profiling

# COMMAND ----------

display(df_orders)

# COMMAND ----------

dbutils.data.summarize(df_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning and Normalizing Customers Table

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG course_training_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver_olist;

# COMMAND ----------

display(df_customers)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW customer_base AS
# MAGIC SELECT DISTINCT
# MAGIC   customer_id,
# MAGIC   customer_unique_id,
# MAGIC   customer_zip_code_prefix,
# MAGIC   customer_city,
# MAGIC   customer_state
# MAGIC FROM course_training_catalog.bronze_olist.customers
# MAGIC WHERE customer_id IS NOT NULL; 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customer_base LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW customers_norm AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   customer_unique_id,
# MAGIC   CAST(customer_zip_code_prefix AS INT) AS customer_zip_code_prefix,
# MAGIC   LOWER(TRIM(regexp_replace(customer_city, '\s+', ' '))) AS customer_city,
# MAGIC   UPPER(TRIM(regexp_replace(customer_state, '\s+', ' '))) AS customer_state
# MAGIC
# MAGIC FROM customer_base; 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_olist.customers
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   customer_unique_id,
# MAGIC   customer_zip_code_prefix,
# MAGIC   customer_city,
# MAGIC   customer_state
# MAGIC FROM customers_norm; 

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE silver_olist.customers IS
# MAGIC 'Source: bronze_olist.customers.
# MAGIC Silver transformations: PK hygiene (DISTINCT, non-null), city lowercase+trim, state uppercase+trim, zip INT cast.
# MAGIC Purpose: clean, analysis-ready customer dimension.';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED silver_olist.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Olist Sellers data: Transforming Bronze to Silver

# COMMAND ----------

display(df_sellers)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW sellers_nn AS
# MAGIC SELECT 
# MAGIC   seller_id,
# MAGIC   seller_zip_code_prefix,
# MAGIC   seller_city,
# MAGIC   seller_state
# MAGIC FROM course_training_catalog.bronze_olist.sellers
# MAGIC WHERE seller_id IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW sellers_base AS
# MAGIC SELECT *
# MAGIC FROM sellers_nn
# MAGIC QUALIFY ROW_NUMBER() OVER(
# MAGIC   PARTITION BY seller_id
# MAGIC   ORDER BY 
# MAGIC     seller_city ASC NULLS LAST,
# MAGIC     seller_state ASC NULLS LAST,
# MAGIC     seller_zip_code_prefix ASC NULLS LAST
# MAGIC ) = 1;

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df_base = spark.table("sellers_base")

# COMMAND ----------

display(df_base)

# COMMAND ----------

df_norm = (df_base.withColumn("seller_zip_code_prefix", F.col("seller_zip_code_prefix").cast("int")).withColumn("seller_city", F.lower(F.trim(F.regexp_replace(F.col("seller_city"), r"\s+", " ")))).withColumn("seller_state", F.upper(F.trim(F.regexp_replace(F.col("seller_state"), r"\s+", " ")))))

# COMMAND ----------

display(df_norm)

# COMMAND ----------

df_norm.select("seller_id","seller_zip_code_prefix","seller_city","seller_state").write.mode("overwrite").format("delta").saveAsTable("course_training_catalog.silver_olist.sellers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS n_rows,
# MAGIC   COUNT(DISTINCT seller_id) AS unique_ids,
# MAGIC   COUNT(*) - COUNT(DISTINCT seller_id) AS dupes,
# MAGIC   COUNT(CASE WHEN seller_id IS NULL THEN 1 END) AS null_ids
# MAGIC FROM course_training_catalog.silver_olist.sellers;

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE course_training_catalog.silver_olist.sellers IS
# MAGIC 'Source: course_training_catalog.bronze_olist.sellers.
# MAGIC Silver: PK hygiene (ROW_NUMBER per seller_id), city/state normalization, zip INT cast.
# MAGIC Note: Profile hinted dupes; exact SQL found none. DISTINCT kept as hygiene.';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG course_training_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED silver_olist.sellers;