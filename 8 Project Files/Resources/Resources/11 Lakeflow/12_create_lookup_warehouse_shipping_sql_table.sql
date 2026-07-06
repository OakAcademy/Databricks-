CREATE TABLE IF NOT EXISTS course_training_catalog.bronze_ecommerce_orders.lookup_warehouse_shipping_sql (
    warehouse_code STRING,
    shipping_type STRING
)
USING DELTA;

DELETE FROM course_training_catalog.bronze_ecommerce_orders.lookup_warehouse_shipping_sql;

INSERT INTO course_training_catalog.bronze_ecommerce_orders.lookup_warehouse_shipping_sql
SELECT DISTINCT
    warehouse_code,
    shipping_type
FROM course_training_catalog.bronze_ecommerce_orders.silver_us_ecommerce_orders
WHERE warehouse_code IS NOT NULL
  AND shipping_type IS NOT NULL;