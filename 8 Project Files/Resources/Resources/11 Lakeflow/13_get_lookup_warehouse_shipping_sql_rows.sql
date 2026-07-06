SELECT
    warehouse_code,
    shipping_type
FROM course_training_catalog.bronze_ecommerce_orders.lookup_warehouse_shipping_sql
ORDER BY warehouse_code, shipping_type;