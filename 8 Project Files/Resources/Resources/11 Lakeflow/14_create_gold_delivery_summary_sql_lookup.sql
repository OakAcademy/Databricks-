CREATE TABLE IF NOT EXISTS course_training_catalog.bronze_ecommerce_orders.gold_delivery_summary_by_warehouse_shipping_sql_lookup (
    warehouse_code STRING,
    shipping_type STRING,
    total_orders BIGINT,
    total_sales_amount DOUBLE,
    avg_delivery_days DOUBLE,
    late_orders BIGINT,
    on_time_orders BIGINT,
    late_order_percentage DOUBLE
)
USING DELTA;