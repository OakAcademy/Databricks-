CREATE OR REPLACE TABLE course_training_catalog.bronze_ecommerce_orders.gold_state_segment_sales_summary AS

SELECT
    state,
    customer_segment,
    COUNT(*) AS total_orders,
    ROUND(SUM(net_amount), 2) AS total_sales_amount,
    ROUND(AVG(delivery_days), 2) AS avg_delivery_days
FROM course_training_catalog.bronze_ecommerce_orders.silver_us_ecommerce_orders
GROUP BY state, customer_segment;



