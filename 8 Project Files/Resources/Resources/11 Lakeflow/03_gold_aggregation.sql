CREATE OR REPLACE TABLE course_training_catalog.bronze_ecommerce_orders.gold_state_category_sales_summary AS

SELECT
    state,
    product_category,
    COUNT(*) AS total_orders,
    ROUND(SUM(net_amount), 2) AS total_sales_amount,
    ROUND(AVG(delivery_days), 2) AS avg_delivery_days,
    ROUND(
        100.0 * SUM(CASE WHEN is_late_delivery = true THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS late_delivery_rate_pct
FROM course_training_catalog.bronze_ecommerce_orders.silver_us_ecommerce_orders
GROUP BY state, product_category;
