SELECT
    order_status,
    COUNT(*) AS status_count
FROM course_training_catalog.bronze_ecommerce_orders.silver_us_ecommerce_orders
WHERE order_status IS NOT NULL
GROUP BY order_status
ORDER BY status_count DESC, order_status ASC;