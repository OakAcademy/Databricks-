CREATE OR REPLACE TABLE course_training_catalog.bronze_ecommerce_orders.silver_us_ecommerce_orders_quality_check AS

SELECT
    current_timestamp() AS quality_check_timestamp,
    COUNT(*) AS total_row_count,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id_count,
    SUM(CASE WHEN net_amount < 0 THEN 1 ELSE 0 END) AS negative_net_amount_count,
    SUM(CASE WHEN state IS NULL OR TRIM(state) = '' THEN 1 ELSE 0 END) AS null_state_count,
    SUM(CASE 
            WHEN order_status NOT IN ('completed', 'shipped', 'cancelled', 'returned') 
            OR order_status IS NULL 
            THEN 1 
            ELSE 0 
        END) AS invalid_order_status_count
FROM course_training_catalog.bronze_ecommerce_orders.lakeflow_connect_orders_us;
