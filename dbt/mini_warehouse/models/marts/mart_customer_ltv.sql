WITH f AS (
    SELECT *
    FROM {{ ref('fact_order_items') }}
),

customer_orders AS (
    SELECT
        customer_id,
        order_id,
        MIN(CAST(order_ts AS DATE)) AS order_date,
        SUM(revenue) AS order_revenue
    FROM f
    WHERE customer_id IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    customer_id,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    SUM(order_revenue) AS lifetime_revenue,
    COUNT(DISTINCT order_id) AS order_count
FROM customer_orders
GROUP BY 1