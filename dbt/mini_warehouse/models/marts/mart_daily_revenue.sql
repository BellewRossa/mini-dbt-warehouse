WITH f AS (
    SELECT *
    FROM {{ ref('fact_order_items') }}
),

daily AS (
    SELECT
        CAST(order_ts AS DATE) AS date,
        SUM(revenue) AS total_revenue,
        COUNT(DISTINCT order_id) AS total_orders
    FROM f
    WHERE order_status != 'cancelled'
    GROUP BY 1
)

SELECT
    date,
    total_revenue,
    total_orders,
    total_revenue / NULLIF(total_orders, 0) AS aov,
    SUM(total_revenue) OVER (
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_revenue
FROM daily
ORDER BY date;