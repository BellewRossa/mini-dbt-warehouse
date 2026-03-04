WITH f AS (
    SELECT *
    FROM {{ ref('fact_order_items') }}
),

p AS (
    SELECT *
    FROM {{ ref('dim_products') }}
),

agg AS (
    SELECT
        product_id,
        SUM(revenue) AS revenue,
        SUM(gross_margin) AS margin
    FROM f
    GROUP BY 1
)

SELECT
    a.product_id,
    p.product_name,
    p.category,
    a.revenue,
    a.margin,
    DENSE_RANK() OVER (ORDER BY a.revenue DESC) AS rank_by_revenue
FROM agg AS a
LEFT JOIN p
    ON a.product_id = p.product_id
ORDER BY rank_by_revenue
