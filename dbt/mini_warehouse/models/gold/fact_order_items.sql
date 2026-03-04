WITH order_items AS (
    SELECT *
    FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),

products AS (
    SELECT *
    FROM {{ ref('stg_products') }}
)

SELECT
    oi.order_id,
    o.order_ts,
    o.customer_id,
    oi.product_id,
    oi.quantity,
    oi.unit_price,

    -- Metrics
    o.status AS order_status,
    oi.quantity * oi.unit_price AS revenue,
    oi.quantity * p.unit_cost AS total_cost,
    (oi.quantity * oi.unit_price) - (oi.quantity * p.unit_cost) AS gross_margin

FROM order_items AS oi
LEFT JOIN orders AS o
    ON oi.order_id = o.order_id
LEFT JOIN products AS p
    ON oi.product_id = p.product_id
