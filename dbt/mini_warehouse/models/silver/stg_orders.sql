WITH src AS (
    SELECT *
    FROM {{ source('raw', 'raw_orders') }}
),

typed AS (
    SELECT
        nullif(trim(order_id), '') AS order_id,
        nullif(trim(customer_id), '') AS customer_id,
        try_cast(order_ts AS timestamp) AS order_ts,
        nullif(lower(trim(status)), '') AS status
    FROM src
)

SELECT
    order_id,
    customer_id,
    order_ts,
    status
FROM typed
WHERE order_id IS NOT NULL
