WITH src AS (
    SELECT *
    FROM {{ source('raw', 'raw_order_items') }}
),

typed AS (
    SELECT
        NULLIF(TRIM(order_id), '') AS order_id,
        NULLIF(TRIM(product_id), '') AS product_id,
        TRY_CAST(quantity AS INTEGER) AS quantity,
        TRY_CAST(unit_price AS DOUBLE) AS unit_price
    FROM src
)

SELECT
    order_id,
    product_id,
    quantity,
    unit_price
FROM typed
WHERE
    order_id IS NOT NULL
    AND product_id IS NOT NULL
