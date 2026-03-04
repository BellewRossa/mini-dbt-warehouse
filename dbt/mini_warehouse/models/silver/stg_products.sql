WITH src AS (
    SELECT *
    FROM {{ source('raw', 'raw_products') }}
),

typed AS (
    SELECT
        NULLIF(TRIM(product_id), '') AS product_id,
        NULLIF(TRIM(product_name), '') AS product_name,
        CASE
            WHEN category IS NULL THEN NULL
            ELSE NULLIF(LOWER(TRIM(category)), '')
        END AS category,
        TRY_CAST(unit_cost AS DOUBLE) AS unit_cost
    FROM src
)

SELECT
    product_id,
    product_name,
    category,
    unit_cost
FROM typed
WHERE product_id IS NOT NULL
