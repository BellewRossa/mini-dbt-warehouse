WITH customers AS (
    SELECT *
    FROM {{ ref('stg_customers') }}
)

SELECT
    customer_id,
    first_seen_date,
    country,
    email
FROM customers
