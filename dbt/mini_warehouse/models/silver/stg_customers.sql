WITH src AS (
    SELECT *
    FROM {{ source('raw', 'raw_customers') }}
),

typed AS (
    SELECT
        NULLIF(TRIM(customer_id), '') AS customer_id,
        TRY_CAST(first_seen_date AS DATE) AS first_seen_date,
        NULLIF(TRIM(country), '') AS country,
        NULLIF(LOWER(TRIM(email)), '') AS email
    FROM src
),

deduped AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY first_seen_date ASC NULLS LAST
            ) AS rn
        FROM typed
        WHERE customer_id IS NOT NULL
    )
    WHERE rn = 1
)

SELECT
    customer_id,
    first_seen_date,
    country,
    email
FROM deduped
