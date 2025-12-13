

WITH raw_loyalty_points AS (
    SELECT *
    FROM {{ref("src_loyalty_points")}}
)
    SELECT
    loyalty_id,
    customer_id,
    points_earned,
    transaction_date,
    LOWER(SOURCE) AS source
FROM raw_loyalty_points
