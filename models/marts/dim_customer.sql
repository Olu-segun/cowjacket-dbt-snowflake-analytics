SELECT
    customer_id,
    first_name,
    last_name,
    email,

    MIN(first_order_date) AS first_order_date,
    MAX(total_orders) AS total_orders

FROM {{ ref('int_customer_orders') }}
GROUP BY
    customer_id,
    first_name,
    last_name,
    email
ORDER BY customer_id

