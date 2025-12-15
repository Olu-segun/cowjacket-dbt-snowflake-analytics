SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_amount,
    i.total_quantity
FROM {{ ref('stg_orders') }} o
LEFT JOIN (
    SELECT
        order_id,
        SUM(quantity) AS total_quantity
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
) i
    ON o.order_id = i.order_id
ORDER BY order_id