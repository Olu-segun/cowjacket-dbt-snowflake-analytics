SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,

    COUNT(o.order_id) AS total_orders,
    SUM(o.total_quantity) AS quantity_order,
    MIN(o.order_date) AS first_order_date

FROM {{ ref('int_orders') }} o
JOIN {{ ref('stg_customers') }} c
  ON o.customer_id = c.customer_id
GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email
ORDER BY customer_id
