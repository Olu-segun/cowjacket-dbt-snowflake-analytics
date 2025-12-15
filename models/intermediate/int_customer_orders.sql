WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_items_agg AS (
    SELECT
        order_id,
        SUM(quantity) AS total_quantity
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
),

loyalty_points_agg AS (
    SELECT
        customer_id,
        SUM(points_earned) AS total_points
    FROM {{ ref('stg_loyalty_points') }}
    GROUP BY customer_id
),

customer_orders AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        o.order_id,
        o.order_date,
        o.total_amount,
        i.total_quantity,
        l.total_points,

        COUNT(o.order_id) OVER (
            PARTITION BY c.customer_id
        ) AS total_orders,

        SUM(i.total_quantity) OVER (
            PARTITION BY c.customer_id
        ) AS quantity_order,

        MIN(o.order_date) OVER (
            PARTITION BY c.customer_id
        ) AS first_order_date

    FROM customers c
    LEFT JOIN orders o
        ON c.customer_id = o.customer_id
    LEFT JOIN order_items_agg i
        ON o.order_id = i.order_id
    LEFT JOIN loyalty_points_agg l
        ON c.customer_id = l.customer_id
)

SELECT *
FROM customer_orders
ORDER BY customer_id
