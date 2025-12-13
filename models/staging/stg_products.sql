WITH raw_products AS (
    SELECT * 
    FROM {{ref("src_product")}}
)

SELECT 
    product_id,
    TRIM(PRODUCT_NAME) AS product_name,
    category,
    price,
    -- Flag high-value products (e.g., price above 10,000)
    CASE WHEN price > 10000 THEN TRUE ELSE FALSE END AS is_premium,
    -- Categorize price range
    CASE 
        WHEN price < 5000 THEN 'Low'
        WHEN price BETWEEN 5000 AND 15000 THEN 'Medium'
        ELSE 'High'
    END AS price_tier
FROM raw_products
