WITH filtered_orders AS (
    SELECT order_id, product_id, customer_id, quantity, price
    FROM orders
    WHERE quantity > 1
)
SELECT 
    c.category_name,
    COUNT(DISTINCT f.customer_id) AS total_customers,
    SUM(f.quantity * f.price) AS total_revenue
FROM filtered_orders f
JOIN products p 
    ON f.product_id = p.product_id
JOIN categories c 
    ON p.category_id = c.category_id
GROUP BY c.category_name
ORDER BY total_revenue DESC;


--1) filter the dataset before join (cte)
--2) add appropriate indexes
