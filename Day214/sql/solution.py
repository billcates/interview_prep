SELECT 
    CASE 
        WHEN order_id % 2 = 1 
             AND order_id = (SELECT MAX(order_id) FROM orders)
        THEN order_id
        WHEN order_id % 2 = 1 THEN order_id + 1
        ELSE order_id - 1
    END AS order_id,
    item
FROM orders
ORDER BY order_id;