WITH cte AS (
  SELECT a.*, b.revenue
  FROM product_hierarchy a
  JOIN sales b 
  ON a.product_id = b.product_id
),
cte2 AS (
  SELECT 
    category,
    subcategory,
    product_name,
    SUM(revenue) AS agg_revenue,
    GROUPING(category) AS grp_category,
    GROUPING(subcategory) AS grp_sub,
    GROUPING(product_name) AS grp_prd
  FROM cte
  GROUP BY ROLLUP (category, subcategory, product_name)
)
SELECT 
  category,
  subcategory,
  product_name,
  agg_revenue,
  CASE
    WHEN grp_category = 0 AND grp_sub = 0 AND grp_prd = 0
      THEN ROUND(100.0 * agg_revenue /
                 SUM(agg_revenue) FILTER (WHERE grp_prd = 0)
                 OVER (PARTITION BY category, subcategory), 2)
    WHEN grp_category = 0 AND grp_sub = 0 AND grp_prd = 1
      THEN ROUND(100.0 * agg_revenue /
                 SUM(agg_revenue) FILTER (WHERE grp_sub = 0 AND grp_prd = 1)
                 OVER (PARTITION BY category), 2)
    WHEN grp_category = 0 AND grp_sub = 1 AND grp_prd = 1
      THEN ROUND(100.0 * agg_revenue /
                 SUM(agg_revenue) FILTER (WHERE grp_category = 0 AND grp_sub = 1 AND grp_prd = 1)
                 OVER (), 2)
    ELSE 100.0
  END AS share_to_parent
FROM cte2
ORDER BY category, subcategory, product_name;
