/*
===============================================================================
Data Segmentation Analysis
===============================================================================
Purpose:
    - To group data into meaningful categories for targeted insights.
    - For customer segmentation, product categorization, or regional analysis.

SQL Functions Used:
    - CASE: Defines custom segmentation logic.
    - GROUP BY: Groups data into segments.
===============================================================================
*/



-- Segment products into cost ranges and count
-- how many products fall into each category

WITH cost_segmentation AS (

SELECT
product_key,
cost,
CASE WHEN cost < 100 then 'Below 100'
	 WHEN cost > 100 and cost < 500 then '100-500'
	 WHEN cost > 500 and cost < 1000 then '500-1000'
	 ELSE 'Above 1000'
END as cost_segmentation
FROM gold.dim_products
)
SELECT
cost_segmentation,
COUNT(product_key) products
FROM cost_segmentation
GROUP BY cost_segmentation
ORDER BY products DESC;

-- Group customers into three segments based on their spending behaviour:
-- VIP: at least 12 months of history and spending more then 5.000
-- Regular: at least 12 months of history but spending 5.000 or less
-- New: less than 12 months

WITH customer_segmentation AS (
  SELECT
    c.customer_key,
    SUM(f.sales_amount) AS total_spent,
    EXTRACT(YEAR FROM AGE(MAX(f.order_date), MIN(f.order_date))) * 12
    + EXTRACT(MONTH FROM AGE(MAX(f.order_date), MIN(f.order_date)))
    + 1 AS lifespan
  FROM gold.fact_sales f
  JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
  GROUP BY c.customer_key
)
SELECT
  customer_segment,
  COUNT(customer_key) AS total_customers
FROM (
  SELECT
    customer_key,
    CASE
      WHEN lifespan >= 12 AND total_spent >= 5000 THEN 'VIP'
      WHEN lifespan >= 12 AND total_spent < 5000 THEN 'Regular'
      ELSE 'New'
    END AS customer_segment
  FROM customer_segmentation
) t
GROUP BY customer_segment
ORDER BY total_customers DESC;
