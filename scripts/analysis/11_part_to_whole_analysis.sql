/*
===============================================================================
Part-to-Whole Analysis
===============================================================================
Purpose:
    - To compare performance or metrics across dimensions or time periods.
    - To evaluate differences between categories.
    - Useful for A/B testing or regional comparisons.

SQL Functions Used:
    - SUM(), AVG(): Aggregates values for comparison.
    - Window Functions: SUM() OVER() for total calculations.
===============================================================================
*/


-- Which categories contribute the most to overall sales?
WITH category_sales AS (
SELECT
category,
SUM(sales_amount) as total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY category)

SELECT
category,
total_sales,
SUM(total_sales) OVER() as overall_sales,
CONCAT(ROUND((total_sales / SUM(total_sales) OVER ())*100, 2),'%') as percentage_total
FROM category_sales
ORDER BY total_sales DESC;