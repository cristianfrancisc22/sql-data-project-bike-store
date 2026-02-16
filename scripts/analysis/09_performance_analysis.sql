/*
===============================================================================
Performance Analysis (Year-over-Year, Month-over-Month)
===============================================================================
Purpose:
    - To measure the performance of products, customers, or regions over time.
    - For benchmarking and identifying high-performing entities.
    - To track yearly trends and growth.

SQL Functions Used:
    - LAG(): Accesses data from previous rows.
    - AVG() OVER(): Computes average values within partitions.
    - CASE: Defines conditional logic for trend analysis.
===============================================================================
*/


-- Analyze the yearly performance of products by comparing their sales
-- to both the average sales performance of the product and the previous year's sales
WITH yearly_sales as (
SELECT
p.product_name,
EXTRACT('year' from order_date) as order_year,
SUM(sales_amount) as total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name, order_year
) 
SELECT
order_year,
product_name,
total_sales,
CAST(AVG(total_sales) OVER (PARTITION BY product_name) as INT) as avg_sales,
CAST(total_sales - AVG(total_sales) OVER (PARTITION BY product_name) as INT) as avg_diff,
CASE WHEN total_sales - AVG(total_sales) OVER (PARTITION BY product_name) > 0 then 'Above'
	 WHEN total_sales - AVG(total_sales) OVER (PARTITION BY product_name) < 0 then 'Below'
	 ELSE 'Average'
END AS avg_change,
LAG(total_sales) OVER (PARTITION BY product_name ORDER BY order_year ASC) as py_sales,
total_sales - LAG(total_sales) OVER (PARTITION BY product_name ORDER BY order_year ASC) as py_difference,
CASE WHEN total_sales - LAG(total_sales) OVER (PARTITION BY product_name ORDER BY order_year ASC) > 0 then 'Increase'
	 WHEN total_sales - LAG(total_sales) OVER (PARTITION BY product_name ORDER BY order_year ASC) < 0 then 'Decrease'
	 ELSE 'No change'
END AS py_change
FROM yearly_sales;