/*
===============================================================================
Cumulative Analysis
===============================================================================
Purpose:
    - To calculate running totals or moving averages for key metrics.
    - To track performance over time cumulatively.
    - Useful for growth analysis or identifying long-term trends.

SQL Functions Used:
    - Window Functions: SUM() OVER(), AVG() OVER()
===============================================================================
*/



-- Calculate the total sales per month
-- and the running total of sales over time

SELECT
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
AVG(avg_price) OVER (ORDER BY order_date) AS moving_average_price
FROM
(
SELECT
CAST(DATE_TRUNC('month', order_date) as DATE) as order_date,
SUM(sales_amount) as total_sales,
COUNT(DISTINCT customer_key) as total_customers,
SUM(quantity) as total_quantity,
AVG(price) as avg_price
FROM gold.fact_sales
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY order_date
) t
;



