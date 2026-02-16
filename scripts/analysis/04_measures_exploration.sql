/*
===============================================================================
Measures Exploration (Key Metrics)
===============================================================================
Purpose:
    - To calculate aggregated metrics (e.g., totals, averages) for quick insights.
    - To identify overall trends or spot anomalies.

SQL Functions Used:
    - COUNT(), SUM(), AVG()
===============================================================================
*/


-- Find the total sales
SELECT sum(sales_amount) as total_sales from gold.fact_sales;
-- Find how many items are sold
SELECT SUM(quantity) as total_items_sold from gold.fact_sales;
-- Find the average selling price
SELECT AVG(price) as average_price from gold.fact_sales;
-- Find the total number of orders
SELECT COUNT(DISTINCT order_number) as total_orders from gold.fact_sales;
-- Find the total number of products
SELECT COUNT(product_key) as total_products from gold.dim_products;
-- Find the total number of customers
SELECT COUNT(customer_key) as total_customers from gold.dim_customers;
-- Find the total number of customers that has placed an order
SELECT COUNT(DISTINCT customer_key) as total_customers_that_ordered from gold.fact_sales;

SELECT measure_name, measure_value
FROM (
    SELECT 1 AS sort_order, 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value
    FROM gold.fact_sales

    UNION ALL
    SELECT 2, 'Total Quantity', SUM(quantity)
    FROM gold.fact_sales

    UNION ALL
    SELECT 3, 'Average Price', AVG(price)
    FROM gold.fact_sales

    UNION ALL
    SELECT 4, 'Total Orders', COUNT(DISTINCT order_number)
    FROM gold.fact_sales

    UNION ALL
    SELECT 5, 'Total Products', COUNT(product_key)
    FROM gold.dim_products

    UNION ALL
    SELECT 6, 'Total Customers', COUNT(customer_key)
    FROM gold.dim_customers
) kpis
ORDER BY sort_order;
