/*
==================================================================================
Customer Analytics View
==================================================================================
Purpose:
    - Provides a current-state, customer-level analytics view
    - Designed for segmentation, behavior analysis, and value ranking

Grain:
    - One row per customer

Data Scope:
    - Based on all historical completed orders
    - Metrics are calculated up to the current date (non-historical snapshot)

Key Features:
    1. Customer attributes
        - Full name, age, age group, country

    2. Customer segmentation
        - Lifecycle segment (VIP / Regular / New) based on lifespan and spend
        - Age group segmentation

    3. Activity metrics
        - Total orders
        - Recency (months since last order)
        - Customer lifespan (months active)
        - Average orders, quantities, and product variety per month

    4. Value metrics
        - Total sales
        - Average order value
        - Average monthly sales

    5. Behavioral metrics
        - Average units per order
        - Average items (order lines) per order

    6. Breadth metrics
        - Number of distinct products purchased

Notes:
    - This view represents the current customer state and is not time-series based
    - Intended for BI tools, segmentation, and downstream analytics (e.g. churn, CLV tiers)
==================================================================================
*/

CREATE VIEW gold.customers_analytics AS 
WITH base_query AS (
SELECT
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, ' ', c.last_name) as customer_name,
EXTRACT(YEAR FROM AGE(c.birthdate)) as age,
c.cntry as country
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE order_date IS NOT NULL
), customer_aggregation AS (
SELECT
	customer_key,
	customer_number,
	customer_name,
	age,
	country,
	COUNT(DISTINCT order_number) as total_orders,
	COUNT(product_key) as total_order_lines,
	COUNT(DISTINCT product_key) as distinct_products,
	SUM(sales_amount) as total_sales,
	SUM(quantity) as total_quantity,
	MAX(order_date) as latest_order,
	EXTRACT(YEAR FROM AGE(MAX(order_date),MIN(order_date))) * 12
	+EXTRACT(MONTH FROM AGE(MAX(order_date),MIN(order_date)))
	+1 as lifespan
FROM base_query
GROUP BY
	customer_key,
	customer_number,
	customer_name,
	age,
	country
)
SELECT
	customer_key,
	customer_number,
	customer_name,
	age,
	CASE 
	 	WHEN age < 20 THEN 'Under 20'
	 	WHEN age between 20 and 29 THEN '20-29'
	 	WHEN age between 30 and 39 THEN '30-39'
	 	WHEN age between 40 and 49 THEN '40-49'
		ELSE '50 and above'
	END AS age_group,
	CASE 
		WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
		WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
    	ELSE 'New'
	END AS customer_segment,
	country,
	-- Activity
	EXTRACT(YEAR FROM AGE(latest_order)) * 12
	+EXTRACT(MONTH FROM AGE(latest_order))
	+1 as recency,
	total_orders,
	total_quantity,
	latest_order, -- When was the last order placed?
	total_orders / NULLIF(GREATEST(lifespan, 1), 0) as avg_orders_per_month, -- How many orders does a customer place per month?
	total_quantity / NULLIF(GREATEST(lifespan, 1), 0) as avg_quantity_per_month, -- How many products does a customer buy per month?
	distinct_products / NULLIF(GREATEST(lifespan, 1), 0) as avg_products_per_month, -- How many different products does a customer buy per month?
	lifespan, -- How long has the customer been active?
	-- Value
	total_sales,
	CASE
		WHEN total_sales = 0 THEN 0
		ELSE total_sales / total_orders
	END AS avg_order_value,
	CASE
		WHEN lifespan = 0 then total_sales
		ELSE total_sales / lifespan
	END AS avg_sales_per_month,
	-- Behaviour Bulk, single-item, or bundle buyers?
	total_quantity / total_orders as avg_units_per_order,
	total_order_lines / total_orders as avg_items_per_order,
	avg_order_value * avg_orders_per_month * lifespan
	-- Breadth
	distinct_products -- How diverse is their interest?
FROM customer_aggregation;


