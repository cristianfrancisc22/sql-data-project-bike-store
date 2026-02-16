/*
==================================================================================
Product Analytics Report
==================================================================================
Purpose:
    - Consolidates key product-level metrics and behaviours

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify high-performers, mid-range, or low-performers.
    3. Aggregates product-level metrics:
        - total orders
        - total sales
        - total quantity sold
        - total customers (unique)
        - lifespan (in months)
    4. Calculates valuable KPIs:
        - recency (months since last sale)
        - average selling price per unit
        - average monthly revenue
        - average units per order and per month
        - customer penetration (reach)
==================================================================================
*/

CREATE VIEW gold.product_analytics AS 
WITH base_query AS (
	SELECT
		p.product_key,
		p.product_name,
		p.category,
		p.subcategory,
		p.cost,
		f.order_date,
		f.quantity,
		f.sales_amount,
		f.order_number,
		f.customer_key
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
		ON f.product_key = p.product_key
),
product_aggregation AS (
	SELECT
		product_key,
		product_name,
		category,
		subcategory,
		cost,
		MAX(order_date) as latest_sale_date,
		MIN(order_date) as first_sale_date,
		EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12
		+EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date)))
		+1 as lifespan, -- This calculates the gap between months, and with +1 we involve the current month
		SUM(sales_amount) as total_sales,
		SUM(quantity) as total_quantity,
		COUNT(DISTINCT customer_key) as total_customers,
		COUNT(DISTINCT order_number) as total_orders,
		ROUND(AVG(sales_amount/quantity), 2) as avg_selling_price
	FROM base_query
	GROUP BY
		product_key,
		product_name,
		category,
		subcategory,
		cost
)
SELECT
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	-- Performance(Value) -> How much does the product generate?
	total_sales,
	total_quantity,
	total_orders,
	avg_selling_price,
	ROUND(total_sales/ lifespan, 2) as avg_sales_per_month,
	-- Demand & Velocity -> How fast does it sell?
	total_quantity / total_orders as avg_units_per_order,
	total_quantity / lifespan as avg_units_per_month,
	total_orders / lifespan as avg_orders_per_month,
	-- Reach -> How widely is it bought?
	total_customers,
    ROUND(total_customers::NUMERIC / (SELECT COUNT(DISTINCT customer_key) FROM gold.dim_customers), 4) AS customer_penetration,
	-- Lifecycle & Health -> Where is the product in its life?
	lifespan,
	first_sale_date,
	latest_sale_date,
	EXTRACT(YEAR FROM AGE(latest_sale_date)) * 12
	+ EXTRACT(MONTH FROM AGE(latest_sale_date))
	+1 as recency,
	CASE
		WHEN total_sales > 50000 THEN 'High-Performer'
		WHEN total_sales >= 10000 THEN 'Mid-Range'
		ELSE 'Low-Performer'
	END AS product_segment,
	total_sales - cost as profit
FROM product_aggregation;