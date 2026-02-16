/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/


CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    -- Overall execution timestamps
    total_start TIMESTAMP := clock_timestamp();
    total_end   TIMESTAMP;

    -- Step execution timestamps
    start_time TIMESTAMP;
    end_time   TIMESTAMP;
BEGIN

    RAISE NOTICE 'Starting Gold layer creation...';

    -----------------------------------------------------------------
    -- 1) Build Customer Dimension (gold.dim_customers)
    -----------------------------------------------------------------

    start_time := clock_timestamp();
    RAISE NOTICE 'Creating gold.dim_customers...';

    CREATE MATERIALIZED VIEW gold.dim_customers AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
        cst_id       AS customer_id,
        cst_key      AS customer_number,
        cst_firstname AS first_name,
        cst_lastname  AS last_name,
        cl.cntry      AS country,
        cst_marital_status AS marital_status,
        CASE
            WHEN cst_gndr <> 'n/a' THEN cst_gndr
            ELSE COALESCE(cu.gen, 'n/a')
        END AS gender,
        cu.bdate      AS birthdate,
        cst_create_date AS create_date
    FROM silver.crm_cust_info c
    LEFT JOIN silver.erp_cust_az12 cu
        ON c.cst_key = cu.cid
    LEFT JOIN silver.erp_loc_a101 cl
        ON c.cst_key = cl.cid;

    end_time := clock_timestamp();
    RAISE NOTICE 'gold.dim_customers created in % seconds',
        ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2);


    -----------------------------------------------------------------
    -- 2) Build Product Dimension (gold.dim_products)
    -----------------------------------------------------------------

    start_time := clock_timestamp();
    RAISE NOTICE 'Creating gold.dim_products...';

    CREATE MATERIALIZED VIEW gold.dim_products AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY prd_start_dt, prd_key) AS product_key,
        prd_id   AS product_id,
        prd_key  AS product_number,
        prd_nm   AS product_name,
        cat_id   AS category_id,
        cat.cat  AS category,
        cat.subcat AS subcategory,
        cat.maintenance AS maintenance,
        prd_cost AS cost,
        prd_line AS product_line,
        prd_start_dt AS start_date
    FROM silver.crm_prd_info p
    LEFT JOIN silver.erp_px_cat_g1v2 cat
        ON p.cat_id = cat.id
    WHERE prd_end_dt IS NULL;

    end_time := clock_timestamp();
    RAISE NOTICE 'gold.dim_products created in % seconds',
        ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2);


    -----------------------------------------------------------------
    -- 3) Build Sales Fact Table (gold.fact_sales)
    -----------------------------------------------------------------

    start_time := clock_timestamp();
    RAISE NOTICE 'Creating gold.fact_sales...';

    CREATE MATERIALIZED VIEW gold.fact_sales AS
    SELECT
        s.sls_ord_num  AS order_number,
        pr.product_key as product_key,
        cu.customer_key as customer_key,
        s.sls_order_dt AS order_date,
        s.sls_ship_dt  AS shipping_date,
        s.sls_due_dt   AS due_date,
        s.sls_sales    AS sales_amount,
        s.sls_quantity AS quantity,
        s.sls_price    AS price
    FROM silver.crm_sales_details s
    LEFT JOIN gold.dim_products pr
        ON s.sls_prd_key = pr.product_number
    LEFT JOIN gold.dim_customers cu
        ON s.sls_cust_id = cu.customer_id;

    end_time := clock_timestamp();
    RAISE NOTICE 'gold.fact_sales created in % seconds',
        ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2);


    -----------------------------------------------------------------
    -- 4) Final execution summary
    -----------------------------------------------------------------

    total_end := clock_timestamp();
    RAISE NOTICE 'Gold layer successfully created in % seconds',
        ROUND(EXTRACT(EPOCH FROM (total_end - total_start)), 2);


    -----------------------------------------------------------------
    -- Error handling
    -----------------------------------------------------------------
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '==========================================';
            RAISE NOTICE 'ERROR OCCURRED DURING GOLD LAYER CREATION';
            RAISE NOTICE 'Error Message: %', SQLERRM;
            RAISE NOTICE 'SQLSTATE: %', SQLSTATE;
            RAISE NOTICE '==========================================';

END;
$$;
