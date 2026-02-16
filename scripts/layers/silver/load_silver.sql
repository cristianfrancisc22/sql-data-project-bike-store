/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL silver.load_silver;
===============================================================================
*/
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
	total_start TIMESTAMP := clock_timestamp();
	total_end TIMESTAMP;
	start_time TIMESTAMP;
	end_time TIMESTAMP;
BEGIN
	
	RAISE NOTICE 'Silver layer load starting...';

	-----------------------------------------------------------------
    -- 1) Truncate all silver tables
    -----------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE 'Truncating all silver tables...';

    TRUNCATE TABLE 
        silver.crm_cust_info,
        silver.crm_prd_info,
        silver.crm_sales_details,
        silver.erp_px_cat_g1v2,
        silver.erp_cust_az12,
        silver.erp_loc_a101
    RESTART IDENTITY CASCADE;

    end_time := clock_timestamp();
    RAISE NOTICE 'All silver tables truncated in % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2);

	-----------------------------------------------------------------
    -- 2) Load crm_cust_info data
    -----------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading crm_cust_info data...';

	INSERT INTO silver.crm_cust_info (
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE 
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			ELSE 'n/a'
		END AS cst_marital_status,
		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			ELSE 'n/a'
		END AS cst_gndr,
		cst_create_date
		FROM (
			SELECT *,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) WHERE flag_last = 1
	);

	end_time := clock_timestamp();
    RAISE NOTICE 'crm_cust_info data loaded in % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2);

	-----------------------------------------------------------------
    -- 3) Load crm_prd_info data
    -----------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading crm_prd_info data...';

	INSERT INTO silver.crm_prd_info (
		SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
		SUBSTRING(prd_key, 7, LENGTH(prd_key) - 5) as prd_key,
		TRIM(prd_nm) AS prd_nm,
		COALESCE(prd_cost, 0) as prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'M' THEN 'Mountain'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		prd_start_dt,
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 as prd_end_dt_test
		FROM bronze.crm_prd_info
	);

	end_time := clock_timestamp();
    RAISE NOTICE 'crm_prd_info data loaded in % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2);

	-----------------------------------------------------------------
    -- 4) Load crm_sales_details data
    -----------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading crm_sales_details data...';
	
	INSERT INTO silver.crm_sales_details (
		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE
			WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::varchar(50)) != 8 THEN NULL
			ELSE (sls_order_dt::varchar(50))::DATE
		END AS sls_order_dt,
		CASE
			WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::varchar(50)) != 8 THEN NULL
			ELSE (sls_ship_dt::varchar(50))::DATE
		END AS sls_ship_dt,
		CASE
			WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::varchar(50)) != 8 THEN NULL
			ELSE (sls_due_dt::varchar(50))::DATE
		END AS sls_due_dt,
		CASE
			WHEN sls_sales <= 0 OR sls_sales IS NULL
			OR sls_sales != sls_quantity * sls_price THEN sls_price/sls_quantity
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE
			WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales/NULLIF(sls_quantity, 0)
			ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details
	);

	end_time := clock_timestamp();
    RAISE NOTICE 'crm_sales_details data loaded in % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2);

	-----------------------------------------------------------------
    -- 4) Load erp_cust_az12 data
    -----------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading erp_cust_az12 data...';
	
	INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
	)
	SELECT 
		CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
			ELSE cid
		END AS cid,
		CASE
			WHEN bdate > CURRENT_DATE THEN NULL
			ELSE bdate
		END AS bdate,
		CASE 
			WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen
	FROM bronze.erp_cust_az12;

	end_time := clock_timestamp();
    RAISE NOTICE 'erp_cust_az12 data loaded in % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2);

	-----------------------------------------------------------------
    -- 4) Load erp_loc_a101 data
    -----------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading erp_loc_a101 data...';

	INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
	)
	SELECT 
		REPLACE(cid, '-','') as cid,
		CASE
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' or cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END as cntry
	FROM bronze.erp_loc_a101;	

	end_time := clock_timestamp();
    RAISE NOTICE 'erp_loc_a101 data loaded in % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2);

	-----------------------------------------------------------------
    -- 4) Load erp_px_cat_g1v2 data
    -----------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading erp_px_cat_g1v2 data...';

	INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT 
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2;

	end_time := clock_timestamp();
    RAISE NOTICE 'erp_px_cat_g1v2 data loaded in % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2);

	-----------------------------------------------------------------
    -- 4) Silver layer loaded
    -----------------------------------------------------------------
    total_end := clock_timestamp();
    RAISE NOTICE 'Silver layer loaded completly in % seconds', ROUND(EXTRACT(EPOCH FROM (total_end - total_start)), 2);

	 -- ================== CATCH BLOCK ==================

	EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Code (SQLSTATE): %', SQLSTATE;
        RAISE NOTICE '==========================================';	
END;
$$;