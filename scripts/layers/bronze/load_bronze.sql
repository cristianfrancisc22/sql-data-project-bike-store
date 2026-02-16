/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    total_start TIMESTAMP := clock_timestamp();
    total_end TIMESTAMP;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    RAISE NOTICE 'Bronze layer load starting...';

    -----------------------------------------------------------------
    -- 1) Truncate all Bronze tables
    -----------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE 'Truncating all Bronze tables...';

    TRUNCATE TABLE 
        bronze.crm_cust_info,
        bronze.crm_prd_info,
        bronze.crm_sales_details,
        bronze.erp_px_cat_g1v2,
        bronze.erp_cust_az12,
        bronze.erp_loc_a101
    RESTART IDENTITY CASCADE;

    end_time := clock_timestamp();
    RAISE NOTICE 'All Bronze tables truncated in % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2);

    -----------------------------------------------------------------
    -- 2) Load categories data
    -----------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading categories data...';

    COPY bronze.erp_px_cat_g1v2
    FROM 'C:\Users\computer\Desktop\Proiecte date\Data warehouse SQL\datasets\source_erp\PX_CAT_G1V2.csv'
    DELIMITER ',' CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'Categories data loaded in % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2);

    -----------------------------------------------------------------
    -- 3) Load Customers data
    -----------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading customers data...';

    COPY bronze.erp_cust_az12
    FROM 'C:\Users\computer\Desktop\Proiecte date\Data warehouse SQL\datasets\source_erp\CUST_AZ12.csv'
    DELIMITER ',' CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'Customers data loaded in % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2);

    -----------------------------------------------------------------
    -- 4) Load location
    -----------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading location data...';

    COPY bronze.erp_loc_a101
    FROM 'C:\Users\computer\Desktop\Proiecte date\Data warehouse SQL\datasets\source_erp\LOC_A101.csv'
    DELIMITER ',' CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'Location data loaded in % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2);

    -----------------------------------------------------------------
    -- 5) Load customer information
    -----------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading customer information data...';

    COPY bronze.crm_cust_info
    FROM 'C:\Users\computer\Desktop\Proiecte date\Data warehouse SQL\datasets\source_crm\cust_info.csv'
    DELIMITER ',' CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'Customer information data loaded in % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2);

    -----------------------------------------------------------------
    -- 6) Load product information
    -----------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading product information data...';

    COPY bronze.crm_prd_info
    FROM 'C:\Users\computer\Desktop\Proiecte date\Data warehouse SQL\datasets\source_crm\prd_info.csv'
    DELIMITER ',' CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'Product information data loaded in % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2);

    -----------------------------------------------------------------
    -- 7) Load sales details
    -----------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading sales details data...';

    COPY bronze.crm_sales_details
    FROM 'C:\Users\computer\Desktop\Proiecte date\Data warehouse SQL\datasets\source_crm\sales_details.csv'
    DELIMITER ',' CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'Sales details data loaded in % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2);

    -----------------------------------------------------------------
    -- Final Bronze Layer load Complete
    -----------------------------------------------------------------
    total_end := clock_timestamp();
    RAISE NOTICE 'Bronze layer load completed in % seconds', ROUND(EXTRACT(EPOCH FROM (total_end - total_start)), 2);

END;
$$;

---------------------------------------------------------------------
-- Usage:
-- CALL bronze.load_bronze();
---------------------------------------------------------------------
