/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE IF NOT EXISTS silver.crm_cust_info (
	cst_id INT,
	cst_key varchar(50),
	cst_firstname varchar(50),
	cst_lastname varchar(50),
	cst_marital_status varchar(50),
	cst_gndr varchar(50),
	cst_create_date DATE,
	dwh_create_date DATE DEFAULT CURRENT_DATE
);

DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	cat_id varchar(50),
	prd_key varchar(50),
	prd_nm varchar(50),
	prd_cost INT,
	prd_line varchar(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATE DEFAULT CURRENT_DATE
);

DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
	sls_ord_num varchar(50),
	sls_prd_key varchar(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATE DEFAULT CURRENT_DATE
);
DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
	cid varchar(50),
	bdate DATE,
	gen varchar(50),
	dwh_create_date DATE DEFAULT CURRENT_DATE
);

DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
	cid varchar(50),
	cntry varchar(50),
	dwh_create_date DATE DEFAULT CURRENT_DATE
);

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
	id varchar(50),
	cat varchar(50),
	subcat varchar(50),
	maintenance varchar(50),
	dwh_create_date DATE DEFAULT CURRENT_DATE
);