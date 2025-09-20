/*
===============================================================================
DDL Scripts: Create Bronze tables
===============================================================================
Script purpose : 
	This script creates tables in the 'bronze' schema, dropping existing tables
	If they already exist.
	Run the script to (re)define the DDL structure of 'bronze' Tables

================================================================================
*/

IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;

GO

CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastnaem NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gender NVARCHAR(50),
	cst_create_date DATE

);
GO

IF OBJECT_ID ('bronze.crm_prod_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prod_info;
CREATE TABLE bronze.crm_prod_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_name NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_date DATETIME,
	prd_end_date DATETIME

);
GO

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
	sls_order_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT

);
GO

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);
GO

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);
GO

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);
