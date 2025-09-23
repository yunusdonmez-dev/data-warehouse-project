/*
======================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
======================================================================
Script Purpose:
  This stored procedure performs the ETL process to populate the 'silver' schema
  from 'bronze' schema.
  Actions Performed:
    - Truncate silver tables.
    - Insert transformed and cleansed data from 'bronze' into 'silver' tables

Parameters:
  None
  This stired procedure does not accept any parameters or return any values.

Usage Example
  EXEC silver.load_silver;
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_time_layer DATETIME;
	SET @start_time_layer = GETDATE();
	BEGIN TRY

		
		PRINT '============================================================';
		PRINT 'Loading Silver Layer';
		PRINT '============================================================';

		PRINT '------------------------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>> Inserting Data Into: silver.crm_cust_info';

		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gender,
			cst_create_date
		)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Famale'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize gender status values to readable format
			cst_create_date
		FROM(
			SELECT 
			*,
			ROW_NUMBER() OVER ( -- Give a row number to every row for every partition ( cst_id) and 'order by' is a must for this window function
				PARTITION BY cst_id
				ORDER BY cst_create_date DESC
				) AS flag_row
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t 
		WHERE flag_row = 1  -- Select the most recent record per customer
		SET @end_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prod_info';
		TRUNCATE TABLE silver.crm_prod_info
		PRINT '>> Inserting Data Into: silver.crm_prod_info';

		INSERT INTO silver.crm_prod_info(
			prd_id,
			cat_id,
			prd_key,
			prd_name,
			prd_cost,
			prd_line,
			prd_start_date,
			prd_end_date

		)
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_name,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			CAST(prd_start_date AS DATE) AS prd_start_date,
			CAST(LEAD(prd_start_date) OVER(PARTITION BY prd_key ORDER BY prd_start_date)-1 AS DATE) AS prd_end_date
		FROM bronze.crm_prod_info

		SET @end_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details
		PRINT '>> Inserting Data Into: silver.crm_sales_details';

		INSERT INTO silver.crm_sales_details (
			sls_order_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_order_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,

			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,

				CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,

			CASE WHEN sls_sales IS NULL OR sls_sales = 0 THEN ABS(sls_price) * sls_quantity
				WHEN sls_sales < 0 AND sls_sales IS NOT NULL THEN ABS(sls_sales)
				ELSE sls_sales
			END AS sls_sales,

			sls_quantity,
	
			CASE WHEN sls_price = 0 OR sls_price IS NULL THEN ABS(sls_sales)/NULLIF(sls_quantity,0)
				WHEN sls_price < 0 AND sls_price IS NOT NULL THEN ABS(sls_price)
				ELSE sls_price
			END AS sls_price

		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';

		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		SELECT 

			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4 , LEN(cid))
				ELSE cid
			END AS cid,

			CASE WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate,

			CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';

		INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
		)
		SELECT 
			REPLACE(cid, '-', '') AS cid,

			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				 WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
				 WHEN TRIM(cntry) = '' OR cntry IS NULl THEN 'n/a'
				 ELSE TRIM(cntry)
			END AS cntry
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

		INSERT INTO silver.erp_px_cat_g1v2(
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

		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();

		PRINT '>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------';

		
		SET @end_time = GETDATE();
		PRINT '====================================================';
		PRINT '>>Load Duration of Silver Layer:' + CAST(DATEDIFF(second, @start_time_layer, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '====================================================';
	END TRY
	BEGIN CATCH
		PRINT '====================================================';
		PRINT 'ERROR DURING LOADING SILVER LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '====================================================';
	END CATCH

END
