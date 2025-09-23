/*








*/




SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt IS NULL OR LEN(sls_order_dt) != 8 OR
	sls_ship_dt IS NULL OR LEN(sls_ship_dt) != 8 OR
	sls_due_dt IS NULL OR LEN(sls_due_dt) != 8 OR
	sls_order_dt > sls_ship_dt OR
	sls_order_dt > sls_due_dt OR
	sls_ship_dt > sls_due_dt;

SELECT
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt
FROM bronze.crm_sales_details


2010 12 29 
1234 56 78
SELECT  *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (
	SELECT DISTINCT cst_id 
	FROM bronze.crm_cust_info
)-- FOREIGN KEY CHECK : Does it join cleanly with dimension tables ? 


SELECT prd_name
FROM bronze.crm_prod_info
WHERE prd_name != TRIM(prd_name)  -- Find unwanted spaces in the column


SELECT 
sls_order_num,
COUNT(*)
FROM bronze.crm_sales_details
GROUP BY sls_order_num
HAVING COUNT(*) > 1 or sls_order_num IS NULL -- Find duplicates or NULL


SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_num IS NULL

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info  -- Find distinct waarders in the colomn

SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key IS NULL



SELECT prd_cost
FROM bronze.crm_prod_info
WHERE prd_cost <0 or prd_cost IS NULL -- Find unwanted spaces in the column

SELECT  *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (
	SELECT DISTINCT cst_id 
	FROM bronze.crm_cust_info
)-- FOREIGN KEY CHECK : Does it join cleanly with dimension tables ? 

-- price = sales * quantity

SELECT sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales * sls_quantity != sls_price OR
	sls_sales <= 0 OR sls_sales IS NULL OR 
	sls_quantity <= 0 OR sls_quantity IS NULL OR
	sls_price <= 0 OR sls_price IS NULL

-- 35 bad rows -> Need to speak with an expert to choose an aproach
	-- All negatief values - > positive
	-- Sls_price will be calculated from sales * quantity
	-- Null values in sls_sales - > sls_price/sls_quantity(can not be zero)
SELECT

	CASE WHEN sls_sales IS NULL OR sls_sales = 0 THEN ABS(sls_price)/NULLIF(sls_quantity,0)
		WHEN sls_sales < 0 AND sls_sales IS NOT NULL THEN ABS(sls_sales)
		ELSE sls_sales
	END AS sls_sales_new,
	sls_sales
FROM bronze.crm_sales_details
WHERE sls_sales != (CASE WHEN sls_sales IS NULL OR sls_sales = 0 THEN ABS(sls_price)/NULLIF(sls_quantity,0)
		WHEN sls_sales < 0 AND sls_sales IS NOT NULL THEN ABS(sls_sales)
		ELSE sls_sales

END)


WITH cleaned AS(
	SELECT 
		sd.*,
		CASE	
			WHEN sls_sales IS NULL OR sls_sales = 0
				THEN ABS(sls_price)/NULLIF(sls_quantity,0)
			WHEN sls_sales < 0
				THEN ABS(sls_sales)
			ELSE sls_sales
		END AS sls_sales_new
	FROM bronze.crm_sales_details sd
)

SELECT *
FROM cleaned
WHERE COALESCE(sls_sales, -999999) != COALESCE(sls_sales_new, -999999)
WHERE sls_sales IS  FROM sls_sales_new


SELECT *
FROM silver.erp_cust_az12
WHERE bdate > GETDATE()

SELECT *
FROM silver.crm_cust_info
WHERE cst_key = 'AW00011338'

SELECT DISTINCT 
	cntry AS old,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULl THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry

FROM bronze.erp_loc_a101

SELECT 
	REPLACE(cid, '-', '') AS cid,
	cntry
FROM bronze.erp_loc_a101
WHERE cntry = 'DE'
