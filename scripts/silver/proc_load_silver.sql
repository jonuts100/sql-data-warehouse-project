/*
Script purpose: 
Perform ETL process on bronze layer data to populate the silver schema tables from the bronze schema.
Actions performed
  - TRUNCATE silver tables
  - INSERT transformed and cleansed data from bronze into silver tables

Parameters:
  None
  No paraneters or values returned
Usage:
  EXEC silver.load_server;
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
		DECLARE @start_date DATE, @end_date DATE, @batch_start DATE, @batch_end DATE;

		SET @start_date = GETDATE()

		SET @batch_start = GETDATE()
		PRINT '>>> Truncating table silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>>> Inserting data into table silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname),
		TRIM(cst_lastname),
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 ELSE 'Unknown'
		END cst_marital_status,
		CASE WHEN upper(trim(cst_gndr)) = 'M' THEN 'Male'
			 WHEN upper(trim(cst_gndr)) = 'F' THEN 'Female'
			 ELSE 'Unknown'
		END cst_gndr,
		cst_create_date
		FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date) as last_flag
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t
		WHERE last_flag=1
		set @batch_end = GETDATE()
		PRINT 'Operation took ' + CAST(DATEDIFF(second, @batch_start, @batch_end) AS VARCHAR) + ' seconds'

		set @batch_start = GETDATE()
		PRINT '>>> Truncating table silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>>> Inserting data into table silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
			SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
			ISNULL(prd_cost, 0) as prd_cost, -- Sets null values to 0
			CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 's' THEN 'Other Sales'
				else 'unknown'
			END prd_line,
			CAST (prd_start_dt AS DATE) as prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) as prd_end_dt -- CALCULATE END DATE AS OONE DAY BEFORE NEXT START DATE
		FROM bronze.crm_prd_info
		set @batch_end = GETDATE()
		PRINT 'Operation took ' + CAST(DATEDIFF(second, @batch_start, @batch_end) AS VARCHAR) + ' seconds'

		set @batch_start = GETDATE()
		PRINT '>>> Truncating table silver.crm_sales_detail';
		TRUNCATE TABLE silver.crm_sales_detail;
		PRINT '>>> Inserting data into table silver.crm_sales_detail';
		INSERT INTO silver.crm_sales_detail (
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
		CASE WHEN len(sls_order_dt) != 8 OR sls_order_dt <= 0 THEN NULL
			 ELSE CAST(CAST (sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN len(sls_ship_dt) != 8 OR sls_ship_dt <= 0 THEN NULL
			 ELSE CAST(CAST (sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN len(sls_due_dt) != 8 OR sls_due_dt <= 0 THEN NULL
			 ELSE CAST(CAST (sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales <= 0 or sls_sales is NULL or sls_sales != abs(sls_price)*sls_quantity then abs(sls_price)*sls_quantity 
			 ELSE sls_sales
		END AS sls_sales_new,
		sls_quantity,
		CASE WHEN sls_price <=0 or sls_price is NULL then sls_sales/NULLIF(sls_quantity,0)
			ELSE sls_price
		END as sls_price_new
		FROM bronze.crm_sales_detail
		set @batch_end = GETDATE()
		PRINT 'Operation took ' + CAST(DATEDIFF(second, @batch_start, @batch_end) AS VARCHAR) + ' seconds'


		-- Load
		SET @batch_start = GETDATE()
		PRINT '>>> Truncating table silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>>> Inserting data into table silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2
		set @batch_end = GETDATE()
		PRINT 'Operation took ' + CAST(DATEDIFF(second, @batch_start, @batch_end) AS VARCHAR) + ' seconds'


		SET @batch_start = getdate()
		PRINT '>>> Truncating table silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>>> Inserting data into table silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		SELECT 
		REPLACE(cid, '-', '') as cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR TRIM(cntry) is NULL THEN 'Unknown'
			 ELSE TRIM(cntry)
		end AS cntry
		FROM bronze.erp_loc_a101
		set @batch_end = GETDATE()
		PRINT 'Operation took ' + CAST(DATEDIFF(second, @batch_start, @batch_end) AS VARCHAR) + ' seconds'

		-- Transform then Load
		set @batch_start = getdate()
		PRINT '>>> Truncating table silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>>> Inserting data into table silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			cid, bdate, gen
		)
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, len(cid))
			 ELSE cid
		END AS cid,
		CASE WHEN bdate < '1924-12-30' OR bdate > GETDATE() THEN NULL
			 ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			 ELSE 'Unknown'
		END AS gen
		FROM bronze.erp_cust_az12 as t
		set @batch_end = GETDATE()
		PRINT 'Operation took ' + CAST(DATEDIFF(second, @batch_start, @batch_end) AS VARCHAR) + ' seconds'

		SET @end_date = GETDATE()

		PRINT 'Transform and Load time to Silver Layer: ' + CAST(DATEDIFF(second,@start_date, @end_date) AS VARCHAR) + ' seconds'
	END TRY
	BEGIN CATCH
		PRINT 'Error message: ' + ERROR_MESSAGE();
		PRINT 'Error message: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error message: ' + CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
END
