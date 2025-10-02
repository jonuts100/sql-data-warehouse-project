/*
==============================================================
Purpose:
This script creates the raw source tables in the Bronze layer 
of the Data Warehouse. 

Steps performed:
1. Check if a table already exists (using OBJECT_ID).
   - If it exists, drop it.
   - This ensures a clean re-creation each time the script runs.
2. Create fresh empty Bronze tables that will hold raw 
   data ingested from source systems (CRM and ERP).
3. Tables represent different source domains:
   - CRM: Customer info, Product info, Sales details.
   - ERP: Customer master (AZ12), Location master (A101), 
     Product category master (PX_CAT_G1V2).
4. These tables are staging/raw storage and will later feed 
   into the Silver and Gold layers for transformation 
   and analytics.

Note:
The Bronze layer is meant for *raw ingestion* with minimal 
or no transformation, preserving the source structure.
==============================================================
*/

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);

IF OBJECT_ID('bronze.crm_sales_detail', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_detail;
CREATE TABLE bronze.crm_sales_detail (
	sls_order_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id NVARCHAR(50),
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity INT,
	sls_price int
);

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
)
