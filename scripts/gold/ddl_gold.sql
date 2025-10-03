/*
Script Purpose:

This script creates the **Gold Layer views** for the Data Warehouse.  
The Gold Layer is designed to serve as the **consumption-ready layer** for reporting, analytics, and BI tools.  
It provides clean, business-friendly views by joining and transforming data from the Silver Layer.  

Specifically, this script does the following:

1. gold.dim_customers
   - Creates a dimension view for customers.
   - Generates a surrogate key (customer_key) for consistent joins.
   - Combines CRM customer data with ERP customer attributes (gender, birthdate) and location data.
   - Applies business rules (e.g., prioritize CRM gender when available).

2. gold.dim_products
   - Creates a dimension view for products.
   - Generates a surrogate key (product_key).
   - Joins product master data with product category reference data.
   - Excludes inactive products (filters out records with prd_end_dt not null).
   - Exposes descriptive attributes such as category, subcategory, maintenance type, and product line.

3. gold.fact_sales
   - Creates the main fact view for sales transactions.
   - Connects sales events to customer and product dimensions via surrogate keys.
   - Exposes transactional measures: order date, ship date, due date, sales amount, quantity, and price.
   - Provides a clean, analysis-ready fact table for use in BI dashboards and reporting.

Overall:
- This script transforms the Silver Layer into the Gold Layer by 
  creating business-friendly, analysis-ready **dimension** and **fact** views.
- It enables a **star schema** design, where fact_sales can be joined with 
  dim_customers and dim_products for analytics such as revenue trends, product performance, 
  and customer insights.
*/
/*
Script Purpose:

*/

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers
GO
CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, -- Create surrogate key customer_key
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
ci.cst_marital_status as marital_status,
el.cntry as country,
CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr -- Take from CRM first (master table rule)
	 ELSE COALESCE(ec.gen, 'Unknown')
END as gender,
ec.bdate as birthdate,
ci.cst_create_date as create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ec
ON ec.cid=ci.cst_key
LEFT JOIN silver.erp_loc_a101 el
ON el.cid = ci.cst_key

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products
GO
CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cp.prd_id) AS product_key,
	cp.prd_id AS product_id,
	cp.prd_key as product_number,
	cp.prd_nm as product_name,
	cp.cat_id as category_id,
	ep.cat as category,
	ep.subcat as subcategory,
	ep.maintenance as maintenance,
	cp.prd_cost as cost,
	cp.prd_line as product_line,
	cp.prd_start_dt as start_date
FROM silver.crm_prd_info cp
LEFT JOIN silver.erp_px_cat_g1v2 ep
ON		  cp.cat_id = ep.id
WHERE cp.prd_end_dt IS NULL

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales
GO
CREATE VIEW gold.fact_sales AS
SELECT 
ss.sls_order_num as order_number,
gc.customer_key,
gp.product_key,
ss.sls_order_dt as order_date,
ss.sls_ship_dt as ship_date,
ss.sls_due_dt as due_date,
ss.sls_sales as sales_amount,
ss.sls_quantity as quantity,
ss.sls_price as price
FROM silver.crm_sales_detail ss
LEFT JOIN gold.dim_customers gc
ON	      ss.sls_cust_id = gc.customer_id
LEFT JOIN gold.dim_products gp
ON		  ss.sls_prd_key = gp.product_number
