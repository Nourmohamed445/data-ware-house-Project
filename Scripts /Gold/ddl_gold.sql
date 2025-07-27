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

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('Gold.dim_customers' , 'U') IS NOT NULL
	DROP VIEW Gold.dim_customers;
GO
CREATE VIEW Gold.dim_customers AS
SELECT 
  	ROW_NUMBER() OVER(ORDER BY cst_id) Customer_key,
  	CI.cst_id					      Customer_id,
  	CI.cst_key					    Customer_number,
  	CI.cst_firstname			  First_name,
  	CI.cst_lastname				  Last_name,
  	CL.CNTRY					      Country,
  	CI.cst_marital_status		Marital_status,
  	CASE	
  		WHEN CI.cst_gndr != 'N/A' THEN CI.cst_gndr
  		ELSE COALESCE(CA.GEN,'N/A')
  	END							        Gender,
  	CA.BDATE					      Birth_date,
  	CI.cst_create_date			created_date
  
FROM Silver.crm_cust_info AS CI
LEFT JOIN Silver.erp_CUST_AZ12 AS CA 
	ON		CI.cst_key = CA.CID
LEFT JOIN Silver.erp_LOC_A101 AS CL
	ON		CI.cst_key = CL.CID;
Go

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('Gold.dim_customers' , 'U') IS NOT NULL
	DROP VIEW Gold.dim_customers;
GO

CREATE VIEW Gold.dim_products AS 
SELECT 
	ROW_NUMBER() OVER(ORDER BY prd_start_dt, prd_key)	Product_key,
	PI.prd_id AS						  Product_id,
	PI.prd_key AS						  product_number,
	PI.prd_nm AS						  product_name,
	PI.cat_id AS						  Category_id,
	pc.CAT	AS							  Category,
	pc.SUBCAT AS						  SubCategory,
	PI.prd_line AS						Product_line,
	pc.MAINTENANCE	AS				MAINTENANCE,
	PI.prd_cost AS						Product_cost,
	PI.prd_start_dt AS				start_date

FROM Silver.crm_prd_info	PI
LEFT JOIN Silver.erp_PX_CAT_G1V2 PC
	ON pi.cat_id = pc.ID
WHERE PI.prd_end_dt IS NULL; -- Getting the Current Data
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
CREATE VIEW Gold.Fact_sales AS
SELECT  
        DC.Customer_key  AS     customer_key,
        DP.product_key   AS     product_key,
        SD.sls_ord_num   AS     order_number,
        SD.sls_order_dt  AS     order_date,
        SD.sls_ship_dt   AS     ship_date,
        SD.sls_due_dt    AS     due_date,      
        SD.sls_price     AS     Price,
        SD.sls_quantity  AS     Quantity,
        SD.sls_sales     AS     Sales
FROM Silver.crm_sales_details AS SD
LEFT JOIN Gold.dim_products AS DP  
    ON DP.product_number = SD.sls_prd_key
LEFT JOIN Gold.dim_customers AS DC
    ON DC.customer_id = SD.sls_cust_id;
