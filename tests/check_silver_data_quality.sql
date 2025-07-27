/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- *******************************
-- Checking 'silver.crm_cust_info'
-- ******************************
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
	cst_id,
	COUNT(*)
FROM Silver.crm_cust_info
GROUP BY cst_id 
HAVING cst_id IS NULL OR COUNT(*) != 1

-- *******************************
-- Checking 'silver.crm_prd_info'
-- ******************************
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

SELECT
	prd_id,
	COUNT(*)
FROM Silver.crm_prd_info
GROUP BY prd_id 
HAVING prd_id IS NULL OR COUNT(*) != 1

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
	prd_nm
FROM Silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results
SELECT 
	prd_cost
FROM Silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0 

-- Data Standardization & Consistency
SELECT DISTINCT 
	PRD_LINE
FROM Silver.crm_prd_info

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
SELECT 
	prd_start_dt,
	prd_start_dt
FROM Silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt

-- **********************************
-- Checking 'silver.crm_sales_details'
-- **********************************
-- Check for Invalid Dates
-- Expectation: No Invalid Dates
SELECT 
	sls_due_dt
FROM Silver.crm_sales_details
WHERE 
    sls_due_dt IS NULL
    OR sls_due_dt < '1900-01-01'
    OR sls_due_dt > '2050-01-01'

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

---- *****************************
--   Checking 'silver.erp_cust_az12'
-- -- ****************************
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;

-- **********************************
--   Checking 'silver.erp_loc_a101'
-- **********************************
-- Data Standardization & Consistency
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

-- **********************************
-- Checking 'silver.erp_px_cat_g1v2'
-- **********************************
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;
  










