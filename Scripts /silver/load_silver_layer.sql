/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL(Extract - Transform - Load) process to 
    the Silver schema tables from the Bronze 

process:
    - Truncates the Silver tables before loading data.
    - Inserting the data from the Bronze schema table after transform and clean it 

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE Silver.silver_load AS

BEGIN
	BEGIN TRY 
		DECLARE @start_time DATETIME , @end_time DATETIME; -- Creating variables
		DECLARE @START_LOADING_TIME DATETIME , @END_LOADING_TIME DATETIME -- VARIABLE FOR THE WHOLE BATCH

		PRINT('=============================================');
		PRINT('LOADING OUR DATA TO THE BONZE LAYER');
		PRINT('=============================================');

		PRINT('=-------------------------------------------=');
		PRINT(' -- 1- LOADING THE CRM FILES');
		PRINT('=-------------------------------------------=');

		SET @START_LOADING_TIME = GETDATE();
	-- LOADING DATA AFTER CLEANING IT TO THE SILVER TABLE 

	-- =============================
	-- Silver Customer Information :
	-- =============================

		-- Start off loading
		SET @start_time = GETDATE();

		PRINT ' >> Truncate Customer Information Table from any data' ;
		IF OBJECT_ID('Silver.crm_cust_info','U') IS NOT NULL 
				TRUNCATE TABLE Silver.crm_cust_info;

		PRINT ' >> Inserting / Loading Data from Bronze - customer layer to silver - customer - layer ' ;			
		INSERT INTO Silver.crm_cust_info 
		(
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
			TRIM(cst_firstname) cst_firstname,
			TRIM(cst_lastname) cst_lastname,
			CASE	WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
					WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
					ELSE 'N/A'
			END AS cst_marital_status,
			CASE	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
					WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
					ELSE 'N/A'
			END AS cst_gndr,
			cst_create_date
		FROM (
		SELECT 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_Create_date) crm_deleting_duplicates
		FROM Bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		) t WHERE crm_deleting_duplicates = 1 ;

		-- END LOADING TIME 
		SET @end_time = GETDATE();

		-- COUNT THE DURATION 
		PRINT 'Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR ) + ' Seconds' 
		print'________________________________________________________________________________________';


	-- ============================
	-- Silver Product information :
	-- ============================

		-- Start off loading
		SET @start_time = GETDATE();

		-- recreate the table again:
		PRINT ' >> Recreating Products Information Table ' ;
		IF OBJECT_ID('Silver.crm_prd_info', 'U') IS NOT NULL
			Drop TABLE Silver.crm_prd_info ;

		CREATE TABLE Silver.crm_prd_info
		(
			prd_id					INT ,
			cat_id					NVARCHAR(50),
			prd_key					NVARCHAR(50),
			prd_nm					NVARCHAR(50),
			prd_cost				INT,
			prd_line				NVARCHAR(50),
			prd_start_dt			DATE,
			prd_end_dt				DATE ,
			dwh_create_date			DATETIME2 DEFAULT GETDATE()
		);

		PRINT ' >> Truncate Product Information Table from any data' ;
		-- LOADING THE DATA
		IF OBJECT_ID('Silver.crm_prd_info','U') IS NOT NULL
			TRUNCATE TABLE Silver.crm_prd_info;
	
		PRINT ' >> Inserting / Loading Data from Bronze - Product - layer to silver - Product - layer ' ;	
		INSERT INTO  Silver.crm_prd_info
		(
			prd_id,				
			cat_id,	
			prd_key,
			prd_nm,		
			prd_cost,		
			prd_line,	
			prd_start_dt,	
			prd_end_dt	

		)
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1 ,5), '-' ,'_' ) cat_id, -- extracting category id
			SUBSTRING(prd_key, 7, len(prd_key)) prd_key, -- extracting product key
			prd_nm,
			ISNULL(prd_cost, 0) prd_cost,
			CASE UPPER(TRIM(prd_line)) -- doing a full and meaningfull values
					WHEN 'R' THEN 'Road'
					WHEN 'M' THEN 'Mountain'
					WHEN 'S' THEN 'Other Sales'
					WHEN 'T' THEN 'Touring'
					ELSE 'N/A'
			END prd_line,
			CAST(prd_start_dt AS DATE) prd_start_dt, -- change date to be date instead of date time 
			CAST( LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE ) prd_end_dt -- getting the end date from the start date
		FROM Bronze.crm_prd_info;

		-- END LOADING TIME 
		SET @end_time = GETDATE();

		-- COUNT THE DURATION 
		PRINT 'Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR ) + ' Seconds' 
		print'________________________________________________________________________________________';


	-- ============================
	-- Silver sales details :
	-- ============================

		-- START LOADING TIME 
		SET @start_time = GETDATE();

	-- RECREATE THE TABLE AGAIN 
		PRINT ' >> Recreating Products Information Table ' ;
		IF OBJECT_ID('Silver.crm_sales_details' , 'U') IS NOT NULL
			DROP TABLE Silver.crm_sales_details;

		CREATE TABLE Silver.crm_sales_details
		(
			sls_ord_num				NVARCHAR(50),
			sls_prd_key				NVARCHAR(50),
			sls_cust_id				INT,
			sls_order_dt			DATE,
			sls_ship_dt				DATE,
			sls_due_dt				DATE,
			sls_sales				INT,
			sls_quantity			INT,
			sls_price				INT,
			dwh_create_date			DATETIME2 DEFAULT GETDATE()
		);

		-- INSERTING THE DATA
		PRINT ' >> Truncate sales Information Table from any data' ;
		IF OBJECT_ID('Silver.crm_sales_details' , 'U') IS NOT NULL
			TRUNCATE TABLE Silver.crm_sales_details;

		PRINT ' >> Inserting / Loading Data from Bronze - Sales - layer to silver - Sales - layer ' ;			
		INSERT INTO Silver.crm_sales_details
		(
			sls_ord_num,				
			sls_prd_key,				
			sls_cust_id	,			
			sls_order_dt,			
			sls_ship_dt	,			
			sls_due_dt	,			
			sls_sales	,			
			sls_quantity,			
			sls_price				
		)

		SELECT 
			  sls_ord_num,
			  sls_prd_key,
			  sls_cust_id,
			  CASE  WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL 
					ELSE CONVERT(DATE , CAST(sls_order_dt AS VARCHAR))
			  END sls_order_dt,
			  CASE  WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
					ELSE CONVERT(DATE , CAST(sls_ship_dt AS VARCHAR))
			  END sls_ship_dt,
			  CASE  WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL 
					ELSE CONVERT(DATE , CAST(sls_due_dt AS VARCHAR))
			  END sls_due_dt,
			  CASE	WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity *  abs(sls_price)
					THEN sls_quantity *  sls_price

					ELSE sls_sales
			  END sls_sales,
			  sls_quantity,
			  CASE	WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / nullif(sls_quantity ,0)

					ELSE sls_price
			   END sls_price
		FROM Bronze.crm_sales_details;

		-- END LOADING TIME 
		SET @end_time = GETDATE();

		-- COUNT THE DURATION 
		PRINT 'Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR ) + ' Seconds' 
		print'________________________________________________________________________________________';

				---------
		PRINT('=-------------------------------------------=');
		PRINT(' -- 2- LOADING THE ERP FILES');
		PRINT('=-------------------------------------------=');

				---------

	-- ===================================
	-- Silver EXTRA CUSTOMER INFORMATION :
	-- ===================================

		-- START LOADING TIME 
		SET @start_time = GETDATE();

	-- INSERTING DATA 
		PRINT ' >> Truncate ExtraCustomer Information Table from any data' ;
		IF OBJECT_ID('Silver.erp_CUST_AZ12' , 'U' ) IS NOT NULL 
			TRUNCATE TABLE Silver.erp_CUST_AZ12;

		PRINT ' >> Inserting / Loading Data from Bronze - ExtraCustomerInfo - layer to silver - ExtraCustomerInfo - layer ' ;	
		INSERT INTO Silver.erp_CUST_AZ12
		(
			CID,
			BDATE,
			GEN
		)
		SELECT 
			CASE	WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
					ELSE CID
			END CID,
			CASE	WHEN BDATE > GETDATE() THEN NULL 
					ELSE BDATE
			END BDATE,
			CASE	WHEN UPPER(TRIM(GEN)) = 'F' OR UPPER(TRIM(GEN)) = 'FEMALE' THEN 'Female' 
					WHEN UPPER(TRIM(GEN)) IN ('M' , 'MALE') THEN 'Male'
					ELSE 'N/A'
		END GEN
		FROM Bronze.erp_CUST_AZ12;
		
		-- END LOADING TIME 
		SET @end_time = GETDATE();

		-- COUNT THE DURATION 
		PRINT 'Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR ) + ' Seconds' 
		print'________________________________________________________________________________________';




	-- ====================================
	-- Silver Customer location INFORMATION :
	-- ====================================

		-- START LOADING TIME 
		SET @start_time = GETDATE();

	-- INSERTING DATA 
		PRINT ' >> Truncate Customer location Information Table from any data' ;
		IF OBJECT_ID('Silver.erp_LOC_A101', 'U') IS NOT NULL 
			 TRUNCATE TABLE Silver.erp_LOC_A101;

		PRINT ' >> Inserting / Loading Data from Bronze - location - layer to silver - location - layer ' ;
		INSERT INTO  Silver.erp_LOC_A101
		(
			CID,
			CNTRY
		)
		SELECT 
			REPLACE(CID,'-','') CID,
			CASE	WHEN UPPER(TRIM(CNTRY)) IN ('USA', 'UNITED STATES', 'US')	THEN 'United States'
					WHEN UPPER(TRIM(CNTRY)) IN ('DE', 'GERMANY')	THEN 'Germany'
					WHEN UPPER(TRIM(CNTRY)) = '' OR UPPER(TRIM(CNTRY)) IS NULL	THEN 'N/A'
					ELSE CNTRY
			END CNTRY
		FROM BRONZE.erp_LOC_A101;

		-- END LOADING TIME 
		SET @end_time = GETDATE();

		-- COUNT THE DURATION 
		PRINT 'Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR ) + ' Seconds' 
		print'________________________________________________________________________________________';


	-- ====================================
	-- Silver Category  INFORMATIONs :
	-- ====================================
	
		-- START LOADING TIME 
		SET @start_time = GETDATE();

	-- INSERTING DATA 
		PRINT ' >> Truncate Category Information Table from any data' ;
		IF OBJECT_ID('Silver.erp_LOC_A101', 'U') IS NOT NULL 
			 TRUNCATE TABLE Silver.erp_PX_CAT_G1V2;

		PRINT ' >> Inserting / Loading Data from Bronze - Category - layer to silver - Category - layer ' ;
		INSERT INTO  Silver.erp_PX_CAT_G1V2
		(
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		)
		SELECT 
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		FROM Bronze.erp_PX_CAT_G1V2;

		-- END LOADING TIME 
		SET @end_time = GETDATE();

		-- COUNT THE DURATION 
		PRINT 'Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR ) + ' Seconds' 

		SET @END_LOADING_TIME = GETDATE();
		PRINT 'Silver Layer is already loaded from the bronze layer'
		PRINT 'Whole Batch Loading Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR ) + ' Seconds' 
		PRINT('_______________________________________________');
		print'********************************************************************************************'

	END TRY
	-- WE CATCH THE ERRORS TO HELP US TO FIX
	BEGIN CATCH 
		PRINT '! AN ERROR OCCURED...!!!';
		PRINT '!Error Message!!' + ERROR_MESSAGE();
		PRINT '!ERROR LINE!!' + CAST(ERROR_LINE() AS NVARCHAR) ;
		PRINT '!ERROR NUMBER!!' + CAST(ERROR_NUMBER() AS NVARCHAR) ;
		PRINT '!Error Message!!' + ERROR_MESSAGE();
		PRINT 'PROCEDURE NAME : ' + ERROR_PROCEDURE();
	END CATCH

END






