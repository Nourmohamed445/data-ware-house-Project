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
CREATE OR ALTER PROCEDURE Bronze.load_bronze AS 

BEGIN 
	BEGIN TRY 
		DECLARE @start_time DATETIME , @end_time DATETIME; -- Creating variables
		DECLARE @START_LOADING_TIME DATETIME , @END_LOADING_TIME DATETIME -- VARIABLE FOR THE WHOLE BATCH

		PRINT('=============================================');
		PRINT('LOADING OUR DATA TO THE SILVER LAYER');
		PRINT('=============================================');

		PRINT('=-------------------------------------------=');
		PRINT(' -- 1- LOADING THE CRM FILES');
		PRINT('=-------------------------------------------=');

		SET @START_LOADING_TIME = GETDATE();
		-------------------------------------------------- 1 : CRM FILES ---------------------------------------
		--- Customer information 
		SET @start_time = GETDATE();
		PRINT('>> Truncate data from table : Bronze.crm_cust_info' );
		IF OBJECT_ID('Bronze.crm_cust_info' , 'U') IS NOT NULL 
			TRUNCATE TABLE Bronze.crm_cust_info;

		PRINT('>> INSERT data into table : Bronze.crm_cust_info' );
		BULK INSERT Bronze.crm_cust_info 
		FROM 'E:\Noor\SQL projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRST_ROW = 2 ,
			FIELDTERMINATOR = ','
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR ) + ' Seconds' 
		PRINT('_______________________________________________');


		--- Product information 
		SET @start_time = GETDATE();
		PRINT('>> Truncate data from table : Bronze.crm_prd_info' );
		IF OBJECT_ID('Bronze.crm_prd_info' , 'U') IS NOT NULL 
			TRUNCATE TABLE Bronze.crm_prd_info;

		PRINT('>> INSERT data into table : Bronze.crm_prd_info' );
		BULK INSERT Bronze.crm_prd_info
		FROM 'E:\Noor\SQL projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRST_ROW = 2 ,
			FIELDTERMINATOR = ','
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR ) + ' Seconds' 
		PRINT('_______________________________________________');


		--- Sales Details 
		SET @start_time = GETDATE();
		PRINT('>> Truncate data from table : Bronze.crm_sales_details' );
		IF OBJECT_ID('Bronze.crm_sales_details' , 'U') IS NOT NULL 
			TRUNCATE TABLE Bronze.crm_sales_details

		PRINT('>> INSERT data into table : Bronze.crm_sales_details' );
		BULK INSERT Bronze.crm_sales_details
		FROM 'E:\Noor\SQL projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',' 
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR ) + ' Seconds' 
		PRINT('_______________________________________________');


		PRINT('=-------------------------------------------=');
		PRINT(' -- 2- LOADING THE ERP FILES');
		PRINT('=-------------------------------------------=');

		--------------------------------------------------	2 : ERP FILES ---------------------------------------
		-- erp : customer Details
		SET @start_time = GETDATE();
		PRINT('>> Truncate data from table : Bronze.erp_CUST_AZ12' );
		IF OBJECT_ID('Bronze.erp_CUST_AZ12' , 'U') IS NOT NULL 
			TRUNCATE TABLE Bronze.erp_CUST_AZ12

		PRINT('>> INSERT data into table : Bronze.erp_CUST_AZ12' );
		BULK INSERT Bronze.erp_CUST_AZ12
		FROM 'E:\Noor\SQL projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',' 
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR ) + ' Seconds' 
		PRINT('_______________________________________________');


		-- erp : Location Details
		SET @start_time = GETDATE();
		PRINT('>> Truncate data from table : Bronze.erp_LOC_A101' );
		IF OBJECT_ID('Bronze.erp_LOC_A101' , 'U') IS NOT NULL 
			TRUNCATE TABLE Bronze.erp_LOC_A101

		PRINT('>> INSERT data into table : Bronze.erp_LOC_A101' );
		BULK INSERT Bronze.erp_LOC_A101
		FROM 'E:\Noor\SQL projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',' 
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR ) + ' Seconds' 
		PRINT('_______________________________________________');


		-- erp : Category Details
		SET @start_time = GETDATE();
		PRINT('>> Truncate data from table : Bronze.erp_PX_CAT_G1V2 ' );
		IF OBJECT_ID('Bronze.erp_PX_CAT_G1V2 ' , 'U') IS NOT NULL 
			TRUNCATE TABLE Bronze.erp_PX_CAT_G1V2 

		PRINT('>> INSERT data into table : Bronze.erp_PX_CAT_G1V2 ' );
		BULK INSERT Bronze.erp_PX_CAT_G1V2 
		FROM 'E:\Noor\SQL projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',' 
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR ) + ' Seconds' 
		PRINT('_______________________________________________');

		SET @END_LOADING_TIME = GETDATE();
		PRINT 'Whole Batch Loading Duration : ' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR ) + ' Seconds' 
		PRINT('_______________________________________________');
	END TRY 

	--------------------------------------- CATCH ( Handling the Error ) --------------------------------
	BEGIN CATCH
		PRINT('----------------------------------------------------');
		PRINT('ERROR OCCURED DURING LOADING THE DATA FROM THE FILES');
		PRINT('ERROR MESSAGE : ' + ERROR_MESSAGE()); -- RETURN THE MESSAGE OF THE ERROR 
		PRINT('ERROR MESSAGE : ' +	CAST(ERROR_NUMBER() AS NVARCHAR ));	-- RETURN THE ERROR NUMBER
		PRINT('ERROR MESSAGE : ' + ERROR_PROCEDURE());		-- RETURN THE PREOCEDURE NAME THAT OCCURED AN ERROR 
		PRINT('ERROR MESSAGE : ' + CAST(ERROR_LINE()AS NVARCHAR)); -- RETURN THE LINE NUMBER THAT HAS THE ERROR 

		PRINT('----------------------------------------------------');
	END CATCH
END 
