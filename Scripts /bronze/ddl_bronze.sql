--=================================
-- CREATE THE CRM FILES INTO TABLES 
--=================================

-- check first if it exists already 
IF OBJECT_ID('Bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE Bronze.crm_cust_info
	GO
-- CREATE THE FIRST TABLE IN THE BRONZE LAYER USING THE NAME DEFINATION (sourcename + filename  )
CREATE TABLE Bronze.crm_cust_info
(
	cst_id					INT ,
	cst_key					NVARCHAR(50),
	cst_firstname			NVARCHAR(50),
	cst_lastname			NVARCHAR(50),
	cst_marital_status		NVARCHAR(50),
	cst_gndr				NVARCHAR(50),
	cst_create_date			DATE 
);

-- check first if it exists already 
IF OBJECT_ID('Bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE Bronze.crm_prd_info ;
	GO
-- CREATE THE second TABLE IN THE BRONZE LAYER USING THE NAME DEFINATION (sourcename + filename  )
CREATE TABLE Bronze.crm_prd_info
(
	prd_id			INT ,
	prd_key			NVARCHAR(50),
	prd_nm			NVARCHAR(50),
	prd_cost		INT,
	prd_line		NVARCHAR(50),
	prd_start_dt	DATETIME,
	prd_end_dt		DATETIME 
);


-- check first if it exists already 
IF OBJECT_ID('Bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE Bronze.crm_sales_details ;
	GO
-- CREATE THE THIRD TABLE IN THE BRONZE LAYER USING THE NAME DEFINATION (sourcename + filename  )
CREATE TABLE Bronze.crm_sales_details
(
	sls_ord_num			NVARCHAR(50),
	sls_prd_key			NVARCHAR(50),
	sls_cust_id			INT,
	sls_order_dt		INT,
	sls_ship_dt			INT,
	sls_due_dt			INT,
	sls_sales			INT,
	sls_quantity		INT,
	sls_price			INT
);

--=================================
-- CREATE THE ERP FILES INTO TABLES 
--=================================

-- check first if it exists already 
IF OBJECT_ID('Bronze.erp_CUST_AZ12', 'U') IS NOT NULL
    DROP TABLE Bronze.erp_CUST_AZ12 ;
	GO
-- CREATE THE FOURTH TABLE IN THE BRONZE LAYER USING THE NAME DEFINATION (sourcename + filename  )
CREATE TABLE Bronze.erp_CUST_AZ12
(
	CID			NVARCHAR(50),
	BDATE		DATE ,
	GEN			NVARCHAR(50)
);


-- check first if it exists already 
IF OBJECT_ID('Bronze.erp_LOC_A101', 'U') IS NOT NULL
    DROP TABLE Bronze.erp_LOC_A101 ;
	GO
-- CREATE THE FIFTH TABLE IN THE BRONZE LAYER USING THE NAME DEFINATION (sourcename + filename  )
CREATE TABLE Bronze.erp_LOC_A101
(
	CID			NVARCHAR(50),
	CNTRY		NVARCHAR(50)
);



-- check first if it exists already 
IF OBJECT_ID('Bronze.erp_PX_CAT_G1V2', 'U') IS NOT NULL
    DROP TABLE Bronze.erp_PX_CAT_G1V2 ;
	GO
-- CREATE THE SIXTH TABLE IN THE BRONZE LAYER USING THE NAME DEFINATION (sourcename + filename  )
CREATE TABLE Bronze.erp_PX_CAT_G1V2
(
	ID					NVARCHAR(50),
	CAT					NVARCHAR(50),
	SUBCAT				NVARCHAR(50),
	MAINTENANCE			NVARCHAR(50)

);

