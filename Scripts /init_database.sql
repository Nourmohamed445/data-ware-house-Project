/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
*/

-- we will use the master server in data base so we can create our data wareHouse databse 
use master;

-- after it we have to check if there is a data base call dataWareHouse in our server to avoid the error 
-- DROP AND RECREATE THE DATABASE 

-- CHECKING the system 
IF EXISTS(SELECT 1 FROM sys.databases WHERE name ='DataWareHouse')
	BEGIN 
		-- make only one user for this database and stop any users from using it and cut the transactions 
		ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
		-- THE DELETE THE DATA BASE 
		DROP DATABASE DataWareHouse;
	END;

GO

-- After we have done the checking we go and create the database 
CREATE DATABASE DataWareHouse;
GO

-- USING IT
USE DataWareHouse;


-- CREATE THE SCHEMA 
CREATE SCHEMA Bronze;
Go 

CREATE SCHEMA Silver;
Go 

CREATE SCHEMA Gold;
Go 
