/*
====================================================
Create database en schemas
====================================================
Script Purpose:
	This script creates a new database named 'DataWarehouse' after checking if it already exists. 
	If so, then it is dropped and recreated. Additionally, the script sets up three schemas within 
	the database: 'bronze', 'silver', and 'gold'.

Warning :
	Running this script will delete a database named 'DataWarehouse' if it already exists.
	All the data will be lost. Proceed with caution and ensure that you have a proper backup
	before running this script
*/


USE master;

-- Drop DateWarehouse database if it already exists. Dan recreate it
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create database 'DataWarehouse' 
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--Create schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
