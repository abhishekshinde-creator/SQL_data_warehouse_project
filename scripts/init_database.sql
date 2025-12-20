/*
===============================================================================
Create Database and Schemas
===============================================================================
Script Purpose:
    This script creates a new database 'DataWarehouse' after checking if it already exists.
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
    within the database: bronze, Silver, and gold.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution
    and ensure you have proper backup before running the script.
*/

use master;
go

  -- Drop and recreate the 'DataWarehouse' database
  if EXISTS (select 1 from sys.databases WHERE name = 'DataWarehouse')
  BEGIN
    ALTER DATABASE DataWarehouse SET_SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;

GO

  -- CREATE the 'DataWarehouse' database
create database DataWarehouse;

go

use DataWarehouse;

go

  --create schema
create schema bronze;

go

create schema Silver;

go

create schema gold;

go
