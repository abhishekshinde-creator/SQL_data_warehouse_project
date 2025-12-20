/*
==========================================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==========================================================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses the "BULK INSERT" command to load data from CSV files to bronze tables.

Parameters:
  None.
  This stored procedure does not accept any parameters and does not return any values.

Usage Example:
  EXEC bronze.load_bronze;
==========================================================================================================
*/

--SINCE THIS SCRIPT IS USED FREQUENTLY TO UPDATE DATA, WE CREATE STORED PROCEDURE

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	BEGIN TRY
		
		SET @batch_start_time = getdate();

		print'================================================';
		print 'Loading the bronze layer';
		print'================================================';

			-- to avoid duplicates always truncate first then load

		print'================================================';
		print 'Loading the CRM tables';
		print'================================================';

			set @start_time = getdate();	
				print'Truncating table: bronze.crm_cust_info';
				truncate table bronze.crm_cust_info;

				print'Inserting table: bronze.crm_cust_info';
				bulk insert bronze.crm_cust_info
				from 'D:\SQL resources\SQL data warehouse project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
				with (
					firstrow = 2,
					fieldterminator = ',',
					tablock
				);
			set @end_time = getdate();	
			print'>> Load Duration:' +cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
			print'>>------------------------------------------'

			--data completeness test

			--select count(*) from bronze.crm_cust_info

			--loading other files

			set @start_time = getdate();	
				print'Truncating table: bronze.crm_prd_info';
				truncate table bronze.crm_prd_info;

				print'Inserting table: bronze.crm_prd_info';
				bulk insert bronze.crm_prd_info
				from 'D:\SQL resources\SQL data warehouse project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
				with (
					firstrow = 2,
					fieldterminator = ',',
					tablock
				);
			set @end_time = getdate();	
			print'>> Load Duration:' +cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
			print'>>------------------------------------------'

			set @start_time = getdate();
				print'Truncating table: bronze.crm_sales_details';
				truncate table bronze.crm_sales_details;

				print'Inserting table: bronze.crm_sales_details';
				bulk insert bronze.crm_sales_details
				from 'D:\SQL resources\SQL data warehouse project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
				with (
					firstrow = 2,
					fieldterminator = ',',
					tablock
				);
			set @end_time = getdate();	
			print'>> Load Duration:' +cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
			print'>>------------------------------------------'


			print'================================================';
			print 'Loading the ERP tables';
			print'================================================';

			set @start_time = getdate();
				print'Truncating table: bronze.erp_cust_az12';
				truncate table bronze.erp_cust_az12;

				print'Inserting table: bronze.erp_cust_az12';

				bulk insert bronze.erp_cust_az12
				from 'D:\SQL resources\SQL data warehouse project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
				with (
					firstrow = 2,
					fieldterminator = ',',
					tablock
				);
			set @end_time = getdate();	
			print'>> Load Duration:' +cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
			print'>>------------------------------------------'

			set @start_time = getdate();
				print'Truncating table: bronze.erp_loc_a101';
				truncate table bronze.erp_loc_a101;

				print'inserting table: bronze.erp_loc_a101';
				bulk insert bronze.erp_loc_a101
				from 'D:\SQL resources\SQL data warehouse project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
				with (
					firstrow = 2,
					fieldterminator = ',',
					tablock
				);
			set @end_time = getdate();	
			print'>> Load Duration:' +cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
			print'>>------------------------------------------'

			set @start_time = getdate();
				print'Truncating table: bronze.erp_px_cat_g1v2';
				truncate table bronze.erp_px_cat_g1v2;

				print'Inserting table: bronze.erp_px_cat_g1v2';
				bulk insert bronze.erp_px_cat_g1v2
				from 'D:\SQL resources\SQL data warehouse project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
				with (
					firstrow = 2,
					fieldterminator = ',',
					tablock
				);
			set @end_time = getdate();	
			print'>> Load Duration:' +cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
			print'>>------------------------------------------'

		
		set @batch_end_time = getdate();
		print'==================================';
		print'Loading Bronze Layer is Completed';
		print' - Total Load Duration:' +cast(datediff(second,@batch_start_time, @batch_end_time) as nvarchar) + ' seconds';
	
	END TRY
	BEGIN CATCH
		print '==============================================';
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		print 'Error Message' + error_message();
		print 'Error Message' + cast(error_number() as nvarchar);
		print 'Error Message' + cast(error_state() as nvarchar);
	END CATCH
END


