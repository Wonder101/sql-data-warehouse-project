/*
	================================================================
	Stored Procedure: Load Bronze Layer (Source -> Bronze)  
	================================================================
	
	Script Purpose: 
		This stored procedure loads data into the 'bronze' schema from 
		external CSV files. It performs the following actions: 
		- Truncates the bronze tables before loading data. 
		- Uses the 'COPY' command to load data from csv files
		
	Parameters: None. 
	This stored procedure does not accept any parameters or return any values. 
	
	Usage Example: 
		CALL bronze.load_bronze(); 
	================================================================
 */
drop procedure if exists bronze.load_bronze;
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $procedure$
declare start_time timestamp; 
declare end_time timestamp; 
declare batch_start timestamp; 
declare batch_end timestamp; 
begin
	begin 
		batch_start := clock_timestamp(); 
		raise notice 'Loading Bronze Layer';

		start_time := clock_timestamp();
		raise notice 'Loading CRM Tables';
		raise notice '>>Truncating Table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
		raise notice '>>Inserting Data Into: bronze.crm_cust_info';
		copy bronze.crm_cust_info
		from '/Users/isabella/Documents/Learning/SQL/Baraa Course/proj_files/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
		with (
		format csv, 
		header true, 
		delimiter ','
		); 
		end_time := clock_timestamp(); 
		raise notice '>>Load Duration: %', end_time - start_time; 
		
		
		start_time := clock_timestamp(); 
		raise notice '>>Truncating Table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
		raise notice '>>Inserting Data Into: bronze.crm_prd_info';
		copy bronze.crm_prd_info 
		from '/Users/isabella/Documents/Learning/SQL/Baraa Course/proj_files/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
		with (
			format csv, 
			header true,
			delimiter ','
		);
		end_time := clock_timestamp(); 
		raise notice '>>Load Duration: %', end_time - start_time;
		
		start_time := clock_timestamp();
		raise notice '>>Truncating Table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details; 
		raise notice '>>Inserting Data Into: bronze.crm_sales_details';
		copy bronze.crm_sales_details 
		from '/Users/isabella/Documents/Learning/SQL/Baraa Course/proj_files/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
		with (
			format csv, 
			header true, 
			delimiter ','
		); 
		end_time := clock_timestamp(); 
		raise notice '>>Load Duration: %', end_time - start_time; 

		start_time := clock_timestamp(); 
		raise notice 'Loading ERP Tables';
		raise notice '>>Truncating Table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;
		raise notice '>>Inserting Data Into: bronze.erp_cust_az12';
		copy bronze.erp_cust_az12
		from '/Users/isabella/Documents/Learning/SQL/Baraa Course/proj_files/sql-data-warehouse-project/datasets/source_crm/CUST_AZ12.csv'
		with ( 
			format csv, 
			header true, 
			delimiter ','
		); 
		end_time := clock_timestamp(); 
		raise notice '>>Load Duration: %', end_time - start_time; 
		
		start_time := clock_timestamp(); 
		raise notice '>>Truncating Table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101; 
		raise notice '>>Inserting Data Into: bronze.erp_loc_a101';
		copy bronze.erp_loc_a101
		from '/Users/isabella/Documents/Learning/SQL/Baraa Course/proj_files/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
		with (
			format csv, 
			header true, 
			delimiter ','
		); 
		end_time := clock_timestamp(); 
		raise notice '>>Load Duration: %', end_time - start_time; 		

		start_time := clock_timestamp(); 
		raise notice '>>Truncating Table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
		raise notice '>>Inserting Data Into: bronze.erp_px_catg1v2';
		copy bronze.erp_px_cat_g1v2
		from '/Users/isabella/Documents/Learning/SQL/Baraa Course/proj_files/sql-data-warehouse-project/datasets/source_crm/PX_CAT_G1V2.csv'
		with (
			format csv, 
			header true, 
			delimiter ','
		); 
		end_time := clock_timestamp(); 
		raise notice '>>Load Duration: %', end_time - start_time; 
		batch_end := clock_timestamp(); 
		raise notice '>>Batch Completion Time: %', batch_end - batch_start;

	exception 
	when others then  
		raise notice 'ERROR OCCURED DURING LOADING BRONZE LAYER'; 
		raise notice 'Error Message: %', sqlerrm;
		raise notice 'Error Code: %', sqlstate;  
	end;
end;
$procedure$;
