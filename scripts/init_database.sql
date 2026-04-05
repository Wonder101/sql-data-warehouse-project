/*
 =================================================
 Create Database and Schemas 
 =================================================
 
  Script purpose: 
  	This script creats a new database named 'datawarehouse' after checking if it already exists.
  	If the database exists, it is dropped and recreated. Additionally, the script sets up three 
  	schemas within the database: 'bronze', 'silver', and 'gold'. 
  	
  WARNING: 
  	Running this script will drop the entire 'datawarehouse' database if it exists.
  	All data in the database will be permanetly deleted. Proceed with caution and
  	ensure that you have proper backups before running this script. 
  	 
 */

-- drop and recreate the 'datawarehouse' database 
drop if exists datawarehouse; 
create database datawarehouse; 

-- create schemas
create schema bronze; 
create schema silver; 
create schema gold; 
