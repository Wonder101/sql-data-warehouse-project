/* CHECKING THE QUALITY OF THE BRONZE TABLES (for silver layer data cleaning) */ 

-- Counting duplicates and NULL primary keys
-- Expectation: No Results 
select cst_id, count(*) from bronze.crm_cust_info cci 
group by cst_id 
having count(*) > 1 or cst_id is null; 

-- Finding spaces 
-- Expectation: No Results 
select cst_key 
from bronze.crm_cust_info
where cst_key != trim(cst_key);

-- Check for negative numbers or NULLS 
-- Expectation: No results 
select prd_cost from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null; 

-- Data standardization and Consistency 
-- Mapping to meaningful values
select distinct cst_gndr 
from bronze.crm_cust_info; 
select distinct cst_marital_status
from bronze.crm_cust_info; 

-- Check for invalid date orders 
select * from bronze.crm_prd_info 
where prd_end_dt < prd_start_dt;




