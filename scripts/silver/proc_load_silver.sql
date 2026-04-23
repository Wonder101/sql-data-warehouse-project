create or replace procedure silver.load_silver ()
LANGUAGE plpgsql
AS $$
begin 
	-- INSERTING DATA INTO silver.crm_cust_info
	truncate table silver.crm_cust_info;
	insert into silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)
	-- handle deduplication by keeping only the most recent record per customer
	-- newesest tied data gets 1, thus kept when using row_number() where each row gets a unique row no matter what
	select 
		cst_id, 
		cst_key, 
		trim(cst_firstname), -- cleaned up (no unwanted spaces)
		trim(cst_lastname), -- cleaned up (no unwanted spaces)
		case when trim(upper(cst_marital_status)) = 'S' then 'Single' 
			when trim(upper(cst_marital_status)) = 'M' then 'Married'
			else 'n/a'
		end cst_marital_status, -- mapping 'S' to 'Single', 'M' to 'Married', 'n/a' otherwise
		case when trim(upper(cst_gndr)) = 'F' then 'Female'
			when trim(upper(cst_gndr)) = 'M' then 'Male'
			else 'n/a'
		end cst_gndr, -- mapping 'F' to 'Female', 'M' to 'Male', 'n/a' otherwise
		cst_create_date 
	from (
	select *, row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info )t
	where flag_last = 1;
	
	
	-- INSERTING DATA INTO silver.crm_prd_info
	truncate table silver.crm_prd_info;
	insert into silver.crm_prd_info ( 
			prd_id,
			cat_id,
			prd_key,
			prd_nm,  
			prd_cost,
			prd_line, 
			prd_start_dt, 
			prd_end_dt 
	)
	select 
		prd_id,
		replace(substring(prd_key, 1, 5), '-', '_') as cat_id, -- first five letters of the product key is the category id 
		substring(prd_key, 7, length(prd_key)) as prd_key, -- last five letters of the product key is the actual product key 
		prd_nm,
		coalesce(prd_cost,0) as prd_cost,
		case when upper(trim(prd_line)) = 'M' then 'Mountain'
			when upper(trim(prd_line)) = 'R' then 'Road'
			when upper(trim(prd_line)) = 'S' then 'Other Sales' 
			when upper(trim(prd_line)) = 'T' then 'Touring' 
			else 'n/a'
		end as prd_line, 
		cast(prd_start_dt as date) as prd_start_dt,
		cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) - interval '1 day' as date)  as prd_end_dt
	from bronze.crm_prd_info;
	
	
	-- INSERTING DATA INTO silver.crm_sales_details
	truncate table silver.crm_sales_details;
	insert into silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	select
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case when sls_order_dt = 0 or length(cast(sls_order_dt as varchar)) !=8 then null 
		ELSE cast(cast(sls_order_dt as varchar) as date)
	end as sls_order_dt,
	case when sls_ship_dt = 0 or length(cast(sls_ship_dt as varchar)) !=8 then null 
		ELSE cast(cast(sls_ship_dt as varchar) as date)
	end as sls_ship_dt,
	case when sls_due_dt = 0 or length(cast(sls_due_dt as varchar)) !=8 then null 
		ELSE cast(cast(sls_due_dt as varchar) as date)
	end as sls_due_dt,
	case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price) then 
		sls_quantity * abs(sls_price)
	else sls_sales
	end as sls_sales,
	sls_quantity,
	case when sls_price is null or sls_price <= 0 then 
	sls_sales / nullif(sls_quantity,0) 
	else sls_price 
	end as sls_price
	from bronze.crm_sales_details;
	
	
	-- INSERTING DATA INTO silver.erp_cust_az12
	truncate table silver.erp_cust_az12;
	insert into silver.erp_cust_az12 (
	cid, 
	bdate, 
	gen
	)
	select 
	case when cid like 'NAS%' then 
		substring(cid,4,length(cid))
		else cid 
	end as cid, 
	case when bdate > now() then null 
	else bdate 
	end as bdate,
	case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
		when upper(trim(gen)) in ('M', 'Male') then 'Male' 
		else 'n/a'
	end as gen 
	from bronze.erp_cust_az12;
	
	
	-- INSERTING DATA INTO silver.erp_loc_a101
	truncate table silver.erp_loc_a101;
	insert into silver.erp_loc_a101(
	cid, 
	cntry)
	select 
	replace(cid,'-',''), 
	case when trim(cntry) = 'DE' then 'Germany'
		when trim(cntry) in ('US', 'USA') then 'United States'
		when trim(cntry) = '' or cntry is null then 'n/a' 
		else trim(cntry)
	end as cntry
	from bronze.erp_loc_a101; 
	
	
	-- INSERTING DATA INTO silver.erp_px_cat_g1v2
	truncate table silver.erp_px_cat_g1v2;
	insert into silver.erp_px_cat_g1v2 
	(	id,
		cat,
		subcat,
		maintenance)
	select 
		id,
		cat,
		subcat,
		maintenance
	from bronze.erp_px_cat_g1v2;
end;
$$;
