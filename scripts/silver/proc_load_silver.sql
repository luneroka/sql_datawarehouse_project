/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL Silver.load_silver();
===============================================================================
*/

create or replace procedure silver.load_silver()
language plpgsql
as $$
declare
	v_start_ts timestamp;
	v_end_ts timestamp;
	v_elapsed interval;
begin
	v_start_ts := clock_timestamp();

-- =========================================
-- Processing Table: silver.crm_cust_info
-- =========================================

-- Drop constraints and allow NULLs for ingestion
alter table silver.crm_cust_info drop constraint if exists crm_cust_info_pkey;
alter table silver.crm_cust_info alter column cst_id drop not null;

-- Truncate table
truncate table silver.crm_cust_info;

-- Load Data into silver.crm_cust_info
insert into silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
select
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case
		when upper(trim(cst_marital_status)) = 'S' then 'Single'
		when upper(trim(cst_marital_status)) = 'M' then 'Married'
		else 'N/A'
	end as cst_marital_status,	
	case 
		when upper(trim(cst_gndr)) = 'F' then 'Female'
		when upper(trim(cst_gndr)) = 'M' then 'Male'
		else 'N/A'
	end as cst_gndr,
	cst_create_date
from (
	select 	
		*,
		row_number() over(partition by cst_id order by cst_create_date DESC) as flag_last
	from bronze.crm_cust_info
)t where flag_last = 1;



-- =========================================
-- Processing Table: silver.crm_prd_info
-- =========================================

-- Drop constraints and allow NULLs for ingestion
alter table silver.crm_prd_info drop constraint if exists crm_prd_info_pkey;
alter table silver.crm_prd_info alter column prd_id drop not null;

-- Truncate table
truncate table silver.crm_prd_info;

-- Load Data
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
	replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
	substring(prd_key from 7) as prd_key,
	prd_nm,
	coalesce(prd_cost, 0) as prd_cost,
	case upper(trim(prd_line))
		when 'M' then 'Mountain'
		when 'R' then 'Road'
		when 'S' then 'Other Sales'
		when 'T' then 'Touring'
		else 'N/A'
	end as prd_line,	
	cast(prd_start_dt as date) as prd_start_dt,
	cast(lead(prd_start_dt::date) over (partition by prd_key order by prd_start_dt) - 1 as date) as prd_end_dt
from bronze.crm_prd_info;



-- =========================================
-- Processing Table: silver.crm_sales_details
-- =========================================

-- Drop constraints and allow NULLs for ingestion
alter table silver.crm_sales_details drop constraint if exists crm_sales_details_pkey;
alter table silver.crm_sales_details alter column sls_ord_num drop not null;

-- Truncate table
truncate table silver.crm_sales_details;

-- Load Data
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
	case
		when sls_order_dt = 0 or length(cast(sls_order_dt as varchar)) != 8 then null
		else to_date(cast(sls_order_dt as varchar), 'YYYYMMDD')
	end as sls_order_dt,
	case
		when sls_ship_dt = 0 or length(cast(sls_ship_dt as varchar)) != 8 then null
		else to_date(cast(sls_ship_dt as varchar), 'YYYYMMDD')
	end as sls_ship_dt,
	case
		when sls_due_dt = 0 or length(cast(sls_due_dt as varchar)) != 8 then null
		else to_date(cast(sls_due_dt as varchar), 'YYYYMMDD')
	end as sls_due_dt,
	case 
		when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price)
		else sls_sales
	end as sls_sales,
	sls_quantity,
	case
		when sls_price is null or sls_price <= 0 then (sls_sales::numeric / nullif(sls_quantity, 0))
		else sls_price
	end as sls_price	
from bronze.crm_sales_details;


-- =========================================
-- Processing Table: silver.erp_cust_az12
-- =========================================

-- Drop constraints and allow NULLs for ingestion
alter table silver.erp_cust_az12  drop constraint if exists erp_cust_az12_pkey;
alter table silver.erp_cust_az12 alter column cid drop not null;

-- Truncate table
truncate table silver.erp_cust_az12;

-- Load Data
insert into silver.erp_cust_az12 (
	cid,
	bdate,
	gen
)
select
	case
		when cid like 'NAS%' then substring(cid, 4, length(cid))
		else cid
	end as cid,	
	case
		when bdate > current_date then null 
		else bdate
	end as bdate,	
	case 
		when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
		when upper(trim(gen)) in ('M', 'MALE') then 'Male'
		else 'N/A'
	end as gen	
from bronze.erp_cust_az12;



-- =========================================
-- Processing Table: silver.erp_loc_a101
-- =========================================

-- Drop constraints and allow NULLs for ingestion
alter table silver.erp_loc_a101  drop constraint if exists erp_loc_a101_pkey;
alter table silver.erp_loc_a101 alter column cid drop not null;

-- Truncate table
truncate table silver.erp_loc_a101;

-- Load Data
insert into silver.erp_loc_a101 (
	cid,
	cntry
)
select
	replace(cid, '-', '') as cid,
	case
		when trim(cntry) = 'DE' then 'Germany'
		when trim(cntry) in ('US', 'USA') then 'United States'
		when trim(cntry) = '' or cntry is null then 'N/A'
		else trim(cntry)
	end as cntry	
from bronze.erp_loc_a101;



-- =========================================
-- Processing Table: silver.erp_px_cat_g1v2
-- =========================================

-- Drop constraints and allow NULLs for ingestion
alter table silver.erp_px_cat_g1v2 drop constraint if exists erp_px_cat_g1v2_pkey;
alter table silver.erp_px_cat_g1v2 alter column id drop not null;

-- Truncate table
truncate table silver.erp_px_cat_g1v2;

-- Load Data
insert into silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance
)
select
	id,
	cat,
	subcat,
	maintenance
from bronze.erp_px_cat_g1v2;

	v_end_ts := clock_timestamp();
	v_elapsed := v_end_ts - v_start_ts;
	raise notice 'Total silver load duration: %', v_elapsed;

end;
$$;