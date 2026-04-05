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
    EXEC Silver.load_silver;
===============================================================================
*/

-- =========================================
-- Processing Table: silver.crm_cust_info
-- =========================================

-- Drop constraints and allow NULLs for ingestion
alter table silver.crm_cust_info drop constraint if exists crm_cust_info_pkey;
alter table silver.crm_cust_info alter column cst_id drop not null;

-- Truncate table
truncate table silver.crm_cust_info;

-- Quality checks on bronze data before transformation

-- Check for NULLs or duplicates in PK
select
  cst_id,
  count(cst_id)
from bronze.crm_cust_info cci
group by cst_id
having count(*) > 1 or cst_id is null;

-- Check for unwanted spaces
select 
  cst_firstname 
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname);

-- Data standardization & Consistency
select 
  distinct cst_gndr
from bronze.crm_cust_info;

-- Transformation
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

-- Verify quality of silver.crm_cust_info data

-- Check for NULLs or duplicates in PK
select
  cst_id,
  count(cst_id)
from silver.crm_cust_info cci
group by cst_id
having count(*) > 1 or cst_id is null;

-- Check for unwanted spaces
select 
  cst_firstname 
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname);

-- Data standardization & Consistency
select distinct cst_marital_status from silver.crm_cust_info;
select distinct cst_gndr from silver.crm_cust_info;

-- Final look to the table
select * from silver.crm_cust_info;



-- =========================================
-- Processing Table: silver.crm_prd_info
-- =========================================

-- Drop constraints and allow NULLs for ingestion
alter table silver.crm_prd_info drop constraint if exists crm_prd_info_pkey;
alter table silver.crm_prd_info alter column prd_id drop not null;

-- Truncate table
truncate table silver.crm_prd_info;

-- Quality checks on bronze data before transformation

-- Check for NULLs or duplicates in PK
select 
	prd_id,
	count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;

-- Check for unwanted spaces
select 
  prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm);

-- Check for NULLs or negative numbers
select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- Data standardization & Consistency
select distinct prd_line from bronze.crm_prd_info;

-- Check for invalid date orders
select
	*
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt;

-- Transformation
-- 1) Split prd_key column to extract category_id and prd_key
-- 2) Replace prd_cost NULL values with 0
select
	prd_id,
	replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
	substring(prd_key, 7, length(prd_key)) as prd_key,
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
	substring(prd_key, 7, length(prd_key)) as prd_key,
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

-- Verify quality of silver.crm_prd_info data

-- Check for NULLs or duplicates in PK
select 
	prd_id,
	count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;

-- Check for unwanted spaces
select 
  prd_nm
from silver.crm_prd_info
where prd_nm != trim(prd_nm);

-- Check for NULLs or negative numbers
select prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- Data standardization & Consistency
select distinct prd_line from silver.crm_prd_info;

-- Check for invalid date orders
select
	*
from silver.crm_prd_info
where prd_end_dt < prd_start_dt;

-- Final look to the table
select * from silver.crm_prd_info;



-- =========================================
-- Processing Table: silver.crm_sales_details
-- =========================================

-- Drop constraints and allow NULLs for ingestion
alter table silver.crm_sales_details drop constraint if exists crm_sales_details_pkey;
alter table silver.crm_sales_details alter column sls_ord_num drop not null;

-- Truncate table
truncate table silver.crm_sales_details;

-- Quality checks on bronze data before transformation

-- Check for unwanted spaces
select
	sls_ord_num
from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num);

-- Check for integrity of prd_key and cust_id cols
select 
	sls_prd_key
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info);

select 
	sls_cust_id
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info);

-- Check for invalid dates (duplicate with all _dt cols)
select 
	nullif(sls_order_dt, 0) as sls_order_dt
from bronze.crm_sales_details
where 
	sls_order_dt <= 0 
	or length(cast(sls_order_dt as varchar)) != 8
	or sls_order_dt > 20500101
	or sls_order_dt < 19000101;

-- Check for invalid date orders
select 
	*
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

-- Check data consistency between quantity, price and sales 
-- (sales = quantity * price), values must not be NULL, zero or negative
select distinct
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details
where 
	sls_sales != sls_quantity * sls_price
	or sls_sales is null or sls_quantity is null or sls_price is null
	or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
	order by sls_sales, sls_quantity, sls_price;

-- Transformation
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
		when sls_price is null or sls_price <= 0 then sls_sales / nullif(sls_quantity, 0)
		else sls_price
	end as sls_price	
from bronze.crm_sales_details;

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
		when sls_price is null or sls_price <= 0 then sls_sales / nullif(sls_quantity, 0)
		else sls_price
	end as sls_price	
from bronze.crm_sales_details;

-- Verify quality of silver.crm_sales_details data

-- Check for unwanted spaces
select
	sls_ord_num
from silver.crm_sales_details
where sls_ord_num != trim(sls_ord_num);

-- Check for integrity of prd_key and cust_id cols
select 
	sls_prd_key
from silver.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info);

select 
	sls_cust_id
from silver.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info);

-- Check for invalid date orders
select 
	*
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

-- Check data consistency between quantity, price and sales 
-- (sales = quantity * price), values must not be NULL, zero or negative
select distinct
	sls_sales,
	sls_quantity,
	sls_price
from silver.crm_sales_details
where 
	sls_sales != sls_quantity * sls_price
	or sls_sales is null or sls_quantity is null or sls_price is null
	or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
	order by sls_sales, sls_quantity, sls_price;

-- Final look to the table
select * from silver.crm_sales_details;



-- =========================================
-- Processing Table: silver.erp_cust_az12
-- =========================================

-- Drop constraints and allow NULLs for ingestion
alter table silver.erp_cust_az12  drop constraint if exists erp_cust_az12_pkey;
alter table silver.erp_cust_az12 alter column cid drop not null;

-- Truncate table
truncate table silver.erp_cust_az12;

-- Quality checks on bronze data before transformation

-- Check cid
	-- note : done directly on the transformation

-- Identify out-of-range dates
select distinct
	bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > current_date;

-- Data standardization & consistency
select distinct 
	gen
from bronze.erp_cust_az12;

-- Transformation
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

-- Verify quality of silver.erp_cust_az12 data

-- Check cid -> compare match with foreign key
	-- note : done directly on the transformation

-- Identify out-of-range dates
select distinct
	bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > current_date;

-- Data standardization & consistency
select distinct 
	gen
from silver.erp_cust_az12;


-- Final look to the table
select * from silver.erp_cust_az12;



-- =========================================
-- Processing Table: silver.erp_loc_a101
-- =========================================

-- Drop constraints and allow NULLs for ingestion
alter table silver.erp_loc_a101  drop constraint if exists erp_loc_a101_pkey;
alter table silver.erp_loc_a101 alter column cid drop not null;

-- Truncate table
truncate table silver.erp_loc_a101;

-- Quality checks on bronze data before transformation

-- Check cid -> compare match with foreign key
	-- note : done directly on the transformation

-- Data standardization & Consistency
select distinct
	cntry
from bronze.erp_loc_a101
order by cntry;

-- Transformation
select
	replace(cid, '-', '') as cid,
	case
		when trim(cntry) = 'DE' then 'Germany'
		when trim(cntry) in ('US', 'USA') then 'United States'
		when trim(cntry) = '' or cntry is null then 'N/A'
		else trim(cntry)
	end as cntry	
from bronze.erp_loc_a101;

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

-- Verify quality of silver.erp_loc_a101 data

-- Check cid -> compare match with foreign key
	-- note : done directly on the transformation

-- Data standardization & Consistency
select distinct
	cntry
from silver.erp_loc_a101
order by cntry;

-- Final look to the table
select * from silver.erp_loc_a101;



-- =========================================
-- Processing Table: silver.erp_px_cat_g1v2
-- =========================================

-- Drop constraints and allow NULLs for ingestion
alter table silver.erp_px_cat_g1v2 drop constraint if exists erp_px_cat_g1v2_pkey;
alter table silver.erp_px_cat_g1v2 alter column id drop not null;

-- Truncate table
truncate table silver.erp_px_cat_g1v2;

-- Quality checks on bronze data before transformation

-- Check cid -> compare match with foreign key
	-- note : done directly on the transformation

-- Check for unwanted spaces
select 
	*
from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance);

-- Data standardization & Consistency (alternate between cat, subcat and maintenance cols)
select distinct
	maintenance
from bronze.erp_px_cat_g1v2;

-- Transformation
select
	id,
	cat,
	subcat,
	maintenance
from bronze.erp_px_cat_g1v2;

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

-- Verify quality of silver.erp_px_cat_g1v2 data

-- Check cid -> compare match with foreign key
	-- note : done directly on the transformation

-- Check for unwanted spaces
select 
	*
from silver.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance);

-- Data standardization & Consistency (alternate between cat, subcat and maintenance cols)
select distinct
	maintenance
from silver.erp_px_cat_g1v2;

-- Final look to the table
select * from silver.erp_px_cat_g1v2;