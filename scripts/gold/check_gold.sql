/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================

-- BEFORE CHECKS

-- Join all customer related tables and check for duplicate cst_id rows
select 
	cst_id,
	count(*)
from (
	select 
		cci.cst_id,
		cci.cst_key,
		cci.cst_firstname,
		cci.cst_lastname,
		cci.cst_marital_status,
		cci.cst_gndr,
		cci.cst_create_date,
		eca.bdate,
		eca.gen,
		ela.cntry
	from silver.crm_cust_info cci 
	left join silver.erp_cust_az12 eca 
		on cci.cst_key = eca.cid
	left join silver.erp_loc_a101 ela 
		on cci.cst_key = ela.cid
)t group by cst_id
having count(*) > 1;

-- Data Integration : handle duplicate gender cols
select distinct
	cci.cst_gndr,
	eca.gen,
	case 
		when cci.cst_gndr != 'N/A' then cci.cst_gndr -- CRM is the Master for gender info
		else coalesce(eca.gen, 'N/A')
	end as new_gen
from silver.crm_cust_info cci 
left join silver.erp_cust_az12 eca 
	on cci.cst_key = eca.cid
left join silver.erp_loc_a101 ela 
	on cci.cst_key = ela.cid
order by 1, 2;


-- AFTER CHECKS

-- Verify gender values in the final view
select distinct gender from gold.dim_customers;

-- Check for duplicate surrogate keys in dim_customers
select 
    customer_key,
    count(*) as duplicate_count
from gold.dim_customers
group by customer_key
having count(*) > 1;



-- ====================================================================
-- Checking 'gold.dim_products'
-- ====================================================================

-- BEFORE CHECKS

-- Join all product related tables and check for duplicate prd_key` rows
select 
	prd_key,
	count(*)
from (
	select
		cpi.prd_id,
		cpi.cat_id,
		cpi.prd_key,
		cpi.prd_nm,
		cpi.prd_cost,
		cpi.prd_line,
		cpi.prd_start_dt,
		cpi.prd_end_dt,
		epcgv.cat,
		epcgv.subcat,
		epcgv.maintenance
	from silver.crm_prd_info cpi
	left join silver.erp_px_cat_g1v2 epcgv 
		on cpi.cat_id = epcgv.id
	where cpi.prd_end_dt is null -- filter out all historical data
)t 
group by prd_key
having count(*) > 1;

-- AFTER CHECKS 

select * from gold.dim_products;



-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================

-- Foreign key integrity (dimensions)
select 
	*
from gold.fact_sales fs
left join gold.dim_customers dc 
	on fs.customer_key = dc.customer_key 
where dc.customer_key is null;

select 
	*
from gold.fact_sales fs
left join gold.dim_products dp 
	on fs.product_key = dp.product_key 
where dp.product_key is null;


