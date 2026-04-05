
/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/


-- =================
-- CRM tables
-- =================

-- Drop tables if exist
drop table if exists silver.crm_cust_info cascade;
drop table if exists silver.crm_prd_info cascade;
drop table if exists silver.crm_sales_details  cascade;

-- Recreate crm_cust_info table
create table silver.crm_cust_info (
    cst_id INT primary key,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Recreate crm_prd_info table
create table silver.crm_prd_info (
	prd_id INT primary key,
	cat_id VARCHAR(50),
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	dwh_update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Recreate crm_sales_details table
create table silver.crm_sales_details (
	sls_ord_num VARCHAR(50) primary key,
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	dwh_update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- =================
-- ERP tables
-- =================

-- Drop tables if exist
drop table if exists silver.erp_loc_a101 cascade;
drop table if exists silver.erp_cust_az12 cascade;
drop table if exists silver.erp_px_cat_g1v2 cascade;

-- Recreate erp_loc_a101
create table silver.erp_loc_a101 (
	cid VARCHAR(50) primary key,
	cntry VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	dwh_update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Recreate erp_cust_az12
create table silver.erp_cust_az12 (
	cid VARCHAR(50) primary key,
	bdate DATE,
	gen VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	dwh_update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Recreate erp_px_cat_g1v2
create table silver.erp_px_cat_g1v2 (
	id VARCHAR(50) primary key,
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	dwh_update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
