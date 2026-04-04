
/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/


-- =================
-- CRM tables
-- =================

-- Drop tables if exist
drop table if exists bronze.crm_cust_info cascade;
drop table if exists bronze.crm_prd_info cascade;
drop table if exists bronze.crm_sales_details  cascade;

-- Recreate crm_cust_info table
create table bronze.crm_cust_info (
    cst_id INT primary key,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE,
    ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Recreate crm_prd_info table
create table bronze.crm_prd_info (
	prd_id INT primary key,
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt TIMESTAMP,
	prd_end_dt TIMESTAMP,
	ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Recreate crm_sales_details table
create table bronze.crm_sales_details (
	sls_ord_num VARCHAR(50) primary key,
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- =================
-- ERP tables
-- =================

-- Drop tables if exist
drop table if exists bronze.erp_loc_a101 cascade;
drop table if exists bronze.erp_cust_az12 cascade;
drop table if exists bronze.erp_px_cat_g1v2 cascade;

-- Recreate erp_loc_a101
create table bronze.erp_loc_a101 (
	cid VARCHAR(50) primary key,
	cntry VARCHAR(50),
	ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Recreate erp_cust_az12
create table bronze.erp_cust_az12 (
	cid VARCHAR(50) primary key,
	bdate DATE,
	gen VARCHAR(50),
	ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Recreate erp_px_cat_g1v2
create table bronze.erp_px_cat_g1v2 (
	id VARCHAR(50) primary key,
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50),
	ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
