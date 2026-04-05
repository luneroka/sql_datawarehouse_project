/*
===============================================================================
Title: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This script loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Drops constraints (PRIMARY KEY, NOT NULL) to allow raw ingestion
    - Truncates bronze tables before loading data
    - Uses PostgreSQL COPY command to bulk load CSV data into tables

Parameters:
    None.
    This script does not accept parameters or return values.

Execution Notes:
    - Must be run from a PostgreSQL client with access to local files
    - Ensure file paths are absolute and accessible by the PostgreSQL server
    - CSV files must match table column order or be explicitly mapped
    - Adjust file paths before execution

Usage Example:
    Run the script in DBeaver or psql while connected to 'data_warehouse'
===============================================================================
*/

-- =========================================
-- STEP 1: Drop constraints for raw ingestion
-- =========================================

-- DROP PRIMARY KEY
alter table bronze.crm_cust_info drop constraint if exists crm_cust_info_pkey;
alter table bronze.crm_prd_info drop constraint if exists crm_prd_info_pkey;
alter table bronze.crm_sales_details drop constraint if exists crm_sales_details_pkey;
alter table bronze.erp_cust_az12  drop constraint if exists erp_cust_az12_pkey;
alter table bronze.erp_loc_a101  drop constraint if exists erp_loc_a101_pkey;
alter table bronze.erp_px_cat_g1v2 drop constraint if exists erp_px_cat_g1v2_pkey;


-- Allow NULLs in cst_id
alter table bronze.crm_cust_info alter column cst_id drop not null;
alter table bronze.crm_prd_info alter column prd_id drop not null;
alter table bronze.crm_sales_details alter column sls_ord_num drop not null;
alter table bronze.erp_cust_az12 alter column cid drop not null;
alter table bronze.erp_loc_a101 alter column cid drop not null;
alter table bronze.erp_px_cat_g1v2 alter column id drop not null;

-- =========================================
-- STEP 2: Truncate tables
-- =========================================

truncate table bronze.crm_cust_info;
truncate table bronze.crm_prd_info;
truncate table bronze.crm_sales_details;
truncate table bronze.erp_cust_az12;
truncate table bronze.erp_loc_a101;
truncate table bronze.erp_px_cat_g1v2;

-- =========================================
-- STEP 3: Bulk load data from CSV
-- =========================================

-- In DBeaver : use import wizard or psql command

-- =========================================
-- STEP 4: Verification
-- =========================================

select count(*) as total_rows from bronze.crm_cust_info;
select count(*) as total_rows from bronze.crm_prd_info;
select count(*) as total_rows from bronze.crm_sales_details;
select count(*) as total_rows from bronze.erp_cust_az12;
select count(*) as total_rows from bronze.erp_loc_a101;
select count(*) as total_rows from bronze.erp_px_cat_g1v2;
