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

-- Allow NULLs in cst_id
alter table bronze.crm_cust_info alter column cst_id drop not null;


-- =========================================
-- STEP 2: Truncate tables
-- =========================================

truncate table bronze.crm_cust_info;


-- =========================================
-- STEP 3: Bulk load data from CSV
-- =========================================

-- In DBeaver : use import wizard or psql command

-- =========================================
-- STEP 4: Verification
-- =========================================

select count(*) as total_rows
from bronze.crm_cust_info;
