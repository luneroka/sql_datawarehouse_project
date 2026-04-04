-- =========================================
-- Title: Data Warehouse Initialization Script
-- =========================================
-- Purpose:
-- This script fully resets and initializes the 'data_warehouse' PostgreSQL database.
-- It drops the existing database (if any), recreates it from scratch, and sets up
-- the core schemas: bronze, silver, and gold.
--
-- Warning:
-- This script is DESTRUCTIVE.
-- Running it will permanently delete the existing 'data_warehouse' database
-- and all its data. Ensure you have backups if needed before execution.
--
-- Execution Notes:
-- - Must be run from a connection to a different database (e.g., 'postgres')
-- - The '\c data_warehouse' command only works in psql (not all SQL editors like DBeaver)
-- - In DBeaver, execute database creation and schema creation in separate steps
-- =========================================

-- Step 1: Create Database 'Data_Warehouse'
drop database if exists data_warehouse; 
create database data_warehouse;

-- Step 2: Connect to the new DB (manually)

-- Step 3: Create schemas
create schema if not exists  bronze;
create schema if not exists silver;
create schema if not exists gold;

-- Step 4 (optional): Verify
select schema_name 
from information_schema.schemata
where schema_name in ('bronze', 'silver', 'gold');
