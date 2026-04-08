import pandas as pd
import psycopg
import os
from pathlib import Path
from datetime import datetime
from time import perf_counter
from dotenv import load_dotenv

# ==============================
# CONFIG
# ==============================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / '.env')

DB_CONFIG = {
  'host': os.getenv('DB_HOST'),
  'port': int(os.getenv('DB_PORT', '5432')),
  'dbname': os.getenv('DB_NAME'),
  'user': os.getenv('DB_USER'),
  'password': os.getenv('DB_PASSWORD')
}

datasets_dir_env = os.getenv('DATASETS_DIR', 'datasets')
DATASETS_DIR = Path(datasets_dir_env)
if not DATASETS_DIR.is_absolute():
  DATASETS_DIR = PROJECT_ROOT / DATASETS_DIR

FILE_PATHS = {
  'crm_cust_info': DATASETS_DIR / 'source_crm' / 'cust_info.csv',
  'crm_prd_info': DATASETS_DIR / 'source_crm' / 'prd_info.csv',
  'crm_sales_details': DATASETS_DIR / 'source_crm' / 'sales_details.csv',
  'erp_cust_az12': DATASETS_DIR / 'source_erp' / 'CUST_AZ12.csv',
  'erp_loc_a101': DATASETS_DIR / 'source_erp' / 'LOC_A101.csv',
  'erp_px_cat_g1v2': DATASETS_DIR / 'source_erp' / 'PX_CAT_G1V2.csv'
}

# ==============================
# HELPER
# ==============================
def log(msg):
  print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] {msg}')

def get_connection():
  missing_vars = [
    key for key, value in DB_CONFIG.items()
    if value is None or value == ''
  ]
  if missing_vars:
    raise ValueError(f'Missing database config values: {", ".join(missing_vars)}')

  return psycopg.connect(**DB_CONFIG)

# ==============================
# LOAD FUNCTION
# ==============================
def load_table(table_name, file_path):
  full_table_name = f'bronze.{table_name}'

  try:
    log(f'Starting load for {full_table_name}')

    with get_connection() as conn:
      with conn.cursor() as cur:

        # Drop all primary keys
        cur.execute(f"""
          DO $$
          DECLARE constraint_name TEXT;
          BEGIN
            FOR constraint_name IN
              SELECT conname
              FROM pg_constraint c
              JOIN pg_class t ON t.oid = c.conrelid
              WHERE t.relname = '{table_name}' AND c.contype = 'p'
            LOOP
              EXECUTE format('ALTER TABLE {full_table_name} DROP CONSTRAINT %I', constraint_name);
            END LOOP;
          END $$;
        """)

        # Drop all NOT NULL constraints
        cur.execute(f"""
          DO $$
          DECLARE col RECORD;
          BEGIN
            FOR col IN
              SELECT column_name
              FROM information_schema.columns
              WHERE table_name = '{table_name}' AND is_nullable = 'NO'
            LOOP
              EXECUTE format('ALTER TABLE {full_table_name} ALTER COLUMN %I DROP NOT NULL', col.column_name);
            END LOOP;
          END $$;
        """)

        # Truncate table
        log(f'Truncating table {full_table_name}')
        cur.execute(f'TRUNCATE TABLE {full_table_name}')

        # Load data with COPY (omit ingestion_date column)
        log(f'Loading data into {full_table_name} from {file_path}')
        # Read header from CSV to get columns
        with open(file_path, "r") as f:
          header = f.readline().strip()
          csv_columns = [col.strip() for col in header.split(",")]
          # Compose COPY command with only CSV columns (exclude ingestion_date)
          columns_str = ', '.join(csv_columns)
          copy_sql = f'COPY {full_table_name} ({columns_str}) FROM STDIN WITH CSV HEADER'
        # Now COPY
        with open(file_path, "r") as f:
          with cur.copy(copy_sql) as copy:
            for line in f:
              copy.write(line)
        conn.commit()

    log(f'Finished loading {full_table_name}')  

  except Exception as e:
    log(f'Error loading {full_table_name}: {e}')
    raise

# ==============================
# MAIN
# ==============================
if __name__ == '__main__':
  start_time = perf_counter()

  for table_name, file_path in FILE_PATHS.items():
    load_table(table_name, file_path)

  elapsed_seconds = perf_counter() - start_time
  log(f'Total load time: {elapsed_seconds:.2f} seconds')