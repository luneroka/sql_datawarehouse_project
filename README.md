# SQL Data Warehouse Project

> A comprehensive data warehouse implementation built with PostgreSQL, demonstrating modern ETL processes, multi-layer data architecture, and analytics-ready data models.
>
> **Part of the [Data With Baraa SQL Course](https://www.datawithbaraa.com/wiki/sql#sql-welcome)**

## 📋 Overview

This project implements a **medallion architecture** data warehouse that ingests raw data from multiple source systems (CRM and ERP), transforms it through three distinct layers, and delivers clean, business-aligned analytics tables ready for reporting and insights.

### Architecture Layers

The data warehouse follows the medallion (lambda) architecture pattern with three progressive layers:

#### 🥉 Bronze Layer

**Purpose:** Raw data ingestion layer  
**Characteristics:**

- Exact copies of raw source data with minimal transformations
- Direct loads from CRM and ERP systems
- Includes technical metadata (ingestion timestamps)
- Serves as the single source of truth for data lineage

**Tables:**

- `bronze.crm_cust_info` - Customer information from CRM
- `bronze.crm_prd_info` - Product information from CRM
- `bronze.crm_sales_details` - Sales transactions from CRM
- `bronze.erp_cust_az12` - Customer data from ERP
- `bronze.erp_loc_a101` - Location data from ERP
- `bronze.erp_px_cat_g1v2` - Product categories from ERP

#### 🥈 Silver Layer

**Purpose:** Cleaned and standardized intermediate layer  
**Characteristics:**

- Deduplication, validation, and standardization of bronze data
- Business rule application
- Data quality checks and transformations
- Maintains historical context with DWH metadata columns

**Tables:**

- `silver.crm_cust_info` - Cleaned customer records
- `silver.crm_prd_info` - Standardized product data
- `silver.crm_sales_details` - Validated sales transactions
- `silver.erp_cust_az12` - Deduplicated ERP customer records
- `silver.erp_loc_a101` - Standardized location information
- `silver.erp_px_cat_g1v2` - Consistent product categories

#### 🥇 Gold Layer

**Purpose:** Analytics-ready business layer  
**Characteristics:**

- Dimension and fact tables optimized for analytics and reporting
- Business-friendly naming and structures
- Surrogate keys for referential integrity and performance
- Ready for BI tools and analytical queries

**Dimensional Model:**

- `gold.dim_customers` - Customer dimension with enriched attributes
- `gold.dim_products` - Product dimension with categorization
- `gold.dim_dates` - Date dimension (optional for time-series analysis)
- `gold.fact_sales` - Sales fact table with transactional metrics

---

## 📁 Project Structure

```
sql_datawarehouse_project/
├── README.md                          # This file
├── LICENSE                            # MIT License
│
├── datasets/                          # Source data files
│   ├── source_crm/                   # CRM system exports
│   │   ├── cust_info.csv            # Customer master data
│   │   ├── prd_info.csv             # Product catalog
│   │   └── sales_details.csv        # Sales transactions
│   │
│   └── source_erp/                   # ERP system exports
│       ├── CUST_AZ12.csv            # ERP customer records
│       ├── LOC_A101.csv             # Location/geography data
│       └── PX_CAT_G1V2.csv          # Product categories
│
├── scripts/                           # ETL and DDL scripts
│   ├── init_database.sql             # Database initialization
│   │
│   ├── bronze/                       # Bronze layer scripts
│   │   ├── ddl_bronze.sql           # Create bronze tables
│   │   ├── load_bronze.py           # Load raw data (Python)
│   │   └── proc_load_bronze.sql     # Alternative load procedure
│   │
│   ├── silver/                       # Silver layer scripts
│   │   ├── ddl_silver.sql           # Create silver tables
│   │   ├── proc_load_silver.sql     # Transform bronze → silver
│   │   └── check_silver.sql         # Data quality validation
│   │
│   └── gold/                        # Gold layer scripts
│       ├── ddl_gold.sql             # Create gold dimension/fact tables
│       └── check_gold.sql           # Analytical checks and validations
│
└── docs/                              # Documentation
    └── data_catalog.md               # Complete data dictionary
```

---

## 🚀 Getting Started

### Prerequisites

- **PostgreSQL 12+** - For database engine
- **Python 3.8+** - For ETL scripts
- **psycopg2** or **psycopg** - Python PostgreSQL driver

### Installation & Setup

1. **Clone or download the project**

   ```bash
   cd sql_datawarehouse_project
   ```

2. **Create Python virtual environment** (recommended)

   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Python dependencies**

   ```bash
   pip install pandas psycopg python-dotenv
   ```

4. **Initialize the database**
   - Open your PostgreSQL client (psql, DBeaver, pgAdmin, etc.)
   - Connect to the default `postgres` database
   - Run `scripts/init_database.sql` to create schemas and database structure

   ```sql
   -- From your PostgreSQL client
   \i /path/to/scripts/init_database.sql
   ```

5. **Create bronze tables**

   ```sql
   \i /path/to/scripts/bronze/ddl_bronze.sql
   ```

6. **Load source data into bronze layer**

   ```bash
   python scripts/bronze/load_bronze.py
   ```

7. **Create silver tables and transform data**

   ```sql
   \i /path/to/scripts/silver/ddl_silver.sql
   \i /path/to/scripts/silver/proc_load_silver.sql
   ```

8. **Create gold tables and load dimensional model**
   ```sql
   \i /path/to/scripts/gold/ddl_gold.sql
   ```

---

## 📊 Data Flow

```
Source Systems (CRM + ERP)
           ↓
    [CSV Files]
           ↓
    BRONZE Layer ────→ Raw data landing zone
    (load_bronze.py)
           ↓
    SILVER Layer ────→ Cleansed & standardized data
    (proc_load_silver.sql)
           ↓
    GOLD Layer ─────→ Analytics-ready star schema
    (ddl_gold.sql)
           ↓
    BI Tools & Analytics
```

---

## 🔄 ETL Processes

### Bronze → Silver Transformation

- **Deduplication:** Remove duplicate records from source systems
- **Data Cleaning:** Standardize formats, handle NULL values
- **Validation:** Check business rules and data constraints
- **Enrichment:** Add derived attributes and technical metadata
- **Run:** `scripts/silver/proc_load_silver.sql`

### Silver → Gold Transformation

- **Dimensional Modeling:** Create dimension and fact tables
- **Surrogate Keys:** Add system-generated primary keys
- **Denormalization:** Combine related data for analytical queries
- **Aggregation:** Pre-compute common metrics where beneficial
- **Run:** `scripts/gold/ddl_gold.sql`

---

## 📖 Data Dictionary & Documentation

Detailed documentation for all tables, columns, data types, and business meanings is available in:

- **[docs/data_catalog.md](docs/data_catalog.md)** - Complete data catalog with table definitions

---

## 🔍 Data Quality & Validation

Quality checks are embedded throughout the ETL pipeline:

- **Bronze validation:** `scripts/bronze/` - Ingestion logging
- **Silver validation:** `scripts/silver/check_silver.sql` - Consistency checks
- **Gold validation:** `scripts/gold/check_gold.sql` - Dimensional model verification

---

## 🏗️ Technology Stack

| Component       | Technology               |
| --------------- | ------------------------ |
| **Database**    | PostgreSQL 12+           |
| **ETL**         | Python (Pandas, psycopg) |
| **DDL/DML**     | SQL (PL/pgSQL)           |
| **Data Format** | CSV                      |
| **License**     | MIT                      |

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🎓 About This Project

This project is part of the **Data With Baraa SQL Course**, a comprehensive program for learning modern data warehouse design and SQL best practices.

- **Course Link:** [Data With Baraa - SQL Course](https://www.datawithbaraa.com/wiki/sql#sql-welcome)
- **Topics Covered:** ETL Design, Data Modeling, Dimensional Modeling, SQL Optimization, Data Quality

---

## 📧 Contact & Support

For questions or support regarding this project, visit the Data With Baraa course resources or community forums.
