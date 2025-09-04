# Snowflake Data Tools

Enhanced Python utilities for Snowflake data operations with multi-layer data processing.

## Quick Start

### 1. **Install dependencies**
```bash
pip install -r requirements.txt
```

### 2. **Configure Snowflake connection**
- Update `.env` with your Snowflake credentials:
```env
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
```

### 3. **Configure environments**
- Update `config.yaml` with your database and schema configurations

### 4. **Add your data**
- Place CSV files in `data/` folder

### 5. **Run data pipeline**
```bash
# Step 1: Ingest raw data
python ingestion/ingest.py

# Step 2: Process silver layer
python silver/process_silver.py loan_contracts sp_process_loan_contracts_silver
```

## What This Does

### **Raw Layer (Bronze)**
- Ingests CSV files into Snowflake as raw tables with VARIANT data type
- Auto-detects CSV columns and creates structured JSON objects
- Stores data in `{database}.{raw_schema}.{table_name}_raw` format

### **Silver Layer** 
- Transforms raw VARIANT data into structured, analytics-ready tables
- Applies business logic and calculations
- Supports environment-specific processing via parameterized stored procedures
- Stores processed data in `{database}.{silver_schema}.{table_name}_silver` format

## Project Structure

```
snowflake-data-tools/
├── data/                    # Your CSV files
│   └── loan.csv            # Sample loan data
├── metadata/               # Table metadata configurations (optional)
│   └── loan_metadata.json
├── ingestion/              # Raw data ingestion (Bronze layer)
│   ├── __init__.py
│   └── ingest.py          # Main ingestion script
├── silver_layer/                 # Silver layer processing
│   ├── __init__.py
│   └── process_silver.py  # Generic silver layer executor
├── sql/                    # SQL DDL scripts (manual execution)
│   └── silver_schemas.sql # Table and stored procedure definitions
├── config.yaml            # Environment and table configurations
├── requirements.txt       # Python dependencies
├── .env                   # Snowflake credentials (create this)
└── README.md              # This file
```

## Configuration Files

### **config.yaml** - Environment Configuration
```yaml
snowflake:
  database: "DEV"
  warehouse: "COMPUTE_WH"
  schemas:
    raw: "RAW"
    silver: "SILVER" 
    gold: "GOLD"
    
environments:
  dev:
    database: "DEV"
    schemas:
      raw: "RAW"
      silver: "SILVER"
  prod:
    database: "PROD"
    schemas:
      raw: "PROD_RAW"
      silver: "PROD_SILVER"
```

### **.env** - Snowflake Credentials
```env
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account.region.cloud
```

## Silver Layer Setup

### **1. Manual SQL Execution (One-time setup)**

Execute the SQL commands from `sql/silver_schemas.sql` in Snowflake:

```sql
-- Create silver table
CREATE OR REPLACE TABLE dev.silver.loan_contracts_silver (
    loan_id VARCHAR(50),
    customer_name VARCHAR(100),
    loan_amount NUMBER(15,2),
    interest_rate NUMBER(5,2),
    start_date DATE,
    status VARCHAR(20),
    monthly_payment NUMBER(15,2),      -- CALCULATED
    loan_term_months NUMBER(3),        -- DERIVED
    total_interest NUMBER(15,2),       -- CALCULATED  
    risk_category VARCHAR(20),         -- DERIVED
    as_of_date DATE,                   -- PROCESSING DATE
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create parameterized stored procedure
CREATE OR REPLACE PROCEDURE dev.silver.sp_process_loan_contracts_silver(
    as_of_date DATE,
    target_database STRING,
    raw_schema STRING,
    silver_schema STRING
)
-- ... (see sql/silver_schemas.sql for full definition)
```

### **2. Python Execution**

```bash
# Basic processing (uses config.yaml defaults)
python silver/process_silver.py loan_contracts sp_process_loan_contracts_silver

# With specific date
python silver/process_silver.py loan_contracts sp_process_loan_contracts_silver --date 2024-12-01

# Different environment
python silver/process_silver.py loan_contracts sp_process_loan_contracts_silver --env prod

# Full example
python silver/process_silver.py loan_contracts sp_process_loan_contracts_silver --date 2024-12-01 --env staging
```

## Silver Layer Transformations

### **Business Logic Applied:**
1. **Extracts** structured data from VARIANT raw records
2. **Calculates** monthly payment amounts (30-year loan assumption)
3. **Derives** total interest over loan term
4. **Categorizes** risk levels based on interest rates:
   - HIGH_RISK: ≥9.0%
   - MEDIUM_RISK: 7.5-8.9%
   - LOW_RISK: <7.5%
5. **Adds** processing metadata (as_of_date, timestamps)

### **Key Features:**
- **Environment-agnostic**: Same code works across dev/staging/prod
- **Parameterized**: Database and schema names passed dynamically
- **Idempotent**: Can be run multiple times safely for same date
- **Auditable**: Tracks processing timestamps and performance metrics

## Usage Examples

### **Raw Data Ingestion**
```bash
python ingestion/ingest.py
```
**Output**: Raw JSON data in `dev.raw.loan_raw`

### **Silver Layer Processing**
```bash
python silver/process_silver.py loan_contracts sp_process_loan_contracts_silver --env dev
```
**Output**: Structured, calculated data in `dev.silver.loan_contracts_silver`

### **Cross-Environment Processing**
```bash
# Development
python silver/process_silver.py loan_contracts sp_process_loan_contracts_silver --env dev

# Production  
python silver/process_silver.py loan_contracts sp_process_loan_contracts_silver --env prod
```

## Sample Data Pipeline Output

**Input (Raw Layer):**
```json
{ "customer_name": "John Doe", "interest_rate": "7.5", "loan_amount": "25000", "loan_id": "1001", "start_date": "2023-01-15", "status": "Active" }
```

**Output (Silver Layer):**
```sql
loan_id: 1001
customer_name: John Doe
loan_amount: 25000.00
interest_rate: 7.5
start_date: 2023-01-15
status: Active
monthly_payment: 174.83      -- CALCULATED
loan_term_months: 360        -- STANDARD ASSUMPTION
total_interest: 37938.80     -- CALCULATED
risk_category: MEDIUM_RISK   -- DERIVED FROM RATE
as_of_date: 2025-09-03      -- PROCESSING DATE
```

## Sample Analytics Queries

After processing, run analytics on silver layer data:

```sql
-- Risk distribution analysis
SELECT 
    risk_category, 
    COUNT(*) as loan_count,
    AVG(loan_amount) as avg_loan_amount,
    AVG(monthly_payment) as avg_monthly_payment
FROM dev.silver.loan_contracts_silver 
WHERE as_of_date = '2025-09-03'
GROUP BY risk_category;

-- Monthly origination trends
SELECT 
    DATE_TRUNC('month', start_date) as origination_month,
    COUNT(*) as loans_originated,
    SUM(loan_amount) as total_loan_amount,
    AVG(interest_rate) as avg_interest_rate
FROM dev.silver.loan_contracts_silver
WHERE as_of_date = '2025-09-03'
GROUP BY origination_month
ORDER BY origination_month;

-- Portfolio status summary
SELECT 
    status,
    COUNT(*) as loan_count,
    SUM(loan_amount) as total_outstanding,
    SUM(total_interest) as projected_interest,
    AVG(risk_category) as avg_risk_profile
FROM dev.silver.loan_contracts_silver
WHERE as_of_date = '2025-09-03'
GROUP BY status;
```

## Adding New Tables

To add processing for new data sources (e.g., customers, transactions):

### **1. Add to config.yaml**
```yaml
# Not needed - the current setup is table-agnostic
```

### **2. Create SQL objects**
```sql
-- Create new silver table
CREATE TABLE dev.silver.new_table_silver (...);

-- Create new stored procedure  
CREATE PROCEDURE dev.silver.sp_process_new_table_silver(...);
```

### **3. Execute processing**
```bash
python silver/process_silver.py new_table sp_process_new_table_silver --env dev
```

## Architecture Benefits

✅ **Environment Portability**: Same codebase across all environments  
✅ **Parameterized Processing**: Dynamic schema and database resolution  
✅ **Separation of Concerns**: DDL (manual) vs DML (automated)  
✅ **Configuration-Driven**: Easy environment management via YAML  
✅ **Auditable Processing**: Timestamped, idempotent operations  
✅ **Scalable Design**: Easy to add new tables and transformations  

## Troubleshooting

### **Common Issues:**

**1. Table/Procedure Not Found**
- Ensure SQL objects are created in correct schema
- Verify config.yaml database/schema names match your Snowflake setup

**2. Permission Errors**  
- Check Snowflake user has access to source and target schemas
- Verify warehouse usage permissions

**3. Data Type Errors**
- Review raw data format in source CSV
- Check VARIANT field extraction syntax in stored procedure

**4. Environment Configuration**
- Verify `.env` file exists and has correct credentials
- Check `config.yaml` environment sections match your setup

This system provides a robust, enterprise-ready data processing pipeline with clear separation between raw ingestion and analytical transformations.