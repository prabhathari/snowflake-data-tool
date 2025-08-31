# Snowflake Data Tools

Simple Python utilities for Snowflake data operations.

## Quick Start

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Snowflake connection**
   - Update `.env` with your Snowflake credentials

3. **Add your data**
   - Place CSV files in `data/` folder

4. **Run ingestion**
   ```bash
   python ingestion/ingest.py
   ```

## What This Does

Ingests CSV files into Snowflake as raw tables with VARIANT data type for flexible processing.

Auto-detects CSV columns and creates structured JSON objects in Snowflake.

## Project Structure

```
snowflake-data-tools/
├── data/              # Your CSV files
├── metadata/          # Table metadata configurations (optional)
├── ingestion/         # Data ingestion scripts
│   ├── __init__.py   # Package marker
│   └── ingest.py     # Main ingestion script
├── requirements.txt   # Python packages
├── .env              # Snowflake credentials
└── README.md         # This file
```

## Configuration

Update `.env` with your Snowflake connection details:
- SNOWFLAKE_USER
- SNOWFLAKE_PASSWORD  
- SNOWFLAKE_ACCOUNT
- SNOWFLAKE_WAREHOUSE
- SNOWFLAKE_DATABASE
- SNOWFLAKE_SCHEMA

## Example Output

Your CSV data will be stored as structured VARIANT objects:
```json
{
  "loan_id": "1001",
  "customer_name": "John Doe",
  "loan_amount": "25000",
  "status": "Active"
}
```

## Extending

This repo is designed to grow with additional Snowflake utilities and data processing tools.