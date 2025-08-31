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

## Project Structure

```
snowflake-data-tools/
├── data/              # Your CSV files
├── ingestion/         # Data ingestion scripts
│   ├── __init__.py   # Package marker
│   └── ingest.py     # Main ingestion script
├── requirements.txt   # Python packages
├── .env              # Snowflake credentials
└── README.md         # This file
```

## Configuration

Update `.env` with your Snowflake connection details.

## Extending

This repo is designed to grow with additional Snowflake utilities and data processing tools.