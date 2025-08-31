import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Snowflake connection details
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

def get_connection():
    """Create and return Snowflake connection"""
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

def setup_file_format():
    """Create CSV file format if not exists"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE OR REPLACE FILE FORMAT my_csv_format
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY='"'
            SKIP_HEADER=1
        """)
        print("✅ File format created successfully")
    finally:
        cur.close()
        conn.close()

def ingest_file(subject: str, file_path: str):
    """Ingest CSV file into Snowflake raw table"""
    table_name = f"{subject}_raw"
    stage_name = f"@%{table_name}"  # table stage
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # 1. Create raw table if not exists
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                raw_record VARIANT,
                load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 2. Put file into Snowflake stage
        cur.execute(f"PUT file://{file_path} {stage_name} OVERWRITE=TRUE")
        
        # 3. Copy into raw table (auto parses CSV → VARIANT)
        cur.execute(f"""
            COPY INTO {table_name}(raw_record)
            FROM {stage_name}
            FILE_FORMAT=(FORMAT_NAME=my_csv_format)
        """)
        
        print(f"Successfully ingested {file_path} into {table_name}")
        
    except Exception as e:
        print(f"Error ingesting {file_path}: {str(e)}")
        
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    # Setup file format first
    setup_file_format()
    
    # Example usage
    ingest_file("loan", "./data/loan.csv")