import os
import json
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

def ingest_file_simple(subject: str, file_path: str):
    """Simple ingestion - auto-detects CSV columns"""
    print(f"🔍 Reading CSV file: {file_path}")
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return
    
    try:
        # Read CSV to get column names
        df = pd.read_csv(file_path, nrows=0)  # Just headers
        columns = df.columns.tolist()
        print(f"📊 Detected columns: {columns}")
        
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return
    
    table_name = f"{subject}_raw"
    stage_name = f"@%{table_name}"
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        print(f"🏗️  Creating table: {table_name}")
        
        # 1. Create raw table
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                raw_record VARIANT,
                load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        print(f"📤 Uploading file to stage...")
        
        # 2. Put file into stage
        cur.execute(f"PUT file://{file_path} {stage_name} OVERWRITE=TRUE")
        
        print(f"🔄 Building object structure...")
        
        # 3. Build OBJECT_CONSTRUCT dynamically
        object_parts = []
        for i, col in enumerate(columns, 1):
            object_parts.append(f"'{col}', ${i}")
        
        object_construct = f"OBJECT_CONSTRUCT({', '.join(object_parts)})"
        print(f"🔧 Object construct: {object_construct}")
        
        # 4. Copy into table
        copy_sql = f"""
            COPY INTO {table_name}(raw_record)
            FROM (
                SELECT {object_construct}
                FROM {stage_name}
            )
            FILE_FORMAT=(FORMAT_NAME=my_csv_format)
        """
        
        print(f"💾 Executing COPY INTO...")
        cur.execute(copy_sql)
        
        print(f"✅ Successfully ingested {file_path} into {table_name}")
        
    except Exception as e:
        print(f"❌ Error during ingestion: {str(e)}")
        print(f"🔍 Error type: {type(e)}")
        
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    print("🚀 Starting Snowflake ingestion...")
    
    # Setup file format first
    setup_file_format()
    
    # Ingest loan data
    ingest_file_simple("loan", "./data/loan.csv")
    
    print("✨ Ingestion complete!")