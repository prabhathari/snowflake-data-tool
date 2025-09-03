import os
import sys
import argparse
import yaml
import snowflake.connector
from datetime import datetime, date
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def load_config(config_file='config.yaml', environment=None):
    """Load configuration from YAML file"""
    try:
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
        
        # Apply environment-specific overrides if specified
        if environment and environment in config.get('environments', {}):
            env_config = config['environments'][environment]
            if 'database' in env_config:
                config['snowflake']['database'] = env_config['database']
            if 'schemas' in env_config:
                config['snowflake']['schemas'].update(env_config['schemas'])
        
        return config
    except FileNotFoundError:
        print(f"❌ Config file {config_file} not found!")
        sys.exit(1)

def get_connection(config):
    """Create and return Snowflake connection"""
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=config['snowflake']['warehouse'],
        database=config['snowflake']['database'],
        schema=config['snowflake']['schemas']['silver']
    )

def execute_silver_procedure(config, table_name, procedure_name, as_of_date=None):
    """Execute silver layer stored procedure"""
    
    # Use today's date if no date provided
    if as_of_date is None:
        as_of_date = date.today()
    elif isinstance(as_of_date, str):
        as_of_date = datetime.strptime(as_of_date, '%Y-%m-%d').date()
    
    # Get database and schemas from config
    database = config['snowflake']['database']
    raw_schema = config['snowflake']['schemas']['raw']
    silver_schema = config['snowflake']['schemas']['silver']
    
    print(f"🔄 Processing {table_name}...")
    print(f"📋 Procedure: {procedure_name}")
    print(f"🏢 Database: {database}")
    print(f"📂 Raw Schema: {raw_schema}")
    print(f"📂 Silver Schema: {silver_schema}")
    print(f"📅 As of date: {as_of_date}")
    
    conn = get_connection(config)
    cur = conn.cursor()
    
    try:
        # Call the stored procedure with parameters
        call_sql = f"CALL {database}.{silver_schema}.{procedure_name}('{as_of_date}', '{database}', '{raw_schema}', '{silver_schema}')"
        print(f"🚀 Executing: {call_sql}")
        
        cur.execute(call_sql)
        result = cur.fetchone()
        
        print(f"✅ Success!")
        print(f"📊 Result: {result[0] if result else 'No result returned'}")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise
        
    finally:
        cur.close()
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='Execute Silver Layer Processing')
    parser.add_argument('table_name', help='Table name (e.g., loan_contracts)')
    parser.add_argument('procedure_name', help='Stored procedure name (e.g., sp_process_loan_contracts_silver)')
    parser.add_argument('--date', default=None, help='As of date (YYYY-MM-DD)')
    parser.add_argument('--env', default=None, help='Environment (dev/staging/prod)')
    
    args = parser.parse_args()
    
    print("🚀 Starting Silver Layer Processing...")
    
    # Load config
    config = load_config(environment=args.env)
    
    # Execute processing
    execute_silver_procedure(
        config=config,
        table_name=args.table_name,
        procedure_name=args.procedure_name,
        as_of_date=args.date
    )
    
    print("✨ Processing complete!")

if __name__ == "__main__":
    main()