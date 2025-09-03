create database dev;

Create schema silver
-- =====================================================
-- SILVER LAYER TABLE: loan_contracts_silver
-- =====================================================

CREATE OR REPLACE TABLE  dev.silver.loan_contracts_silver (
    loan_id VARCHAR(50),
    customer_name VARCHAR(100),
    loan_amount NUMBER(15,2),
    interest_rate NUMBER(5,2),
    start_date DATE,
    status VARCHAR(20),
    -- Derived/calculated fields
    monthly_payment NUMBER(15,2),
    loan_term_months NUMBER(3),
    total_interest NUMBER(15,2),
    risk_category VARCHAR(20),
    -- Metadata fields
    as_of_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- PARAMETERIZED STORED PROCEDURE: Process Loan Contracts Silver Layer
-- =====================================================

-- =====================================================
-- SIMPLIFIED PARAMETERIZED STORED PROCEDURE
-- =====================================================

CREATE OR REPLACE PROCEDURE dev.silver.sp_process_loan_contracts_silver(
    as_of_date DATE,
    target_database STRING,
    raw_schema STRING,
    silver_schema STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    raw_table_name STRING;
    silver_table_name STRING;
    result_msg STRING;
BEGIN
    -- Build dynamic table names
    raw_table_name := target_database || '.' || raw_schema || '.loan_raw';
    silver_table_name := target_database || '.' || silver_schema || '.loan_contracts_silver';
    
    -- Delete existing data for the as_of_date to ensure idempotency
    EXECUTE IMMEDIATE 'DELETE FROM ' || silver_table_name || ' WHERE as_of_date = ''' || as_of_date || '''';
    
    -- Insert processed data from raw layer
    EXECUTE IMMEDIATE '
    INSERT INTO ' || silver_table_name || ' (
        loan_id, customer_name, loan_amount, interest_rate, start_date, status,
        monthly_payment, loan_term_months, total_interest, risk_category, as_of_date
    )
    SELECT 
        raw_record:"loan_id"::VARCHAR(50) as loan_id,
        raw_record:"customer_name"::VARCHAR(100) as customer_name,
        raw_record:"loan_amount"::NUMBER(15,2) as loan_amount,
        raw_record:"interest_rate"::NUMBER(5,2) as interest_rate,
        raw_record:"start_date"::DATE as start_date,
        raw_record:"status"::VARCHAR(20) as status,
        
        ROUND(
            (raw_record:"loan_amount"::NUMBER * (raw_record:"interest_rate"::NUMBER/100/12)) / 
            (1 - POWER(1 + (raw_record:"interest_rate"::NUMBER/100/12), -360)), 2
        ) as monthly_payment,
        
        360 as loan_term_months,
        
        ROUND(
            ((raw_record:"loan_amount"::NUMBER * (raw_record:"interest_rate"::NUMBER/100/12)) / 
            (1 - POWER(1 + (raw_record:"interest_rate"::NUMBER/100/12), -360))) * 360 
            - raw_record:"loan_amount"::NUMBER, 2
        ) as total_interest,
        
        CASE 
            WHEN raw_record:"interest_rate"::NUMBER >= 9.0 THEN ''HIGH_RISK''
            WHEN raw_record:"interest_rate"::NUMBER >= 7.5 THEN ''MEDIUM_RISK''
            ELSE ''LOW_RISK''
        END as risk_category,
        
        ''' || as_of_date || ''' as as_of_date
        
    FROM ' || raw_table_name || '
    WHERE raw_record IS NOT NULL AND raw_record:"loan_id" IS NOT NULL';
    
    -- Update the updated_at timestamp
    EXECUTE IMMEDIATE 'UPDATE ' || silver_table_name || ' SET updated_at = CURRENT_TIMESTAMP() WHERE as_of_date = ''' || as_of_date || '''';
    
    -- Build success message
    result_msg := 'SUCCESS: Processed loan contracts for date ' || as_of_date ||
           ' from ' || raw_table_name || ' to ' || silver_table_name ||
           '. Processing completed in ' || DATEDIFF(millisecond, start_time, CURRENT_TIMESTAMP()) || 'ms';
    
    RETURN result_msg;
           
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;