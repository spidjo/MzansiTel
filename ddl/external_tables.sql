/*
====================================================================================================
Script: Create External Tables for MzansiTel Data Loading
Purpose: Dynamically creates external tables for loading CDR, subscriber, tariff plan, 
         and subscriber plan data from CSV files

Description:
This PL/SQL script creates four external tables that map to CSV files containing:
1. Call Detail Records (CDR)
2. Subscriber information
3. Tariff plan details
4. Subscriber-plan mappings

Key Features:
- Generates dynamic filenames with current date (YYYYMMDD format)
- Uses Oracle external tables for direct CSV file access
- Handles proper date formatting for all date fields
- Includes comprehensive error handling
- Skips header rows in CSV files
====================================================================================================
*/

DECLARE
    -- Generate dynamic filenames with current date to ensure uniqueness
    v_cdr_location VARCHAR2(100) := 'cdr_data_' || TO_CHAR(sysdate, 'YYYYMMDD') || '.csv';
    v_subscriber_location VARCHAR2(100) := 'subscriber_data_' || TO_CHAR(sysdate, 'YYYYMMDD') || '.csv';
    v_tariff_plan_location VARCHAR2(100) := 'tariff_plan_data_' || TO_CHAR(sysdate, 'YYYYMMDD') || '.csv';
    v_subscriber_plan_location VARCHAR2(100) := 'subscriber_plan_data_' || TO_CHAR(sysdate, 'YYYYMMDD') || '.csv';
    
    -- Variable to hold dynamic SQL statements
    v_sql VARCHAR2(4000);
BEGIN
    /*
    ====================================================================
    Create External Table for Call Detail Records (CDR)
    Purpose: Maps to CSV file containing call/sms/data usage records
    File Format: Comma-delimited with optional quoting, header row
    Special Handling: Timestamp fields with precise format
    ====================================================================
    */
    v_sql := 'CREATE TABLE external_cdr_file (
        subscriber_msisdn VARCHAR2(20) NOT NULL,       -- Mobile number of calling subscriber
        call_type VARCHAR2(20),                        -- Type: VOICE/SMS/DATA
        call_start_time DATE,                          -- When call/SMS/data session started
        call_end_time DATE,                            -- When call/SMS/data session ended
        call_duration_sec NUMBER,                      -- Duration in seconds
        destination_number VARCHAR2(20),               -- Called/SMSed number
        call_cost NUMBER,                              -- Cost of the call/SMS/data
        call_direction VARCHAR2(10),                   -- INBOUND/OUTBOUND
        source_file_name VARCHAR2(255)                 -- Original source filename
    )
    ORGANIZATION EXTERNAL (
        TYPE ORACLE_LOADER                            -- Use Oracle loader driver
        DEFAULT DIRECTORY DATA_DIR                    -- Oracle directory object pointing to file location
        ACCESS PARAMETERS (
            RECORDS DELIMITED BY NEWLINE              -- Each line is a record
            SKIP 1                                    -- Skip header row in CSV
            FIELDS TERMINATED BY '','' OPTIONALLY ENCLOSED BY ''"''  -- CSV format
            MISSING FIELD VALUES ARE NULL             -- Treat empty fields as NULL
            (
                subscriber_msisdn,
                call_type,
                call_start_time CHAR(19) DATE_FORMAT DATE MASK ''YYYY-MM-DD HH24:MI:SS'',
                call_end_time CHAR(19) DATE_FORMAT DATE MASK ''YYYY-MM-DD HH24:MI:SS'',
                call_duration_sec,
                destination_number,
                call_cost,
                call_direction,
                source_file_name CHAR(255) 
            )
        )
        LOCATION (''' || v_cdr_location || ''')       -- Dynamic filename with date
    )
    REJECT LIMIT UNLIMITED';                          -- Allow all rows to be rejected if needed

    EXECUTE IMMEDIATE v_sql;
    DBMS_OUTPUT.PUT_LINE('External table for CDR data created successfully.');

    /*
    ====================================================================
    Create External Table for Subscriber Data
    Purpose: Maps to CSV file containing customer/subscriber information
    File Format: Comma-delimited with optional quoting, header row
    Special Handling: Date fields with standard format
    ====================================================================
    */
    v_sql := 'CREATE TABLE external_subscriber_file (
        msisdn VARCHAR2(20) NOT NULL,                 -- Mobile number (unique identifier)
        first_name VARCHAR2(100),                     -- Customer first name
        last_name VARCHAR2(100),                      -- Customer last name
        date_of_birth DATE,                           -- Customer date of birth
        email_address VARCHAR2(100),                  -- Customer email
        registration_date DATE,                       -- When customer registered
        status VARCHAR2(20),                          -- Account status (ACTIVE/INACTIVE/etc)
        source_file_name VARCHAR2(255)                -- Original source filename
    )
    ORGANIZATION EXTERNAL (
        TYPE ORACLE_LOADER
        DEFAULT DIRECTORY DATA_DIR
        ACCESS PARAMETERS (
            RECORDS DELIMITED BY NEWLINE
            SKIP 1                                    -- Skip header row
            FIELDS TERMINATED BY '','' OPTIONALLY ENCLOSED BY ''"''
            MISSING FIELD VALUES ARE NULL
            (
                msisdn,
                first_name,
                last_name,
                date_of_birth CHAR(10) DATE_FORMAT DATE MASK ''YYYY-MM-DD'',
                email_address,
                registration_date CHAR(10) DATE_FORMAT DATE MASK ''YYYY-MM-DD'',
                status,
                source_file_name CHAR(255) 
            )
        )
        LOCATION (''' || v_subscriber_location || ''')
    )
    REJECT LIMIT UNLIMITED';

    EXECUTE IMMEDIATE v_sql;
    DBMS_OUTPUT.PUT_LINE('External table for subscriber data created successfully.');

    /*
    ====================================================================
    Create External Table for Tariff Plan Data
    Purpose: Maps to CSV file containing service plan details
    File Format: Comma-delimited with optional quoting, header row
    Special Handling: Date fields for plan validity period
    ====================================================================
    */
    v_sql := 'CREATE TABLE external_tariff_plan_file (
        plan_id VARCHAR2(20) NOT NULL,                -- Unique plan identifier
        plan_name VARCHAR2(100),                      -- Human-readable plan name
        description VARCHAR2(255),                    -- Detailed plan description
        monthly_fee NUMBER(10,2),                     -- Base monthly charge
        call_rate_per_minute NUMBER(10,2),            -- Cost per minute for calls
        sms_rate_per_message NUMBER(10,2),            -- Cost per SMS
        data_rate_per_mb NUMBER(10,2),                -- Cost per MB of data
        data_limit_mb NUMBER(10,2),                   -- Included monthly data (MB)
        voice_limit_minutes NUMBER(10,2),             -- Included monthly voice minutes
        sms_limit NUMBER(10,2),                       -- Included monthly SMS
        valid_from DATE,                              -- Plan effective start date
        valid_to DATE,                                -- Plan end date (NULL for ongoing)
        source_file_name VARCHAR2(255)                -- Original source filename
    )
    ORGANIZATION EXTERNAL (
        TYPE ORACLE_LOADER
        DEFAULT DIRECTORY DATA_DIR
        ACCESS PARAMETERS (
            RECORDS DELIMITED BY NEWLINE
            SKIP 1                                    -- Skip header row
            FIELDS TERMINATED BY '','' OPTIONALLY ENCLOSED BY ''"''
            MISSING FIELD VALUES ARE NULL
            (
                plan_id,
                plan_name,
                description,
                monthly_fee,
                call_rate_per_minute,
                sms_rate_per_message,
                data_rate_per_mb,
                data_limit_mb,
                voice_limit_minutes,
                sms_limit,
                valid_from CHAR(10) DATE_FORMAT DATE MASK ''YYYY-MM-DD'',
                valid_to CHAR(10) DATE_FORMAT DATE MASK ''YYYY-MM-DD'',
                source_file_name CHAR(255) 
            )
        )
        LOCATION (''' || v_tariff_plan_location || ''')
    )
    REJECT LIMIT UNLIMITED';

    EXECUTE IMMEDIATE v_sql;
    DBMS_OUTPUT.PUT_LINE('External table for tariff plan data created successfully.');

    /*
    ====================================================================
    Create External Table for Subscriber Plan Data
    Purpose: Maps to CSV file containing subscriber-plan mappings
    File Format: Comma-delimited with optional quoting, header row
    Special Handling: Date fields for plan start/end dates
    ====================================================================
    */
    v_sql := 'CREATE TABLE external_subscriber_plan_file (
        subscriber_msisdn VARCHAR2(20) NOT NULL,      -- Subscriber mobile number
        plan_id VARCHAR2(20) NOT NULL,                -- Associated plan ID
        plan_start_date DATE,                         -- When plan became active
        plan_end_date DATE,                           -- When plan was terminated
        source_file_name VARCHAR2(255)                -- Original source filename
    )
    ORGANIZATION EXTERNAL (
        TYPE ORACLE_LOADER
        DEFAULT DIRECTORY DATA_DIR
        ACCESS PARAMETERS (
            RECORDS DELIMITED BY NEWLINE
            SKIP 1                                    -- Skip header row
            FIELDS TERMINATED BY '','' OPTIONALLY ENCLOSED BY ''"''
            MISSING FIELD VALUES ARE NULL
            (
                subscriber_msisdn,
                plan_id,
                plan_start_date CHAR(10) DATE_FORMAT DATE MASK ''YYYY-MM-DD'',
                plan_end_date CHAR(10) DATE_FORMAT DATE MASK ''YYYY-MM-DD'',
                source_file_name CHAR(255) 
            )
        )
        LOCATION (''' || v_subscriber_plan_location || ''')
    )
    REJECT LIMIT UNLIMITED';

    EXECUTE IMMEDIATE v_sql;
    DBMS_OUTPUT.PUT_LINE('External table for subscriber plan data created successfully.');

    DBMS_OUTPUT.PUT_LINE('All external tables created successfully.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error creating external tables: ' || SQLERRM);
        RAISE;  -- Re-raise the exception for calling code to handle
END;
/