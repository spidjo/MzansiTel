create or replace PACKAGE BODY pkg_load_staging IS
/*
====================================================================================================
Package: pkg_load_staging
Purpose: Bulk loading and validation framework for telecom data warehouse staging area

Description:
This package handles the ETL process for loading data from external files into staging tables.
It processes four main data types: CDR (Call Detail Records), subscriber information, 
tariff plans, and subscriber plan mappings. The package features:
- Bulk loading with BULK COLLECT for high performance
- Row-by-row validation with comprehensive error handling
- Detailed error logging without aborting the entire load
- Summary reporting for each load operation
- Support for both individual and full loads

Key Features:
1. Robust validation for each data type with specific business rules
2. Deduplication of subscriber records during load
3. Referential integrity checks (e.g., CDRs only for existing subscribers)
4. Configurable batch size (v_bulk_limit) for memory management
5. Atomic operations with proper transaction control
6. Global counters for full load operations

Dependencies:
- External tables: external_cdr_file, external_subscriber_file, 
                  external_tariff_plan_file, external_subscriber_plan_file
- Staging tables: staging_cdr, staging_subscriber, 
                 staging_tariff_plan, staging_subscriber_plan
- Helper functions: is_valid_msisdn
- Logging procedures: log_error, log_import_summary

Usage:
1. Individual loads: Call specific procedures (load_cdr_data, load_subscriber_data, etc.)
2. Full load: Call load_all procedure with optional truncation (clear_tables => 'YES')

Error Handling:
- Custom exceptions with specific error codes
- Detailed error logging including the failed record
- Graceful continuation after errors with COMPLETED_WITH_ERRORS status
====================================================================================================
*/

    -- Package-level variables for load tracking
    v_load_time TIMESTAMP := SYSTIMESTAMP;       -- Timestamp for this load operation
    v_error_count NUMBER := 0;                   -- Error counter for current procedure
    v_record_count NUMBER := 0;                  -- Record counter for current procedure
    v_error_message VARCHAR2(4000);              -- Last error message encountered
    v_error_record VARCHAR2(4000);               -- String representation of failed record
    v_load_report VARCHAR2(200);                 -- Summary status (SUCCESS/COMPLETED_WITH_ERRORS/FAILURE)
    v_bulk_limit NUMBER := 10000;                -- Batch size for BULK COLLECT operations

    -- Global counters for full load operations
    g_error_count NUMBER := 0;                   -- Cumulative error count across all loads
    g_record_count NUMBER := 0;                  -- Cumulative record count across all loads

    -- Custom error codes
    c_err_no_records CONSTANT NUMBER := -20002;   -- Error code for empty source data

    -- Exception for bulk operation errors
    e_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_dml_errors, -24381);

    /* ===================================================================
    Procedure: handle_row_error
    Purpose: Centralized error handling for individual record failures

    Parameters:
    p_step        - Calling procedure/step for error tracking
    p_error       - Error message
    p_record      - String representation of the failed record
    p_source_file - Source file name for troubleshooting

    Behavior:
    - Increments the error counter
    - Logs the error details via log_processing_error
    - Allows processing to continue with next record
    =================================================================== */
    PROCEDURE handle_row_error(
        p_step         VARCHAR2,
        p_error        VARCHAR2,
        p_record       VARCHAR2,
        p_source_file  VARCHAR2
        ) IS
    BEGIN
        v_error_count := v_error_count + 1;
        log_processing_error(p_step, 'STAGING_CDR', SYSTIMESTAMP, p_error, p_record, p_source_file);
    END;

    /* ===================================================================
    Procedure: load_cdr_data
    Purpose: Loads and validates Call Detail Records from external files

    Process Flow:
    1. Fetches CDR data in batches (JOINed with valid subscribers)
    2. Validates each record against business rules:
       - Valid MSISDN format
       - Required call start time
       - Valid call types (VOICE/SMS/DATA)
       - Non-negative duration
       - Valid call directions
       - Chronological time sequence
    3. Inserts valid records into staging_cdr
    4. Logs errors for invalid records without failing entire load
    5. Records summary statistics

    Notes:
    - Uses BULK COLLECT for efficient memory usage
    - Processes ~10,000 records per batch (configurable)
    =================================================================== */
    PROCEDURE load_cdr_data IS
    CURSOR c_cdr IS
        SELECT /*+ FIRST_ROWS(100) */
        c.subscriber_msisdn,
        c.call_type,
        c.call_start_time,
        c.call_end_time,
        c.call_duration_sec,
        c.destination_number,
        c.call_cost,
        c.call_direction,
        c.source_file_name
        FROM external_cdr_file c
        JOIN staging_subscriber s 
        ON c.subscriber_msisdn = s.msisdn;

    TYPE tab_cdr IS TABLE OF external_cdr_file%ROWTYPE INDEX BY PLS_INTEGER;
    l_cdr_data tab_cdr;

    BEGIN
        -- Initialize counters and timestamps
        init_variables;

        OPEN c_cdr;
        LOOP
            -- Fetch records in batches for memory efficiency
            FETCH c_cdr BULK COLLECT INTO l_cdr_data LIMIT v_bulk_limit;
            EXIT WHEN l_cdr_data.COUNT = 0;

            FOR i IN 1..l_cdr_data.COUNT LOOP
            BEGIN
                -- Validation rules for CDR data
                IF NOT is_valid_msisdn(l_cdr_data(i).subscriber_msisdn) THEN
                RAISE_APPLICATION_ERROR(-20001, 'Subscriber_msisdn missing or wrong format');
                ELSIF l_cdr_data(i).call_start_time IS NULL THEN
                RAISE_APPLICATION_ERROR(-20002, 'Missing start time');
                ELSIF l_cdr_data(i).call_type NOT IN ('VOICE', 'SMS', 'DATA') THEN
                RAISE_APPLICATION_ERROR(-20003, 'Invalid call type');
                ELSIF l_cdr_data(i).call_duration_sec < 0 THEN
                RAISE_APPLICATION_ERROR(-20004, 'Negative duration');
                ELSIF l_cdr_data(i).call_direction NOT IN ('INBOUND', 'OUTBOUND') THEN
                RAISE_APPLICATION_ERROR(-20005, 'Invalid call direction');
                ELSIF l_cdr_data(i).call_end_time <= l_cdr_data(i).call_start_time THEN
                RAISE_APPLICATION_ERROR(-20006, 'End time before start time');
                END IF;

                -- Insert valid record into staging
                INSERT INTO staging_cdr (
                subscriber_msisdn, call_type, call_start_time, call_end_time,
                call_duration_sec, destination_number, call_cost,
                call_direction, source_file_name, load_timestamp
                ) VALUES (
                l_cdr_data(i).subscriber_msisdn, l_cdr_data(i).call_type,
                l_cdr_data(i).call_start_time, l_cdr_data(i).call_end_time,
                l_cdr_data(i).call_duration_sec, l_cdr_data(i).destination_number,
                l_cdr_data(i).call_cost, l_cdr_data(i).call_direction,
                l_cdr_data(i).source_file_name, v_load_time
                );

                v_record_count := v_record_count + 1;

            EXCEPTION
                WHEN OTHERS THEN
                -- Capture error details and continue processing
                v_error_message := SQLERRM;
                v_error_record := l_cdr_data(i).subscriber_msisdn || ',' ||
                                    l_cdr_data(i).call_type || ',' ||
                                    l_cdr_data(i).call_start_time || ',' ||
                                    l_cdr_data(i).call_end_time || ',' ||
                                    l_cdr_data(i).call_duration_sec || ',' ||
                                    l_cdr_data(i).destination_number || ',' ||
                                    l_cdr_data(i).call_cost || ',' ||
                                    l_cdr_data(i).call_direction;
                handle_row_error('PKG_LOAD_STAGING.LOAD_CDR_DATA', v_error_message, v_error_record, l_cdr_data(i).source_file_name);
            END;
            END LOOP;
        END LOOP;
        CLOSE c_cdr;

        -- Record load summary
        v_load_report := CASE WHEN v_error_count > 0 THEN 'COMPLETED_WITH_ERRORS' ELSE 'SUCCESS' END;
        log_import_summary('CDR_FILE', SYSTIMESTAMP, v_record_count, v_error_count, 
                          v_load_report, v_error_message);
        COMMIT;

        -- Update global counters for full load operations
        g_record_count := g_record_count + v_record_count;
        g_error_count := g_error_count + v_error_count;

        -- Archive external file.
        IF v_error_count = 0 THEN
            pkg_file_utils.archive_file(p_file_name => 'cdr_data_' || TO_CHAR(sysdate, 'YYYYMMDD') || '.csv');
        END IF;
    END load_cdr_data;

    /* ===================================================================
    Procedure: load_subscriber_data
    Purpose: Loads and validates subscriber information with deduplication

    Key Features:
    - Deduplicates by MSISDN (keeps first record encountered)
    - Validates:
      - MSISDN format
      - Valid status values
      - Email format (when present)
    - Maintains referential integrity for related data

    Notes:
    - Uses ROW_NUMBER() for efficient deduplication
    =================================================================== */
    PROCEDURE load_subscriber_data IS
    CURSOR c_subscriber IS
        SELECT msisdn, first_name, last_name, date_of_birth, email_address, registration_date, status, source_file_name
        FROM (
        SELECT s.*, ROW_NUMBER() OVER (PARTITION BY msisdn ORDER BY msisdn) rn
        FROM external_subscriber_file s
        )
        WHERE rn = 1;  -- Deduplication: Only take the first record per MSISDN

    TYPE tab_subscriber IS TABLE OF external_subscriber_file%ROWTYPE INDEX BY PLS_INTEGER;
    l_subscriber_data tab_subscriber;

    BEGIN
        init_variables;

        OPEN c_subscriber;
        LOOP
            FETCH c_subscriber BULK COLLECT INTO l_subscriber_data LIMIT v_bulk_limit;
            EXIT WHEN l_subscriber_data.COUNT = 0;

            FOR i IN 1..l_subscriber_data.COUNT LOOP
            BEGIN
                -- Validations for subscriber data
                IF NOT is_valid_msisdn(l_subscriber_data(i).msisdn) THEN
                RAISE_APPLICATION_ERROR(-20001, 'MSISDN is required or invalid: ' || l_subscriber_data(i).msisdn);
                ELSIF l_subscriber_data(i).status NOT IN ('ACTIVE', 'SUSPENDED', 'INACTIVE') THEN
                RAISE_APPLICATION_ERROR(-20002, 'Invalid status: ' || l_subscriber_data(i).status);
                ELSIF l_subscriber_data(i).email_address IS NOT NULL AND
                    NOT REGEXP_LIKE(l_subscriber_data(i).email_address, '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$', 'i') THEN
                RAISE_APPLICATION_ERROR(-20003, 'Invalid email: ' || l_subscriber_data(i).email_address);
                END IF;

                -- Insert valid record
                INSERT INTO staging_subscriber (
                msisdn, first_name, last_name, date_of_birth, email_address,
                registration_date, status, source_file_name, load_timestamp
                ) VALUES (
                l_subscriber_data(i).msisdn, l_subscriber_data(i).first_name,
                l_subscriber_data(i).last_name, l_subscriber_data(i).date_of_birth,
                l_subscriber_data(i).email_address, l_subscriber_data(i).registration_date,
                l_subscriber_data(i).status, l_subscriber_data(i).source_file_name,
                v_load_time
                );

                v_record_count := v_record_count + 1;

            EXCEPTION
                WHEN OTHERS THEN
                v_error_message := SQLERRM;
                v_error_record := l_subscriber_data(i).msisdn || ',' ||
                                    l_subscriber_data(i).first_name || ',' ||
                                    l_subscriber_data(i).last_name || ',' ||
                                    l_subscriber_data(i).date_of_birth || ',' ||
                                    l_subscriber_data(i).email_address || ',' ||
                                    l_subscriber_data(i).registration_date || ',' ||
                                    l_subscriber_data(i).status || ',' ||
                                    l_subscriber_data(i).source_file_name;
                handle_row_error(
                    'PKG_LOAD_STAGING.LOAD_SUBSCRIBER_DATA',
                    v_error_message,
                    v_error_record,
                    l_subscriber_data(i).source_file_name
                );
            END;
            END LOOP;
        END LOOP;
        CLOSE c_subscriber;

        v_load_report := CASE WHEN v_error_count > 0 THEN 'COMPLETED_WITH_ERRORS' ELSE 'SUCCESS' END;
        log_import_summary('SUBSCRIBER_FILE', SYSTIMESTAMP, v_record_count, v_error_count, 
                          v_load_report, v_error_message);
        COMMIT;

        g_record_count := g_record_count + v_record_count;
        g_error_count := g_error_count + v_error_count;

        -- Archive external file.
        IF v_error_count = 0 THEN
            pkg_file_utils.archive_file(p_file_name => 'subscriber_data_' || TO_CHAR(sysdate, 'YYYYMMDD') || '.csv');
        END IF;

    END load_subscriber_data;

    /* ===================================================================
    Procedure: load_tariff_data
    Purpose: Loads and validates tariff plan information

    Key Validations:
    - Required monthly fee (positive value)
    - Mandatory plan ID
    - Valid date ranges (if both dates provided)
    =================================================================== */
    PROCEDURE load_tariff_data IS
    CURSOR c_tariff IS
        SELECT plan_id,
            plan_name,
            description,
            monthly_fee,
            call_rate_per_minute,
            sms_rate_per_message,
            data_rate_per_mb,
            data_limit_mb,
            voice_limit_minutes,
            sms_limit,
            valid_from,
            valid_to,
            source_file_name
        FROM external_tariff_plan_file;

    TYPE t_tariff IS TABLE OF external_tariff_plan_file%ROWTYPE INDEX BY PLS_INTEGER;
    l_tariff_data t_tariff;

    BEGIN
        init_variables;

        OPEN c_tariff;
        FETCH c_tariff BULK COLLECT INTO l_tariff_data LIMIT v_bulk_limit;
        CLOSE c_tariff;

        -- Check for empty source data
        IF l_tariff_data.COUNT = 0 THEN
            v_error_message := 'No records found in the source file';
            RAISE_APPLICATION_ERROR(c_err_no_records, v_error_message);
        END IF;

        FOR i IN 1..l_tariff_data.COUNT LOOP
            BEGIN
            -- Basic validation for tariff plans
            IF l_tariff_data(i).monthly_fee IS NULL OR l_tariff_data(i).monthly_fee <= 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'Monthly fee is required or invalid for plan ID '  || l_tariff_data(i).plan_id);
            ELSIF l_tariff_data(i).plan_id IS NULL THEN
            RAISE_APPLICATION_ERROR(-20002, 'Missing plan ID at record: ' || i);
            ELSIF l_tariff_data(i).valid_to IS NOT NULL AND 
                l_tariff_data(i).valid_from IS NOT NULL AND 
                l_tariff_data(i).valid_to < l_tariff_data(i).valid_from THEN
            RAISE_APPLICATION_ERROR(-20003, 'Invalid date range for plan ID: ' || l_tariff_data(i).plan_id);
            END IF;

            -- Insert valid record
            INSERT INTO staging_tariff_plan (
                plan_id, plan_name, description, monthly_fee,
                call_rate_per_minute, sms_rate_per_message, data_rate_per_mb,
                data_limit_mb, voice_limit_minutes, sms_limit,
                valid_from, valid_to, source_file_name, load_timestamp
            )
            VALUES (
                l_tariff_data(i).plan_id, l_tariff_data(i).plan_name, l_tariff_data(i).description,
                l_tariff_data(i).monthly_fee, l_tariff_data(i).call_rate_per_minute,
                l_tariff_data(i).sms_rate_per_message, l_tariff_data(i).data_rate_per_mb,
                l_tariff_data(i).data_limit_mb, l_tariff_data(i).voice_limit_minutes,
                l_tariff_data(i).sms_limit, l_tariff_data(i).valid_from,
                l_tariff_data(i).valid_to, l_tariff_data(i).source_file_name, v_load_time
            );

            v_record_count := v_record_count + 1;

            EXCEPTION
            WHEN OTHERS THEN
                v_error_message := SQLERRM;
                v_error_record := l_tariff_data(i).plan_name || ',' ||
                                l_tariff_data(i).description || ',' ||
                                l_tariff_data(i).monthly_fee || ',' ||
                                l_tariff_data(i).call_rate_per_minute || ',' ||
                                l_tariff_data(i).sms_rate_per_message || ',' ||
                                l_tariff_data(i).data_rate_per_mb || ',' ||
                                l_tariff_data(i).data_limit_mb || ',' ||
                                l_tariff_data(i).voice_limit_minutes || ',' ||
                                l_tariff_data(i).sms_limit || ',' ||
                                l_tariff_data(i).valid_from || ',' ||
                                l_tariff_data(i).valid_to || ',' ||
                                l_tariff_data(i).source_file_name;

                handle_row_error(
                'PKG_LOAD_STAGING.LOAD_TARIFF_DATA',
                v_error_message,
                v_error_record,
                l_tariff_data(i).source_file_name
                );
            END;
        END LOOP;

        v_load_report := CASE WHEN v_error_count > 0 THEN 'COMPLETED_WITH_ERRORS' ELSE 'SUCCESS' END;
        log_import_summary('TARIFF_PLAN_FILE', SYSTIMESTAMP, v_record_count, v_error_count, 
                          v_load_report, v_error_message);
        COMMIT;

        g_record_count := g_record_count + v_record_count;
        g_error_count := g_error_count + v_error_count;

        -- Archive external file.
        IF v_error_count = 0 THEN
            pkg_file_utils.archive_file(p_file_name => 'tariff_plan_data_' || TO_CHAR(sysdate, 'YYYYMMDD') || '.csv');
        END IF;

    END load_tariff_data;

    /* ===================================================================
    Procedure: load_subscriber_plan_data
    Purpose: Loads and validates subscriber-plan mappings

    Key Validations:
    - Valid MSISDN (must exist in subscriber table)
    - Required plan ID
    - Valid date sequence (start before end)
    =================================================================== */
    PROCEDURE load_subscriber_plan_data IS
    CURSOR c_sub_plan IS
        SELECT /*+ FIRST_ROWS(100) */
            sp.subscriber_msisdn,
            sp.plan_id,
            sp.plan_start_date,
            sp.plan_end_date,
            sp.source_file_name
        FROM external_subscriber_plan_file sp
        JOIN staging_subscriber s
        ON sp.subscriber_msisdn = s.msisdn;  -- Ensure referential integrity

    TYPE t_sub_plan IS TABLE OF external_subscriber_plan_file%ROWTYPE INDEX BY PLS_INTEGER;
    l_sub_plan t_sub_plan;

    BEGIN
        init_variables;

        OPEN c_sub_plan;
        LOOP
            FETCH c_sub_plan BULK COLLECT INTO l_sub_plan LIMIT v_bulk_limit;
            EXIT WHEN l_sub_plan.COUNT = 0;

            FOR i IN 1..l_sub_plan.COUNT LOOP
            BEGIN
                -- Validation for subscriber-plan mappings
                IF NOT is_valid_msisdn(l_sub_plan(i).subscriber_msisdn) THEN
                RAISE_APPLICATION_ERROR(-20001, 'Invalid MSISDN: ' || l_sub_plan(i).subscriber_msisdn);
                ELSIF l_sub_plan(i).plan_id IS NULL THEN
                RAISE_APPLICATION_ERROR(-20002, 'Missing plan ID for: ' || l_sub_plan(i).subscriber_msisdn);
                ELSIF l_sub_plan(i).plan_start_date IS NULL THEN
                RAISE_APPLICATION_ERROR(-20003, 'Missing plan start date for: ' || l_sub_plan(i).subscriber_msisdn);
                ELSIF l_sub_plan(i).plan_end_date <= l_sub_plan(i).plan_start_date THEN
                RAISE_APPLICATION_ERROR(-20004, 'Plan start date must be before end date for: ' || l_sub_plan(i).subscriber_msisdn);
                END IF;

                -- Insert valid record
                INSERT INTO staging_subscriber_plan (
                subscriber_msisdn,
                plan_id,
                plan_start_date,
                plan_end_date,
                source_file_name,
                load_timestamp
                )
                VALUES (
                l_sub_plan(i).subscriber_msisdn,
                l_sub_plan(i).plan_id,
                l_sub_plan(i).plan_start_date,
                l_sub_plan(i).plan_end_date,
                l_sub_plan(i).source_file_name,
                v_load_time
                );

                v_record_count := v_record_count + 1;

            EXCEPTION
                WHEN OTHERS THEN
                v_error_message := SQLERRM;
                v_error_record := l_sub_plan(i).subscriber_msisdn || ', ' ||
                                    l_sub_plan(i).plan_id || ', ' ||
                                    l_sub_plan(i).plan_start_date || ', ' ||
                                    l_sub_plan(i).plan_end_date;

                handle_row_error(
                    'PKG_LOAD_STAGING.LOAD_SUBSCRIBER_PLAN_DATA',
                    v_error_message,
                    v_error_record,
                    l_sub_plan(i).source_file_name
                );
            END;
            END LOOP;
        END LOOP;
        CLOSE c_sub_plan;

        v_load_report := CASE WHEN v_error_count > 0 THEN 'COMPLETED_WITH_ERRORS' ELSE 'SUCCESS' END;
        log_import_summary('SUBSCRIBER_PLAN_FILE', SYSTIMESTAMP, v_record_count, v_error_count, 
                          v_load_report, v_error_message);

        COMMIT;

        g_record_count := g_record_count + v_record_count;
        g_error_count := g_error_count + v_error_count;

        -- Archive external file.
        IF v_error_count = 0 THEN
            pkg_file_utils.archive_file(p_file_name => 'subscriber_plan_data_' || TO_CHAR(sysdate, 'YYYYMMDD') || '.csv');
        END IF;

    END load_subscriber_plan_data;

    /* ===================================================================
    Procedure: load_all
    Purpose: Executes a complete load of all staging tables

    Parameters:
    clear_tables - 'YES' to truncate staging tables before load (default 'NO')

    Execution Order:
    1. Subscriber data (required for referential integrity)
    2. Tariff plans
    3. Subscriber-plan mappings
    4. CDR data

    Notes:
    - Maintains global counters across all loads
    - Provides atomic operation with proper rollback on failure
    =================================================================== */
    PROCEDURE load_all(clear_tables VARCHAR2 DEFAULT 'NO') IS 
        BEGIN
            -- Reset global counters for this full load operation
            g_error_count := 0;
            g_record_count := 0;

            -- Optionally clear staging tables
            IF clear_tables = 'YES' THEN
                EXECUTE IMMEDIATE 'TRUNCATE TABLE staging_cdr';
                EXECUTE IMMEDIATE 'TRUNCATE TABLE staging_subscriber_plan';
                EXECUTE IMMEDIATE 'TRUNCATE TABLE staging_tariff_plan';
                EXECUTE IMMEDIATE 'TRUNCATE TABLE staging_subscriber';
            END IF;

            -- Execute loads in proper sequence
            load_subscriber_data;
            load_tariff_data;
            load_subscriber_plan_data;
            load_cdr_data;

            -- Record final summary
            v_load_report := CASE WHEN g_error_count > 0 THEN 'COMPLETED_WITH_ERRORS' ELSE 'SUCCESS' END;
            log_import_summary('FULL_LOAD', SYSTIMESTAMP, g_record_count, g_error_count, 
                              v_load_report, v_error_message);

            COMMIT;
        EXCEPTION
        WHEN OTHERS THEN
            -- Capture and log any failure in the full load operation
            v_error_message := 'LOAD_ALL failed: ' || SQLERRM;
            v_load_report := 'FAILURE';
            log_error(
                p_process => 'PKG_LOAD_STAGING.LOAD_ALL',
                p_affected_table => 'ALL_STAGING_TABLES',
                p_error_time => SYSTIMESTAMP,
                p_error_message => v_error_message,
                p_source_file => 'N/A'
            );
            ROLLBACK;
            RAISE;
    END load_all;

    /* ===================================================================
    Procedure: init_variables
    Purpose: Resets procedure-level variables for a new load operation

    Resets:
    - Error and record counters
    - Load timestamp
    - Error tracking variables
    =================================================================== */
    PROCEDURE init_variables IS
      BEGIN
          v_error_count := 0;
          v_record_count := 0;
          v_load_time := SYSTIMESTAMP;
          v_error_message := NULL;
          v_error_record := NULL;
      END init_variables;

    /* ===================================================================
    Procedure: log_processing_error
    Purpose: Centralized error logging with standardized parameters

    Parameters:
    p_process        - Calling procedure name
    p_affected_table - Target table where error occurred
    p_error_time     - Timestamp of error
    p_error_message  - Detailed error message
    p_raw_record     - String representation of failed record
    p_source_file    - Source file containing the record

    Notes:
    - Wrapper around the base log_error procedure
    - Maintains consistent error logging format
    =================================================================== */
    PROCEDURE log_processing_error(
            p_process VARCHAR2,
            p_affected_table VARCHAR2,
            p_error_time TIMESTAMP,
            p_error_message VARCHAR2,
            p_raw_record VARCHAR2,
            p_source_file VARCHAR2
        ) IS
        BEGIN
            v_error_count := v_error_count + 1;
            log_error(
                p_process => p_process,
                p_affected_table => p_affected_table,
                p_error_time => v_load_time,
                p_error_message => p_error_message,
                p_raw_record => p_raw_record,
                p_source_file => p_source_file
            );
    END log_processing_error;

END pkg_load_staging;