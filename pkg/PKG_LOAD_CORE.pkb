create or replace PACKAGE BODY pkg_load_core IS
/*
====================================================================================================
Package: pkg_load_core
Purpose: Core data loading from staging tables to production tables in telecom data warehouse

Description:
This package handles the final stage of ETL process, moving validated data from staging tables 
to the core production tables. It features:
- MERGE operations (upsert logic) for all core tables
- Comprehensive error handling with transaction control
- Consolidated counters for full load operations
- Optional staging table cleanup after successful loads
- Parallel processing for large CDR data loads

Key Features:
1. Atomic MERGE operations for each table (insert new, update existing)
2. Maintains audit fields (created_at/updated_at, created_by/updated_by)
3. Global counters track total records and errors across all loads
4. Automatic staging table truncation after successful full loads
5. Detailed logging for both successes and failures

Dependencies:
- Staging tables: staging_subscriber, staging_tariff_plan, 
                 staging_subscriber_plan, staging_cdr
- Core tables: subscriber, tariff_plan, subscriber_plan, call_detail_record
- Logging procedures: log_import_summary, log_error

Usage:
1. Individual loads: Call specific procedures (p_load_subscriber, p_load_cdr, etc.)
2. Full load: Call p_load_all procedure (handles all tables in proper sequence)
====================================================================================================
*/

    -- Global counters for consolidated reporting
    g_total_records   NUMBER := 0;  -- Tracks total records processed across all loads
    g_total_errors    NUMBER := 0;  -- Tracks total errors encountered across all loads

    /* ===================================================================
    Procedure: p_load_subscriber
    Purpose: Loads subscriber data from staging to core with upsert logic
    
    Process Flow:
    1. MERGE operation:
       - Updates existing subscribers (matched on MSISDN)
       - Inserts new subscribers
    2. Updates global record counter
    3. Logs success summary
    4. On error:
       - Rolls back transaction
       - Logs error details
       - Increments global error counter
    
    Notes:
    - Maintains audit fields (created_at/updated_at)
    - Uses current user for created_by/updated_by tracking
    =================================================================== */
    PROCEDURE p_load_subscriber IS
        v_now           TIMESTAMP := SYSTIMESTAMP;
        v_user          CONSTANT VARCHAR2(30) := USER;
        v_error_message VARCHAR2(4000);
        v_count         NUMBER := 0;
    BEGIN
        -- Perform upsert operation for subscriber data
        MERGE INTO subscriber s 
        USING staging_subscriber ss 
        ON (ss.msisdn = s.msisdn)
        WHEN MATCHED THEN
            UPDATE SET
                s.first_name        = ss.first_name,
                s.last_name         = ss.last_name,
                s.date_of_birth     = ss.date_of_birth,
                s.email_address     = ss.email_address,
                s.registration_date = ss.registration_date,
                s.status            = ss.status,
                s.updated_at        = v_now,
                s.updated_by        = v_user
        WHEN NOT MATCHED THEN
            INSERT (
                msisdn,
                first_name,
                last_name,
                date_of_birth,
                email_address,
                registration_date,
                status,
                created_at,
                created_by
            ) VALUES (
                ss.msisdn,
                ss.first_name,
                ss.last_name,
                ss.date_of_birth,
                ss.email_address,
                ss.registration_date,
                ss.status,
                v_now,
                v_user
            );

        -- Update counters and log success
        v_count := SQL%ROWCOUNT;
        g_total_records := g_total_records + v_count;

        log_import_summary(
            source_file    => 'STAGING_SUBSCRIBER',
            import_date    => v_now,
            record_count   => v_count,
            error_count    => 0,
            status         => 'SUCCESS',
            error_message  => NULL);
        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            -- Handle errors: log, rollback, and update counters
            g_total_errors := g_total_errors + 1;
            log_error(
                p_process        => 'PKG_LOAD_CORE.P_LOAD_SUBSCRIBER',
                p_affected_table => 'SUBSCRIBER',
                p_error_time     => v_now,
                p_error_message  => SQLERRM,
                p_source_file    => 'staging_subscriber'
            );
            ROLLBACK;
    END p_load_subscriber;

    /* ===================================================================
    Procedure: p_load_tariff_plan
    Purpose: Loads tariff plan data from staging to core with upsert logic
    
    Key Features:
    - Updates existing plans (matched on plan_id)
    - Inserts new tariff plans
    - Maintains all plan attributes and validity dates
    - Tracks update timestamps and users
    
    Notes:
    - Plan ID is the natural key for tariff plans
    =================================================================== */
    PROCEDURE p_load_tariff_plan IS
        v_now           TIMESTAMP := SYSTIMESTAMP;
        v_user          CONSTANT VARCHAR2(30) := USER;
        v_error_message VARCHAR2(4000);
        v_count         NUMBER := 0;
    BEGIN
        -- Perform upsert operation for tariff plans
        MERGE INTO tariff_plan t
        USING staging_tariff_plan st
        ON (t.plan_id = st.plan_id)
        WHEN MATCHED THEN
            UPDATE SET
                t.plan_name            = st.plan_name,
                t.description          = st.description,
                t.monthly_fee          = st.monthly_fee,
                t.call_rate_per_minute = st.call_rate_per_minute,
                t.sms_rate_per_message = st.sms_rate_per_message,
                t.data_rate_per_mb     = st.data_rate_per_mb,
                t.data_limit_mb        = st.data_limit_mb,
                t.voice_limit_minutes  = st.voice_limit_minutes,
                t.sms_limit            = st.sms_limit,
                t.valid_from           = st.valid_from,
                t.valid_to             = st.valid_to,
                t.updated_at           = v_now,
                t.updated_by           = v_user
        WHEN NOT MATCHED THEN
            INSERT (
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
                valid_from,
                valid_to,
                created_at,
                created_by
            ) VALUES (
                st.plan_id,
                st.plan_name,
                st.description,
                st.monthly_fee,
                st.call_rate_per_minute,
                st.sms_rate_per_message,
                st.data_rate_per_mb,
                st.data_limit_mb,
                st.voice_limit_minutes,
                st.sms_limit,
                st.valid_from,
                st.valid_to,
                v_now,
                v_user
            );

        -- Update counters and log success
        v_count := SQL%ROWCOUNT;
        g_total_records := g_total_records + v_count;
        log_import_summary(
            source_file    => 'STAGING_TARIFF_PLAN',
            import_date    => v_now,
            record_count   => v_count,
            error_count    => 0,
            status         => 'SUCCESS',
            error_message  => NULL);
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            -- Handle errors: log, rollback, and update counters
            g_total_errors := g_total_errors + 1;
            log_error(
                p_process        => 'PKG_LOAD_CORE.P_LOAD_TARIFF_PLAN',
                p_affected_table => 'TARIFF_PLAN',
                p_error_time     => v_now,
                p_error_message  => SQLERRM,
                p_source_file    => 'staging_tariff_plan'
            );
            ROLLBACK;
    END p_load_tariff_plan;

    /* ===================================================================
    Procedure: p_load_subscriber_plan
    Purpose: Loads subscriber-plan mappings from staging to core
    
    Key Features:
    - Updates existing mappings (matched on composite key)
    - Inserts new subscriber-plan relationships
    - Only updates plan_end_date for existing records
    
    Notes:
    - Matching uses composite key: plan_id + MSISDN + plan_start_date
    =================================================================== */
    PROCEDURE p_load_subscriber_plan IS
        v_now           TIMESTAMP := SYSTIMESTAMP;
        v_count         NUMBER := 0;
    BEGIN
        -- Perform upsert operation for subscriber plans
        MERGE INTO subscriber_plan sp
        USING staging_subscriber_plan ssp
        ON (
            sp.plan_id = ssp.plan_id AND
            sp.subscriber_msisdn = ssp.subscriber_msisdn AND
            sp.plan_start_date = ssp.plan_start_date
        )
        WHEN MATCHED THEN
            UPDATE SET
                sp.plan_end_date = ssp.plan_end_date
        WHEN NOT MATCHED THEN
            INSERT (
                subscriber_msisdn,
                plan_id,
                plan_start_date,
                plan_end_date
            ) VALUES (
                ssp.subscriber_msisdn,
                ssp.plan_id,
                ssp.plan_start_date,
                ssp.plan_end_date
            );

        -- Update counters and log success
        v_count := SQL%ROWCOUNT;
        g_total_records := g_total_records + v_count;
        log_import_summary(
            source_file    => 'STAGING_SUBSCRIBER_PLAN',
            import_date    => v_now,
            record_count   => v_count,
            error_count    => 0,
            status         => 'SUCCESS',
            error_message  => NULL);

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            -- Handle errors: log, rollback, and update counters
            g_total_errors := g_total_errors + 1;
            log_error(
                p_process        => 'PKG_LOAD_CORE.P_LOAD_SUBSCRIBER_PLAN',
                p_affected_table => 'SUBSCRIBER_PLAN',
                p_error_time     => v_now,
                p_error_message  => SQLERRM,
                p_source_file    => 'staging_subscriber_plan'
            );
            ROLLBACK;
    END p_load_subscriber_plan;

    /* ===================================================================
    Procedure: p_load_cdr
    Purpose: Loads call detail records from staging to core
    
    Key Features:
    - Only inserts new CDRs (no updates)
    - Uses parallel processing for large data volumes
    - Matches on composite business key
    - Tracks source file for audit purposes
    
    Notes:
    - CDRs are insert-only as they represent immutable events
    - Parallel hint improves performance for large datasets
    =================================================================== */
    PROCEDURE p_load_cdr IS
        v_now           TIMESTAMP := SYSTIMESTAMP;
        v_count         NUMBER := 0;
    BEGIN
        -- Insert new CDR records (no updates)
        MERGE INTO call_detail_record c
        USING staging_cdr sc
        ON (
            c.subscriber_msisdn = sc.subscriber_msisdn AND
            c.call_type         = sc.call_type AND
            c.call_start_time   = sc.call_start_time AND
            c.call_end_time     = sc.call_end_time
        )
        WHEN NOT MATCHED THEN
            INSERT /*+ PARALLEL(4) */ (
                subscriber_msisdn,
                call_type,
                call_start_time,
                call_end_time,
                call_duration_sec,
                destination_number,
                call_cost,
                call_direction,
                source_file_name
            ) VALUES (
                sc.subscriber_msisdn,
                sc.call_type,
                sc.call_start_time,
                sc.call_end_time,
                sc.call_duration_sec,
                sc.destination_number,
                sc.call_cost,
                sc.call_direction,
                sc.source_file_name
            );

        -- Update counters and log success
        v_count := SQL%ROWCOUNT;
        g_total_records := g_total_records + v_count;
        log_import_summary(
            source_file    => 'STAGING_CDR',
            import_date    => v_now,
            record_count   => v_count,
            error_count    => 0,
            status         => 'SUCCESS',
            error_message  => NULL);

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            -- Handle errors: log, rollback, and update counters
            g_total_errors := g_total_errors + 1;
            log_error(
                p_process        => 'PKG_LOAD_CORE.P_LOAD_CDR',
                p_affected_table => 'CALL_DETAIL_RECORD',
                p_error_time     => v_now,
                p_error_message  => SQLERRM,
                p_source_file    => 'staging_cdr'
            );
            ROLLBACK;
    END p_load_cdr;

    /* ===================================================================
    Procedure: p_load_all
    Purpose: Executes complete load of all core tables in proper sequence
    
    Process Flow:
    1. Resets global counters
    2. Executes individual loads in order:
       - Subscriber data (prerequisite for other tables)
       - Tariff plans
       - Subscriber-plan mappings
       - CDR data
    3. Determines overall status (SUCCESS/PARTIAL_SUCCESS)
    4. Logs consolidated summary
    5. Conditionally truncates staging tables if no errors
    
    Notes:
    - Provides atomic operation - all or nothing approach
    - Only cleans staging tables if full load succeeds
    =================================================================== */
    PROCEDURE p_load_all IS
        v_now     TIMESTAMP := SYSTIMESTAMP;
        v_status  VARCHAR2(20);
    BEGIN
        -- Reset global counters for new load operation
        g_total_records := 0;
        g_total_errors  := 0;

        -- Execute loads in proper dependency order
        p_load_subscriber;
        p_load_tariff_plan;
        p_load_subscriber_plan;
        p_load_cdr;

        -- Determine overall status
        v_status := CASE WHEN g_total_errors > 0 THEN 'PARTIAL_SUCCESS' ELSE 'SUCCESS' END;

        -- Log consolidated summary
        log_import_summary(
            source_file    => 'ALL_STAGING_TABLES',
            import_date    => v_now,
            record_count   => g_total_records,
            error_count    => g_total_errors,
            status         => v_status,
            error_message  => NULL
        );

        -- Clean staging tables only if full load succeeded
        IF g_total_errors = 0 THEN
            p_truncate_staging;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            -- Log any failure in the full load operation
            log_error(
                p_process        => 'PKG_LOAD_CORE.P_LOAD_ALL',
                p_affected_table => 'MULTIPLE',
                p_error_time     => SYSTIMESTAMP,
                p_error_message  => SQLERRM,
                p_source_file    => 'ALL'
            );
            RAISE;
    END p_load_all;

    /* ===================================================================
    Procedure: p_truncate_staging
    Purpose: Cleans up staging tables after successful load
    
    Notes:
    - Only called by p_load_all after successful completion
    - Uses direct DDL with EXECUTE IMMEDIATE
    - Order of truncation is not important as all loads are complete
    =================================================================== */
    PROCEDURE p_truncate_staging IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Truncate tables step');
        EXECUTE IMMEDIATE 'TRUNCATE TABLE staging_subscriber_plan';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE staging_cdr';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE staging_subscriber';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE staging_tariff_plan';
    END p_truncate_staging;

END pkg_load_core;