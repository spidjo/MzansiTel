create or replace PROCEDURE log_error(
    p_process         VARCHAR2,
    p_affected_table  VARCHAR2,
    p_error_time      TIMESTAMP DEFAULT SYSTIMESTAMP,
    p_error_message   VARCHAR2,
    p_raw_record      VARCHAR2 DEFAULT NULL,
    p_source_file     VARCHAR2 DEFAULT NULL
) IS
/**
 * Logs error details to the error tracking table
 * 
 * @param p_process        Name of the process/package where error occurred
 * @param p_affected_table Table affected by the error (if applicable)
 * @param p_error_time     Timestamp of error (defaults to current time)
 * @param p_error_message  Detailed error message/text
 * @param p_raw_record     Raw data that caused the error (optional)
 * @param p_source_file    Source file name for ETL processes (optional)
 *
 * Usage Example:
 * BEGIN
 *   log_error('PKG_BILLING.CALCULATE_CHARGES', 'INVOICE', 
 *             p_error_message => SQLERRM, p_raw_record => 'MSISDN: 27831234567');
 * END;
 */
BEGIN
    -- Insert error record with all provided details
    INSERT INTO log_errors(
        process,
        affected_table,
        error_time,
        error_message,
        raw_record,
        source_file
    )
    VALUES(
        p_process,
        p_affected_table,
        p_error_time,
        p_error_message,
        p_raw_record,
        p_source_file
    );
    
    -- Explicit commit to ensure error is logged even if calling transaction rolls back
    COMMIT;
END log_error;
/

create or replace PROCEDURE log_import_summary (
    source_file    IN VARCHAR2,
    import_date    IN DATE DEFAULT SYSDATE,
    record_count   IN NUMBER,
    error_count    IN NUMBER DEFAULT 0,
    status         IN VARCHAR2 DEFAULT 'SUCCESS',
    error_message  IN VARCHAR2 DEFAULT NULL
) IS 
/**
 * Records summary statistics for data import operations
 * 
 * @param source_file   Name/path of the imported file
 * @param import_date   Date of import (defaults to current date)
 * @param record_count  Total records processed
 * @param error_count   Number of records with errors (default 0)
 * @param status        Overall status ('SUCCESS'/'FAILED'/'PARTIAL')
 * @param error_message Summary error message if status is not SUCCESS
 *
 * Usage Example:
 * BEGIN
 *   log_import_summary('customers_202306.csv', record_count => 1500, 
 *                      error_count => 23, status => 'PARTIAL');
 * END;
 */
BEGIN
    -- Log import metrics for auditing and performance tracking
    INSERT INTO import_log (
        source_file, 
        import_date, 
        record_count, 
        error_count, 
        status, 
        error_message
    )
    VALUES (
        source_file, 
        import_date, 
        record_count, 
        error_count, 
        status, 
        error_message
    );
    
    COMMIT;
END log_import_summary;
/

create or replace PROCEDURE notifications_prc (
    p_subscriber_id      NUMBER,
    p_notification_type  VARCHAR2,
    p_sent_date         DATE DEFAULT SYSDATE,
    p_status            VARCHAR2 DEFAULT 'SENT',
    p_channel           VARCHAR2,
    p_message           VARCHAR2
) IS
/**
 * Handles creation and logging of all system notifications
 * 
 * @param p_subscriber_id      Target subscriber ID
 * @param p_notification_type  Notification category (e.g., 'BILLING', 'PAYMENT')
 * @param p_sent_date          When notification was sent (defaults to current date)
 * @param p_status            Delivery status ('SENT', 'FAILED', 'PENDING')
 * @param p_channel           Delivery channel ('SMS', 'EMAIL', 'APP')
 * @param p_message           Actual notification content
 *
 * Usage Example:
 * BEGIN
 *   notifications_prc(12345, 'BILLING', p_channel => 'SMS',
 *                    p_message => 'Your bill is ready: R299.50');
 * END;
 */
BEGIN
    -- Record notification attempt in the system
    INSERT INTO notification(
        subscriber_id,
        notification_type,
        sent_date,
        status,
        channel,
        message
    )
    VALUES(
        p_subscriber_id,
        p_notification_type,
        p_sent_date,
        p_status,
        p_channel,
        p_message
    );
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        -- Log notification failure but allow calling process to continue
        log_error('NOTIFICATIONS_PRC', 'NOTIFICATION', 
                 p_error_message => SQLERRM, 
                 p_raw_record => 'Subscriber: ' || p_subscriber_id);
END notifications_prc;
/

create or replace FUNCTION is_valid_msisdn(p_msisdn VARCHAR2) RETURN BOOLEAN IS
/**
 * Validates South African MSISDN format (+27 followed by 9 digits)
 * 
 * @param p_msisdn The phone number to validate
 * @return BOOLEAN TRUE if valid format, FALSE otherwise
 *
 * Usage Example:
 * IF is_valid_msisdn('+27831234567') THEN ...
 * 
 * Note: Does not verify if number is actually in use, just checks format
 */
BEGIN
  -- Validate format: +27 country code followed by 9 digits
  RETURN REGEXP_LIKE(p_msisdn, '^\+27\d{9}$');
EXCEPTION
    WHEN OTHERS THEN
        -- Return FALSE for any unexpected errors during validation
        RETURN FALSE;
END is_valid_msisdn;
/