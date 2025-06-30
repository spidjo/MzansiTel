
CREATE OR REPLACE DIRECTORY DATA_DIR AS 'C:/app/siphi/product/21c/mzansitel/data/inbound/';
CREATE OR REPLACE DIRECTORY ARCHIVE_DIR AS 'C:/app/siphi/product/21c/mzansitel/data/archive/';

SELECT table_name FROM all_external_tables WHERE table_name LIKE 'EXTERNAL_%';

-- Test loading staging 
BEGIN
    pkg_load_staging.p_load_all('YES');
END;
/
-- Check staging table
SELECT COUNT(*) FROM staging_cdr;
SELECT * FROM staging_cdr;

-- Test loading core
BEGIN
    pkg_load_core.p_load_all;
END;
/
SELECT COUNT(*) FROM subscriber;
SELECT COUNT(*) FROM subscriber_plan;
SELECT COUNT(*) FROM call_detail_record;
SELECT * FROM call_detail_record;
-- Generate monthly bills
BEGIN
    pkg_billing.generate_monthly_bills(TO_DATE('2025-05-01', 'YYYY-MM-DD'));
END;
/
SELECT * FROM invoice;

--Record payment
BEGIN
    pkg_billing.record_payment(
        p_invoice_id => 103,
        p_payment_date => SYSDATE,
        p_payment_amount => 1610.45,
        p_payment_method => 'Credit Card'
    );
END;
/
SELECT * FROM payment WHERE invoice_id = 103;
SELECT * FROM invoice WHERE invoice_id = 103;

-- Total revenue by month
SELECT TO_CHAR(payment_date, 'YYYY-MM') AS month, SUM(payment_amount) AS revenue
FROM payment
GROUP BY TO_CHAR(payment_date, 'YYYY-MM');

-- Active subscribers
SELECT COUNT(*) AS active_subscribers
FROM subscriber
WHERE status = 'ACTIVE';



EXEC DBMS_STATS.GATHER_TABLE_STATS('mzansitel', 'STAGING_CDR');
EXEC DBMS_STATS.GATHER_TABLE_STATS('mzansitel', 'CALL_DETAIL_RECORD');