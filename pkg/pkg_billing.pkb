create or replace PACKAGE BODY pkg_billing IS
/**
 * Billing Management Package
 * 
 * Handles all billing-related operations including:
 * - Individual subscriber charge calculations
 * - Monthly batch billing generation
 * - Usage charge computations based on tariff plans
 * - Payment recording and notification
 *
 * Dependencies: 
 * - notifications_prc (notification procedure)
 * - log_error (error logging procedure)
 * 
 * Tables Accessed:
 * - call_detail_record
 * - subscriber_plan
 * - subscriber
 * - invoice
 * - payment
 * - tariff_plan
 */

  PROCEDURE calculate_charges(p_msisdn IN VARCHAR2, p_start_date IN DATE, p_end_date IN DATE) IS
    v_voice_secs   NUMBER := 0;
    v_data_mb      NUMBER := 0;
    v_sms_count    NUMBER := 0;
    v_plan_id      NUMBER;
    v_total_amount NUMBER := 0;
    v_subscriber_id NUMBER;
    v_error_message VARCHAR2(4000);
  BEGIN
    -- Aggregate usage
    SELECT NVL(SUM(call_duration_sec), 0)
    INTO v_voice_secs
    FROM call_detail_record
    WHERE subscriber_msisdn = p_msisdn
      AND call_type = 'VOICE'
      AND call_start_time BETWEEN p_start_date AND p_end_date;

    SELECT NVL(SUM(call_duration_sec), 0)
    INTO v_data_mb
    FROM call_detail_record
    WHERE subscriber_msisdn = p_msisdn
      AND call_type = 'DATA'
      AND call_start_time BETWEEN p_start_date AND p_end_date;

    SELECT COUNT(*)
    INTO v_sms_count
    FROM call_detail_record
    WHERE subscriber_msisdn = p_msisdn
      AND call_type = 'SMS'
      AND call_start_time BETWEEN p_start_date AND p_end_date;

    -- Get plan 
    SELECT plan_id INTO v_plan_id
    FROM (
      SELECT sp.plan_id, 
          RANK() OVER (PARTITION BY sp.subscriber_msisdn ORDER BY sp.subscriber_msisdn, plan_start_date DESC, plan_end_date ASC) rnk
      FROM subscriber_plan  sp
      WHERE subscriber_msisdn = p_msisdn
      ) a
    WHERE rnk = 1;

    -- Get subscriber 
    SELECT subscriber_id INTO v_subscriber_id FROM subscriber WHERE msisdn = p_msisdn;

    -- Compute total
    v_total_amount := compute_usage_charges(v_voice_secs, v_data_mb, v_sms_count, v_plan_id);

    -- Insert billing record
    INSERT INTO invoice (
      subscriber_id, billing_period_start,billing_period_end,
      total_amount_due, generated_date, due_date, status, created_at, created_by
    ) VALUES (
      v_subscriber_id, p_start_date, p_end_date, v_total_amount,
      SYSDATE, (p_end_date + 14), 'UNPAID', SYSDATE, 'SYSTEM'
    );

    -- Email notification
    notifications_prc(
          v_subscriber_id,
          'Billing alert', 
          SYSDATE,
          'SENT',
          'EMAIL', 
          'Your bill for ' || TO_CHAR(p_start_date, 'Month YYYY') || 
          ' is R' || TO_CHAR(v_total_amount, '999,990.00') ||
          '. Due date: ' || TO_CHAR(p_end_date + 14, 'DD-Mon-YYYY')
        );

    COMMIT;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
        v_error_message := 'Subscriber or plan not found for MSISDN: ' || p_msisdn;
        log_error(p_process => 'PKG_BILLING.CALCULATE_CHARGES',
                        p_affected_table => 'INVOCE',
                        p_error_time => SYSTIMESTAMP,
                        p_error_message => v_error_message,
                        p_source_file => 'generate_monthly_bills for ' || v_subscriber_id);
        RAISE_APPLICATION_ERROR(-20003, v_error_message);
    WHEN OTHERS THEN
        v_error_message := SQLERRM;
        log_error(p_process => 'PKG_BILLING.CALCULATE_CHARGES',
                        p_affected_table => 'INVOCE',
                        p_error_time => SYSTIMESTAMP,
                        p_error_message => v_error_message,
                        p_source_file => 'generate_monthly_bills for ' || v_subscriber_id);
        ROLLBACK;
        RAISE;
  END calculate_charges;

  PROCEDURE generate_monthly_bills(p_billing_date IN DATE) IS
    CURSOR c_subs IS
      SELECT msisdn FROM subscriber WHERE status = 'ACTIVE'; 
          v_start_date DATE := TRUNC(p_billing_date, 'MM');
          v_end_date   DATE := LAST_DAY(p_billing_date);
          v_batch_size NUMBER := 100; -- Process in batches
          v_counter    NUMBER := 0;
      BEGIN
        FOR r IN c_subs LOOP
          calculate_charges(r.msisdn, v_start_date, v_end_date);
          v_counter := v_counter + 1;

          -- Commit every batch_size records
          IF MOD(v_counter, v_batch_size) = 0 THEN
            COMMIT;
          END IF;
        END LOOP;
        COMMIT; -- Final commit
      END;

  FUNCTION compute_usage_charges(
    p_call_secs IN NUMBER,
    p_data_mb   IN NUMBER,
    p_sms_count IN NUMBER,
    p_plan_id   IN NUMBER
  ) RETURN NUMBER IS
    v_rate_voice NUMBER;
    v_rate_data  NUMBER;
    v_rate_sms   NUMBER;
    v_total      NUMBER := 0;
  BEGIN
    -- Lookup rates from tariff table
    SELECT call_rate_per_minute, data_rate_per_mb, sms_rate_per_message
    INTO v_rate_voice, v_rate_data, v_rate_sms
    FROM tariff_plan
    WHERE plan_id = p_plan_id;

    v_total := ROUND((CEIL(p_call_secs / 60) * v_rate_voice)
             + (p_data_mb * v_rate_data)
             + (p_sms_count * v_rate_sms),2);

    RETURN v_total;
  END;

  PROCEDURE record_payment(
    p_invoice_id IN NUMBER,
    p_payment_date IN DATE DEFAULT SYSDATE,
    p_payment_amount IN NUMBER,
    p_payment_method IN VARCHAR2
  )
  IS
    v_invoice_amount NUMBER;
    v_invoice_status VARCHAR2(20);
    v_subscriber_id NUMBER;
  BEGIN
    -- Get invoice details first
      SELECT total_amount_due, status 
      INTO v_invoice_amount, v_invoice_status
      FROM invoice
      WHERE invoice_id = p_invoice_id;

      -- Validate payment
      IF v_invoice_status = 'PAID' THEN
        RAISE_APPLICATION_ERROR(-20004, 'Invoice is not in UNPAID status');
      END IF;

      --Get subscriber ID
      SELECT subscriber_id INTO v_subscriber_id 
      FROM INVOICE
      WHERE invoice_id = p_invoice_id;

      -- Record payment
      INSERT INTO payment (
        invoice_id,
        payment_date,
        payment_amount,
        payment_method,
        reference_number,
        created_at,
        created_by
      )
      VALUES (
        p_invoice_id,
        p_payment_date,
        p_payment_amount,
        p_payment_method,
        'REF-' || p_invoice_id,
        SYSTIMESTAMP,
        USER
      );

    -- Update invoice status
    UPDATE invoice
      SET status = CASE 
                    WHEN p_payment_amount >= v_invoice_amount THEN 'PAID'
                    ELSE 'PARTIALLY_PAID'
                  END,
          total_amount_due = total_amount_due - p_payment_amount,
          updated_at = SYSTIMESTAMP,
          updated_by = USER
      WHERE invoice_id = p_invoice_id;

      notifications_prc(v_subscriber_id,'Payment alert',SYSDATE,'SENT','SMS', 
              'Payment received: R' || TO_CHAR(p_payment_amount, '9990.00') || ' Thank you :)');
      COMMIT;
  END record_payment;
END pkg_billing;