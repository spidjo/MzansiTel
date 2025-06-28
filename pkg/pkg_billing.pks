create or replace PACKAGE pkg_billing IS
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
  -- Calculates charges for a single subscriber for a billing period
  PROCEDURE calculate_charges(p_msisdn IN VARCHAR2, p_start_date IN DATE, p_end_date IN DATE);

  -- Batch calculates billing for all active subscribers
  PROCEDURE generate_monthly_bills(p_billing_date IN DATE);

  -- Applies business rules and tariffs to usage
  FUNCTION compute_usage_charges(
    p_call_secs IN NUMBER,
    p_data_mb   IN NUMBER,
    p_sms_count IN NUMBER,
    p_plan_id   IN NUMBER
  ) RETURN NUMBER;

  PROCEDURE record_payment(
    p_subscriber_id IN NUMBER,
    p_invoice_id IN NUMBER,
    p_payment_date IN DATE DEFAULT SYSDATE,
    p_payment_amount IN NUMBER,
    p_payment_method IN VARCHAR2
  );
  -- Archives billed CDRs after processing
--   PROCEDURE archive_cdrs(p_billing_date IN DATE);
END pkg_billing;