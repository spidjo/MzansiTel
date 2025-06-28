-- Triggers for audit/history tables
CREATE OR REPLACE TRIGGER trg_audit_subscriber_insert_update
AFTER INSERT OR UPDATE ON subscriber
FOR EACH ROW
DECLARE
    v_change_type VARCHAR2(20);
BEGIN
    IF INSERTING THEN
        v_change_type := 'INSERT';
    ELSIF UPDATING THEN
        v_change_type := 'UPDATE';
    END IF;

    INSERT INTO subscriber_hist (
        subscriber_id,
        msisdn,
        first_name,
        last_name,
        date_of_birth,
        email_address,
        registration_date,
        status,
        change_type,
        change_timestamp,
        changed_by
    )
    VALUES (
        :NEW.subscriber_id,
        :NEW.msisdn,
        :NEW.first_name,
        :NEW.last_name,
        :NEW.date_of_birth,
        :NEW.email_address,
        :NEW.registration_date,
        :NEW.status,
        v_change_type,
        SYSTIMESTAMP,
        USER
    );
END;
/

CREATE OR REPLACE TRIGGER trg_audit_subscriber_delete
AFTER DELETE ON subscriber
FOR EACH ROW
BEGIN
    INSERT INTO subscriber_hist (
        subscriber_id,
        msisdn,
        first_name,
        last_name,
        date_of_birth,
        email_address,
        registration_date,
        status,
        change_type,
        change_timestamp,
        changed_by
    )
    VALUES (
        :OLD.subscriber_id,
        :OLD.msisdn,
        :OLD.first_name,
        :OLD.last_name,
        :OLD.date_of_birth,
        :OLD.email_address,
        :OLD.registration_date,
        :OLD.status,
        'DELETE',
        SYSTIMESTAMP,
        USER
    );
END;
/

CREATE OR REPLACE TRIGGER trg_audit_subscriber_plan_insert_update
AFTER INSERT OR UPDATE ON subscriber_plan
FOR EACH ROW
DECLARE
    v_change_type VARCHAR2(20);
BEGIN
    IF INSERTING THEN
        v_change_type := 'INSERT';
    ELSIF UPDATING THEN
        v_change_type := 'UPDATE';
    END IF;

    INSERT INTO subscriber_plan_hist (
        subscriber_plan_id,
        subscriber_msisdn,
        plan_id,
        plan_start_date,
        plan_end_date,
        change_type,
        change_timestamp,
        changed_by
    )
    VALUES (
        :NEW.subscriber_plan_id,
        :NEW.subscriber_msisdn,
        :NEW.plan_id,
        :NEW.plan_start_date,
        :NEW.plan_end_date,
        v_change_type,
        SYSTIMESTAMP,
        USER
    );
END;
/

CREATE OR REPLACE TRIGGER trg_audit_subscriber_plan_delete
AFTER DELETE ON subscriber_plan
FOR EACH ROW
BEGIN
    INSERT INTO subscriber_plan_hist (
        subscriber_plan_id,
        subscriber_msisdn,
        plan_id,
        plan_start_date,
        plan_end_date,
        change_type,
        change_timestamp,
        changed_by
    )
    VALUES (
        :OLD.subscriber_plan_id,
        :OLD.subscriber_msisdn,
        :OLD.plan_id,
        :OLD.plan_start_date,
        :OLD.plan_end_date,
        'DELETE',
        SYSTIMESTAMP,
        USER
    );
END;
/

CREATE OR REPLACE TRIGGER trg_audit_cdr_insert_update
AFTER INSERT OR UPDATE ON call_detail_record
FOR EACH ROW
DECLARE
    v_change_type VARCHAR2(20);
BEGIN
    IF INSERTING THEN
        v_change_type := 'INSERT';
    ELSIF UPDATING THEN
        v_change_type := 'UPDATE';
    END IF;

    INSERT INTO call_detail_record_hist (
        cdr_id,
        subscriber_msisdn,
        call_type,
        call_start_time,
        call_end_time,
        call_duration_sec,
        destination_number,
        call_cost,
        call_direction,
        change_type,
        change_timestamp,
        changed_by
    )
    VALUES (
        :NEW.cdr_id,
        :NEW.subscriber_msisdn,
        :NEW.call_type,
        :NEW.call_start_time,
        :NEW.call_end_time,
        :NEW.call_duration_sec,
        :NEW.destination_number,
        :NEW.call_cost,
        :NEW.call_direction,
        v_change_type,
        SYSTIMESTAMP,
        USER
    );
END;
/

CREATE OR REPLACE TRIGGER trg_audit_cdr_delete
AFTER DELETE ON call_detail_record
FOR EACH ROW
BEGIN
    INSERT INTO call_detail_record_hist (
        cdr_id,
        subscriber_msisdn,
        call_type,
        call_start_time,
        call_end_time,
        call_duration_sec,
        destination_number,
        call_cost,
        call_direction,
        change_type,
        change_timestamp,
        changed_by
    )
    VALUES (
        :OLD.cdr_id,
        :OLD.subscriber_msisdn,
        :OLD.call_type,
        :OLD.call_start_time,
        :OLD.call_end_time,
        :OLD.call_duration_sec,
        :OLD.destination_number,
        :OLD.call_cost,
        :OLD.call_direction,
        'DELETE',
        SYSTIMESTAMP,
        USER
    );
END;
/

CREATE OR REPLACE TRIGGER trg_audit_invoice_insert_update
AFTER INSERT OR UPDATE ON invoice
FOR EACH ROW
DECLARE
    v_change_type VARCHAR2(20);
BEGIN
    IF INSERTING THEN
        v_change_type := 'INSERT';
    ELSIF UPDATING THEN
        v_change_type := 'UPDATE';
    END IF;

    INSERT INTO invoice_hist (
        invoice_id,
        subscriber_id,
        billing_period_start,
        billing_period_end,
        total_amount_due,
        generated_date,
        due_date,
        status,
        change_type,
        change_timestamp,
        changed_by
    )
    VALUES (
        :NEW.invoice_id,
        :NEW.subscriber_id,
        :NEW.billing_period_start,
        :NEW.billing_period_end,
        :NEW.total_amount_due,
        :NEW.generated_date,
        :NEW.due_date,
        :NEW.status,
        v_change_type,
        SYSTIMESTAMP,
        USER
    );
END;
/

CREATE OR REPLACE TRIGGER trg_audit_invoice_delete
AFTER DELETE ON invoice
FOR EACH ROW
BEGIN
    INSERT INTO invoice_hist (
        invoice_id,
        subscriber_id,
        billing_period_start,
        billing_period_end,
        total_amount_due,
        generated_date,
        due_date,
        status,
        change_type,
        change_timestamp,
        changed_by
    )
    VALUES (
        :OLD.invoice_id,
        :OLD.subscriber_id,
        :OLD.billing_period_start,
        :OLD.billing_period_end,
        :OLD.total_amount_due,
        :OLD.generated_date,
        :OLD.due_date,
        :OLD.status,
        'DELETE',
        SYSTIMESTAMP,
        USER
    );
END;
/

CREATE OR REPLACE TRIGGER trg_audit_payment_insert_update
AFTER INSERT OR UPDATE ON payment
FOR EACH ROW
DECLARE
    v_change_type VARCHAR2(20);
BEGIN
    IF INSERTING THEN
        v_change_type := 'INSERT';
    ELSIF UPDATING THEN
        v_change_type := 'UPDATE';
    END IF;

    INSERT INTO payment_hist (
        payment_id,
        invoice_id,
        payment_date,
        payment_amount,
        payment_method,
        reference_number,
        change_type,
        change_timestamp,
        changed_by
    )
    VALUES (
        :NEW.payment_id,
        :NEW.invoice_id,
        :NEW.payment_date,
        :NEW.payment_amount,
        :NEW.payment_method,
        :NEW.reference_number,
        v_change_type,
        SYSTIMESTAMP,
        USER
    );
END;
/

CREATE OR REPLACE TRIGGER trg_audit_payment_delete
AFTER DELETE ON payment
FOR EACH ROW
BEGIN
    INSERT INTO payment_hist (
        payment_id,
        invoice_id,
        payment_date,
        payment_amount,
        payment_method,
        reference_number,
        change_type,
        change_timestamp,
        changed_by
    )
    VALUES (
        :OLD.payment_id,
        :OLD.invoice_id,
        :OLD.payment_date,
        :OLD.payment_amount,
        :OLD.payment_method,
        :OLD.reference_number,
        'DELETE',
        SYSTIMESTAMP,
        USER
    );
END;
/
