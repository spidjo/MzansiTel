create or replace PACKAGE pkg_load_staging IS
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
====================================================================================================
*/



  -- Loads CDR data from external table into staging table
  PROCEDURE load_cdr_data;

  -- Loads subscriber data from external table
  PROCEDURE load_subscriber_data;

  -- Loads tariff data
  PROCEDURE load_tariff_data; 

  PROCEDURE load_subscriber_plan_data;

  PROCEDURE load_all(clear_tables VARCHAR2 DEFAULT 'NO');

  PROCEDURE log_processing_error(
    p_process VARCHAR2,
    p_affected_table VARCHAR2,
    p_error_time TIMESTAMP,
    p_error_message VARCHAR2,
    p_raw_record VARCHAR2,
    p_source_file VARCHAR2
  );

  PROCEDURE init_variables;

END pkg_load_staging;