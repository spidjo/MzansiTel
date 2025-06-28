create or replace PACKAGE pkg_load_core IS
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
====================================================================================================
*/

    -- Load Procedures
    PROCEDURE p_load_subscriber;
    PROCEDURE p_load_tariff_plan;
    PROCEDURE p_load_subscriber_plan;
    PROCEDURE p_load_cdr;

    -- Main Control Procedure
    PROCEDURE p_load_all;

    -- Utility
    PROCEDURE p_truncate_staging;

END pkg_load_core;