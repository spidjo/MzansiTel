create or replace PACKAGE BODY pkg_file_utils AS
  v_error_message VARCHAR2(4000);
  PROCEDURE archive_file(
    p_file_name   VARCHAR2,
    p_source_dir  VARCHAR2 := 'DATA_DIR',
    p_target_dir  VARCHAR2 := 'ARCHIVE_DIR'
  ) IS
  BEGIN
    -- Copy file to archive
    UTL_FILE.FCOPY(
      src_location  => p_source_dir,
      src_filename  => p_file_name,
      dest_location => p_target_dir,
      dest_filename => p_file_name
    );

    -- Delete original
    UTL_FILE.FREMOVE(
      location => p_source_dir,
      filename => p_file_name
    );

    -- Optional logging
    INSERT INTO file_archive_log (
      file_name,
      source_dir,
      target_dir,
      archive_timestamp
    )
    VALUES (
      p_file_name,
      p_source_dir,
      p_target_dir,
      SYSTIMESTAMP
    );
    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      v_error_message := SQLERRM;
      -- Handle and log error
      INSERT INTO file_archive_log (
        file_name,
        source_dir,
        target_dir,
        archive_timestamp,
        error_message
      )
      VALUES (
        p_file_name,
        p_source_dir,
        p_target_dir,
        SYSTIMESTAMP,
        v_error_message
      );
      COMMIT;
      RAISE;
  END archive_file;

END pkg_file_utils;