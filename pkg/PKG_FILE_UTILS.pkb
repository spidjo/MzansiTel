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

--   PROCEDURE archive_all_files(
--     p_source_dir  VARCHAR2 := 'DATA_DIR',
--     p_target_dir  VARCHAR2 := 'ARCHIVE_DIR'
--   ) IS
--     l_file_list    SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
--     l_filename     VARCHAR2(255);
--     l_file         UTL_FILE.FILE_TYPE;
--     l_dir          VARCHAR2(255) := p_source_dir;
--   BEGIN
    -- Use DBMS_LOB and DBMS_BACKUP_RESTORE or external scheduler to fetch filenames
    -- For simplicity, assume filenames are known or pulled from a control table.

--     FOR r IN (
--       SELECT file_name
--       FROM file_load_log
--       WHERE processed = 'Y' AND archived = 'N'
--     )
--     LOOP
--       BEGIN
--         archive_file(r.file_name, p_source_dir, p_target_dir);
--         UPDATE file_load_log
--         SET archived = 'Y'
--         WHERE file_name = r.file_name;
--         COMMIT;
--       EXCEPTION
--         WHEN OTHERS THEN
--           -- Log handled in archive_file
--           NULL;
--       END;
--     END LOOP;

--   END archive_all_files;

END pkg_file_utils;