create or replace PACKAGE pkg_file_utils AS
/*
 * File Utilities Package
 * 
 * This package provides utility procedures for file operations in the database.
 * It handles common file management tasks such as archiving files between directories.
 *
 * Author: Siphiwo Lumkwana
 */
 
  /**
   * Archives a file by moving it from a source directory to a target archive directory.
   * 
   * @param p_file_name  Name of the file to be archived (including extension if applicable)
   * @param p_source_dir Source directory object name where the file currently resides.
   *                     Defaults to 'DATA_DIR' if not specified.
   * @param p_target_dir Target directory object name where the file will be archived.
   *                     Defaults to 'ARCHIVE_DIR' if not specified.
   *
   * Usage Example:
   * BEGIN
   *   pkg_file_utils.archive_file('report_20230501.csv');
   * END;
   *
   * Notes:
   * - Both directory objects must be created and accessible to the executing user
   * - The user must have appropriate read/write privileges on both directories
   * - The procedure will overwrite existing files in the target directory with the same name
   */
  PROCEDURE archive_file(
    p_file_name   VARCHAR2,
    p_source_dir  VARCHAR2 := 'DATA_DIR',
    p_target_dir  VARCHAR2 := 'ARCHIVE_DIR'
  );

END pkg_file_utils;