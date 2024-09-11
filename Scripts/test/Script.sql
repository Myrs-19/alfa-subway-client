DECLARE
    FILE_DIR     VARCHAR2(100);
    FILE_NAME    VARCHAR2(100); 
    FILE_CONTENT VARCHAR2(4000);
    FILE         UTL_FILE.FILE_TYPE;
BEGIN
    FILE_DIR  := '/tmp/utl_file_dir';
    FILE_NAME := 'my_file.txt';
	
   FILE := UTL_FILE.FOPEN(FILE_DIR, FILE_NAME, 'R', 32767);
--   
--    BEGIN
--       FILE := UTL_FILE.FOPEN(FILE_DIR, FILE_NAME, 'R', 32767);
--    EXCEPTION 
--       WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE('(*) Open_file_error : Unable to open the file.');
--    END; 
--	
--    BEGIN
--       LOOP 
--          BEGIN 
--             UTL_FILE.GET_LINE(FILE, FILE_CONTENT);
--             DBMS_OUTPUT.PUT_LINE(FILE_CONTENT);
--          -- End of the file or Others error
--          EXCEPTION 
--             WHEN OTHERS THEN EXIT;
--          END;
--       END LOOP;
--    EXCEPTION 
--       WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE('(*) Read_file_error : Unable to read the file.');	
--    END;

    UTL_FILE.FCLOSE(FILE);	
END;

select owner, object_type from all_objects where object_name = 'UTL_FILE' ;


