CREATE OR REPLACE PROCEDURE mike.t_e
IS 
	error_message VARCHAR2(256 CHAR);
BEGIN 
	BEGIN
		mike.t_ee();
	EXCEPTION
	   	WHEN OTHERS THEN
	   		error_message := SUBSTR(SQLERRM, 1, 256);
	   		dbms_output.put_line(error_message);
	END;
END;


CREATE OR REPLACE PROCEDURE mike.t_ee
IS 
	x real;
BEGIN 
	SELECT 1/0
	INTO x
	FROM dual;
END;


BEGIN
	mike.t_e();
END;



SELECT *
FROM dual d
GROUP BY 

SELECT *
FROM dual
WHERE (1,2) = (1,2);

DECLARE
j NUMBER;
BEGIN
	SELECT min(j_n) INTO j
	FROM (
		SELECT job_number j_n
		FROM mike.orchestrator_alfa
		WHERE staging_lvl = mike.CONSTANTS.dwi_title_lvl AND is_successful = 1 AND need_process = 1
		MINUS 
		SELECT job_number j_n
		FROM mike.orchestrator_alfa
		WHERE staging_lvl = mike.CONSTANTS.dws_title_lvl
	);

	dbms_output.put_line(j);
END;
