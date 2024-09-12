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