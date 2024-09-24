-- значения для словаря
CREATE OR REPLACE TYPE mike.VALUE_DICT IS TABLE OF varchar2(100 char);
-- словарь
CREATE OR REPLACE TYPE mike.MYDICT IS TABLE OF varchar2(100 char) INDEX BY varchar2(100 char);

CREATE OR REPLACE TYPE mike.MYDICT IS TABLE OF VARCHAR2(100) INDEX BY VARCHAR2(100)

DECLARE 
	v_d VALUE_DICT := VALUE_DICT();
	--d MYDICT := MYDICT();
BEGIN
	v_d.extend(1);
	v_d(1) := '1';
	dbms_output.put_line(v_d(1));
END;