
-- создание пакета, который хранит константы
CREATE OR REPLACE PACKAGE CONSTANTS AS
	-- названия уровней staging dwh
	dwi_title_lvl CONSTANT VARCHAR2(3 CHAR) := 'dwi';
	dws_title_lvl CONSTANT VARCHAR2(3 CHAR) := 'dws';
	stg_title_lvl CONSTANT VARCHAR2(3 CHAR) := 'stg';
END;

-- тест
BEGIN
	dbms_output.put_line(CONSTANTS.stg_title_lvl);
END;
