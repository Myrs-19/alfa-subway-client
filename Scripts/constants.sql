
-- создание пакета, который хранит константы
CREATE OR REPLACE PACKAGE mike.CONSTANTS AS
	
	-- названия уровней staging dwh
	dwi_title_lvl CONSTANT VARCHAR2(3 CHAR) := 'dwi';
	dws_title_lvl CONSTANT VARCHAR2(3 CHAR) := 'dws';
	stg_title_lvl CONSTANT VARCHAR2(3 CHAR) := 'stg';

	
	-- названия таблиц-источников
	table_source_title_star_clnt VARCHAR2(256 CHAR) := 'star_client';
	
	table_source_title_3nf_pi VARCHAR2(256 CHAR) := 'personal_information';
	table_source_title_3nf_pn VARCHAR2(256 CHAR) := 'phone_numbers';
	table_source_title_3nf_docs VARCHAR2(256 CHAR) := 'documents';
	table_source_title_3nf_hp VARCHAR2(256 CHAR) := 'human_params';

	-- название измерения
	dimension_client_title VARCHAR2(256 CHAR) := 'client';

END;

-- тест
/*

BEGIN
	dbms_output.put_line(mike.CONSTANTS.dwi_title_lvl);
END;

*/

DECLARE 
l_id_job NUMBER;
BEGIN
SELECT nvl(min(j_n), -1)
			INTO l_id_job
			FROM (
				SELECT job_number j_n
				FROM mike.orchestrator_alfa
				WHERE staging_lvl = mike.CONSTANTS.dwi_title_lvl AND is_successful = 1 AND need_process = 1
				MINUS 
				SELECT job_number j_n
				FROM mike.orchestrator_alfa
				WHERE staging_lvl = mike.CONSTANTS.dws_title_lvl
			);
END;