
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

END;

-- тест
--BEGIN
--	dbms_output.put_line(mike.CONSTANTS.table_source_title_3nf_hp);
--END;
