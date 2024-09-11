
-- создание пакета, который хранит константы
CREATE OR REPLACE PACKAGE CONSTANTS AS
	dwi_title_lvl CONSTANT VARCHAR2 := 'dwi';
	dws_title_lvl CONSTANT VARCHAR2 := 'dws';
	stg_title_lvl CONSTANT VARCHAR2 := 'stg';
END;

