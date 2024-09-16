-- создание пакета dws
CREATE OR REPLACE PACKAGE mike.dws AS
	-- процедура обертка над всем уровнем dws
	-- в ней генерится номер джоба и передается на след уровни обработки в рамках интерфейсного staging уровня
	-- она же имеет джоб, который запускается каждые 5 минут
	PROCEDURE wrap_dws;

	---------------- таблица clnt, источник звезда ----------------
	
	-- обертка для таблицы-источника clnt
	PROCEDURE wrap_clnt_001_dws(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура очистки дублей таблицы clnt 
	PROCEDURE clnt_001_dws_double(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура преобразования pk в nk таблицы clnt
	PROCEDURE clnt_001_dws_pk_to_nk(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура для формировании delta таблицы clnt
	PROCEDURE clnt_001_dws_delta(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура для актуализации mirror таблицы clnt
	PROCEDURE clnt_001_dws_mirror(
		p_id_job NUMBER -- номер джоба
	);

	---------------- таблица PI, источник 3НФ ----------------

	-- обертка для таблицы-источника PI
	PROCEDURE wrap_PI_002_dws(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура очистки дублей таблицы PI 
	PROCEDURE PI_002_dws_double(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура преобразования pk в nk таблицы PI
	PROCEDURE PI_002_dws_pk_to_nk(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура для формировании delta таблицы PI
	PROCEDURE PI_002_dws_delta(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура для актуализации mirror таблицы PI
	PROCEDURE PI_002_dws_mirror(
		p_id_job NUMBER -- номер джоба
	);

	---------------- таблица PN, источник 3НФ ----------------

	-- обертка для таблицы-источника PN
	PROCEDURE wrap_PN_002_dws(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура очистки дублей таблицы PN 
	PROCEDURE PN_002_dws_double(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура преобразования pk в nk таблицы PN
	PROCEDURE PN_002_dws_pk_to_nk(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура для формировании delta таблицы PN
	PROCEDURE PN_002_dws_delta(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура для актуализации mirror таблицы PN
	PROCEDURE PN_002_dws_mirror(
		p_id_job NUMBER -- номер джоба
	);

	---------------- таблица docs, источник 3НФ ----------------

	-- обертка для таблицы-источника docs
	PROCEDURE wrap_docs_002_dws(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура очистки дублей таблицы docs 
	PROCEDURE docs_002_dws_double(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура преобразования pk в nk таблицы docs
	PROCEDURE docs_002_dws_pk_to_nk(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура для формировании delta таблицы docs
	PROCEDURE docs_002_dws_delta(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура для актуализации mirror таблицы docs
	PROCEDURE docs_002_dws_mirror(
		p_id_job NUMBER -- номер джоба
	);

	---------------- таблица hp, источник 3НФ ----------------

	-- обертка для таблицы-источника hp
	PROCEDURE wrap_hp_002_dws(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура очистки дублей таблицы hp 
	PROCEDURE hp_002_dws_double(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура преобразования pk в nk таблицы hp
	PROCEDURE hp_002_dws_pk_to_nk(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура для формировании delta таблицы hp
	PROCEDURE hp_002_dws_delta(
		p_id_job NUMBER -- номер джоба
	);

	-- процедура для актуализации mirror таблицы hp
	PROCEDURE hp_002_dws_mirror(
		p_id_job NUMBER -- номер джоба
	);
END;