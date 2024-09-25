-- создание пакета stg
CREATE OR REPLACE PACKAGE mike.stg AS
	-- процедура обертка над всем уровнем stg
	-- в ней генерится номер джоба и передается на след уровни обработки в рамках staging уровня
	-- она же имеет джоб, который запускается каждые 5 минут
	PROCEDURE wrap_stg;

	-- обертка для маппинга
	PROCEDURE wrap_mapping(
		p_id_job NUMBER -- номер джоба
	);

	-- маппинг источника с кодом 001
	PROCEDURE mapping_001(
		p_id_job NUMBER -- номер джоба
	);

	-- маппинг источника с кодом 002
	PROCEDURE mapping_002(
		p_id_job NUMBER -- номер джоба
	);

	-- обертка на унификации записи и создание контекста
	PROCEDURE wrap_ucontext(
		p_id_job NUMBER -- номер джоба
	);

	-- унификация записи
	-- здесь генерится новые uk
	PROCEDURE uni_record(
		p_id_job NUMBER -- номер джоба
	);

	-- создание контекста
	PROCEDURE context(
		p_id_job NUMBER -- номер джоба
	);

	-- унификация атрибутов и создание контексной дельты
	PROCEDURE uwdelta(
		p_id_job NUMBER -- номер джоба
	);

	-- создание вьюхи типа I из таблицы с типами унификации для каждого атрибута
	-- для типа унификации атрибута - агрегация
	PROCEDURE create_view_wdelta_A_I(
		p_id_job NUMBER -- номер джоба
	);

	-- создание вьюхи типа U из таблицы с типами унификации для каждого атрибута
	-- для типа унификации атрибута - агрегация
	PROCEDURE create_view_wdelta_A_U(
		p_id_job NUMBER -- номер джоба
	);

	-- создание вьюхи типа I из таблицы с типами унификации для каждого атрибута
	-- для типа унификации атрибута - приоритет
	PROCEDURE create_view_wdelta_P_I(
		p_id_job NUMBER -- номер джоба
	);

	-- создание вьюхи типа U из таблицы с типами унификации для каждого атрибута
	-- для типа унификации атрибута - приоритет
	PROCEDURE create_view_wdelta_P_U(
		p_id_job NUMBER -- номер джоба
	);
END;