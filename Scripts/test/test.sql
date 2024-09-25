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



DECLARE
	c NUMBER;
BEGIN
			SELECT nvl(min(j_n), -1)
			INTO c
			FROM (
				SELECT job_number j_n
				FROM mike.orchestrator_alfa
				WHERE staging_lvl = mike.CONSTANTS.dws_title_lvl 
					AND is_successful = 1 
					AND need_process = 1
				MINUS 
				SELECT job_number j_n
				FROM mike.orchestrator_alfa
				WHERE staging_lvl = 'mike.CONSTANTS.stg_title_lvl '
					AND is_successful = 1
			);
		
		dbms_output.put_line(c);
END;

MERGE INTO STG.CLIENT_UKLINK uklink
USING (
	SELECT *
	FROM STG.CLIENT_CDELTA cdelta
	WHERE dwsuniact = 'U'
) cdelta
ON (
	cdelta.uk = uklink.uk
)
WHEN MATCHED THEN
UPDATE SET 
	uk = mike.key_dwh.nextval;


SELECT context.uk uk,  max(context.ID) ID, max(context.NAME) NAME, max(context.BIRTHDAY) BIRTHDAY, max(context.AGE) AGE, max(context.INN) INN, max(context.PASPORT) PASPORT, max(context.WEIGHT) WEIGHT, max(context.HEIGHT) HEIGHT 
FROM STG.CLIENT_CONTEXT context
LEFT JOIN STG.CLIENT_WDELTA wdelta
	ON context.uk = wdelta.uk
WHERE wdelta.uk IS NULL
GROUP BY context.uk


DECLARE
	p_id_job NUMBER := 1;-- номер джоба
		-- курсор, который проходит по записям из таблицы с типами унификации 
		-- для каждого атрибута, у которых тип унификации = 'A' - агрегация
		c SYS_REFCURSOR;
		-- название атрибута
		l_attr varchar2(128 char);
		-- тип унификация для этого атрибута
		l_uni_type_attr varchar2(128 char);
	
		-- заголовок запроса для создания вьюхи
		view_create_header varchar2(256 char) := 'CREATE OR REPLACE VIEW mike.v_wdelta_I AS ';
	
		-- список столбцов в селекте
		list_select varchar2(1024 char) := 'SELECT context.uk uk, ';
		-- статичное тело селекта
		body_select varchar2(2048 char) := '
FROM STG.CLIENT_CONTEXT context
LEFT JOIN STG.CLIENT_WDELTA wdelta
	ON context.uk = wdelta.uk
WHERE wdelta.uk IS NULL
GROUP BY context.uk';
		-- итоговые сформированный селект
		result_select varchar2(2048 char);

		-- итоговые сформированный запрос для создание вьюхи
		result_create_view varchar2(2048 char);
	BEGIN
		-- открываем курсор по таблице с типами унификации
		OPEN c FOR SELECT attr, uni_type_attr FROM mike.uni_attrs WHERE uni_type_attr = 'A';
	
		mike.logs.log(1, 'курсор открыт', CONSTANTS.stg_title_lvl, 'create_view_wdelta_A_I', p_id_job);
	
		-- проходим по таблицу и формируем столбцы в select 
		LOOP
			FETCH c INTO l_attr, l_uni_type_attr;
			EXIT WHEN c%NOTFOUND;
		
			list_select := list_select || ' max(context.' || l_attr || ') ' || l_attr || ',';
		END LOOP;
	
		mike.logs.log(2, 'сформирован список столбцов для селекта', CONSTANTS.stg_title_lvl, 'create_view_wdelta_A_I', p_id_job);
	
		-- убираем последнюю запятую
		list_select := regexp_replace(list_select, ',$', '');
	
		-- склеиваем список столбцов с телом запроса селекта
		result_select := list_select || ' ' || body_select;
	
		mike.logs.log(3, 'сформирован селект', CONSTANTS.stg_title_lvl, 'create_view_wdelta_A_I', p_id_job);
	
		-- формируем запрос на создание вьюхи
		result_create_view := view_create_header || ' ' || result_select;
	
		
		mike.logs.log(4, 'сформирован итоговый запрос на создание вьюхи', CONSTANTS.stg_title_lvl, 'create_view_wdelta_A_I', p_id_job);
	
		-- создаем вьюху
		EXECUTE IMMEDIATE result_create_view;
		
		mike.logs.log(5, 'запрос выполнен', CONSTANTS.stg_title_lvl, 'create_view_wdelta_A_I', p_id_job);
	END;
	


-- создание вьюхи из таблицы с типами унификации для каждого атрибута
	CREATE OR REPLACE PROCEDURE create_view_wdelta_A_I(
		p_id_job NUMBER -- номер джоба
	)
	IS
		-- курсор, который проходит по записям из таблицы с типами унификации 
		-- для каждого атрибута, у которых тип унификации = 'A' - агрегация
		CURSOR c IS 
			SELECT attr, uni_type_attr 
			FROM mike.uni_attrs 
			WHERE uni_type_attr = 'A';
		-- название атрибута
		l_attr varchar2(128 char);
		-- тип унификация для этого атрибута
		l_uni_type_attr varchar2(128 char);
	
		-- заголовок запроса для создания вьюхи
		view_create_header varchar2(256 char) := 'CREATE OR REPLACE VIEW mike.v_wdelta_I AS ';
	
		-- список столбцов в селекте
		list_select varchar2(1024 char) := 'SELECT context.uk uk, ';
		-- статичное тело селекта
		body_select varchar2(2048 char) := '
FROM STG.CLIENT_CONTEXT context
LEFT JOIN STG.CLIENT_WDELTA wdelta
	ON context.uk = wdelta.uk
WHERE wdelta.uk IS NULL
GROUP BY context.uk';
		-- итоговые сформированный селект
		result_select varchar2(2048 char);

		-- итоговые сформированный запрос для создание вьюхи
		result_create_view varchar2(2048 char);
	BEGIN
		-- открываем курсор по таблице с типами унификации
		OPEN c;

		mike.logs.log(1, 'курсор открыт', CONSTANTS.stg_title_lvl, 'create_view_wdelta_A_I', p_id_job);

		-- проходим по таблицу и формируем столбцы в select 
		LOOP
			FETCH c INTO l_attr, l_uni_type_attr;
			EXIT WHEN c%NOTFOUND;
		
			list_select := list_select || ' max(context.' || l_attr || ') ' || l_attr || ',';
		END LOOP;

		mike.logs.log(2, 'сформирован список столбцов для селекта', CONSTANTS.stg_title_lvl, 'create_view_wdelta_A_I', p_id_job);

		-- убираем последнюю запятую
		list_select := regexp_replace(list_select, ',$', '');

		-- склеиваем список столбцов с телом запроса селекта
		result_select := list_select || ' ' || body_select;

		mike.logs.log(3, 'сформирован селект', CONSTANTS.stg_title_lvl, 'create_view_wdelta_A_I', p_id_job);
	
		-- формируем запрос на создание вьюхи
		result_create_view := view_create_header || ' ' || result_select;
	
		
		mike.logs.log(4, 'сформирован итоговый запрос на создание вьюхи', CONSTANTS.stg_title_lvl, 'create_view_wdelta_A_I', p_id_job);
	
		-- создаем вьюху
		EXECUTE IMMEDIATE result_create_view;
		
		mike.logs.log(5, 'запрос выполнен', CONSTANTS.stg_title_lvl, 'create_view_wdelta_A_I', p_id_job);
	END;



begin
	mike.stg.create_view_wdelta_A_I(1);
end;



























