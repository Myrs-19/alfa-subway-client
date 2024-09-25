	DECLARE 
		p_id_job NUMBER := 1; -- номер джоба
		-- курсор, который проходит по записям из таблицы с типами унификации 
		-- для каждого атрибута, у которых тип унификации = 'A' - агрегация
		CURSOR c IS 
			SELECT attr, uni_type_attr 
			FROM mike.uni_attrs 
			WHERE uni_type_attr = 'P';
		
		l_attr varchar2(128 char); -- название атрибута
		l_uni_type_attr varchar2(128 char); -- тип унификация для этого атрибута
	
		header_view_1 varchar2(1000 CHAR) := 'CREATE OR REPLACE VIEW mike.v_wdelta_P_I_';
		header_view_2 varchar2(1000 CHAR) := ' AS SELECT wcontext.uk uk, wcontext.';
		part_select_1 varchar2(1000 CHAR) := ' FROM (
	SELECT 
		-- ключи dwh
		wcontext.uk uk,
		
		-- унифицируемый атрибут
		-- в примере это атрибут - id
		-- attr
		wcontext.';
		part_select_2 varchar2(1000 char) := ' , ROW_NUMBER() OVER(PARTITION BY wcontext.uk ORDER BY p_a.priority) rn
	FROM (
		SELECT
			-- ключи dwh
			context.uk uk,
			
			-- метаполя
			context.DWSEID DWSEID,
			
			-- унифицируемый атрибут
			-- в примере это атрибут - id
			-- attr
			context.';
		part_select_3 VARCHAR2(1000 char) := ' FROM (
				SELECT *
				FROM STG.CLIENT_CONTEXT context
				WHERE dwsarchive IS NULL
			) context
		LEFT JOIN STG.CLIENT_WDELTA wdelta
			ON context.uk = wdelta.uk
		WHERE wdelta.uk IS NULL 
			-- attr
			AND context.';
		part_select_4 varchar2(1000 char) := ' IS NOT NULL
	) wcontext
	JOIN mike.priority_attrs p_a 
		ON p_a.DWSEID = wcontext.DWSEID
			-- attr
			AND p_a.attr = '''; 
		part_select_5 varchar2(1000 char) := '''
 ) wcontext
WHERE rn = 1';

	result_query varchar2(10000 char);
	BEGIN
		-- открываем курсор по таблице с типами унификации
		OPEN c;

		mike.logs.log(1, 'курсор открыт', CONSTANTS.stg_title_lvl, 'create_view_wdelta_A_U', p_id_job);

		-- проходим по таблицу и формируем столбцы в select 
		LOOP
			FETCH c INTO l_attr, l_uni_type_attr;
			EXIT WHEN c%NOTFOUND;
		
			result_query := header_view_1 || l_attr || ' '
										  ||
							header_view_2 || l_attr || ' '
										  ||
							part_select_1 || l_attr || ' '
										  ||
							part_select_2 || l_attr || ' '
										  ||
							part_select_3 || l_attr || ' '
										  ||
							part_select_4 || l_attr
										  || part_select_5;
			
			dbms_output.put_line(result_query);
										 
			EXECUTE IMMEDIATE result_query;
		END LOOP;
	END;







