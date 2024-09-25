DECLARE 
	
	p_id_job NUMBER := 1;
	-- курсор, который проходит по записям из таблицы с типами унификации 
	-- для каждого атрибута, у которых тип унификации = 'A' - агрегация
	CURSOR c IS 
		SELECT attr, uni_type_attr 
		FROM mike.uni_attrs 
		WHERE uni_type_attr = 'P';
		
	l_attr varchar2(128 char); -- название атрибута
	l_uni_type_attr varchar2(128 char); -- тип унификация для этого атрибута
	
	header_view_1 varchar2(1000 char) := 'CREATE OR REPLACE VIEW mike.v_wdelta_P_U_';
	header_view_2 varchar2(1000 char) := ' AS SELECT
	wdelta_possible_changed.uk uk, wdelta_possible_changed.';
	
	part_select_1 varchar2(1000 char) := ' FROM (
	SELECT
		-- ключи dwh
		wcontext.uk uk,
			
		-- унифицируемый атрибут
		-- в примере это атрибут - PHONE
		-- attr
		wcontext.';
	part_select_2 varchar2(1000 char) := ' FROM (
		SELECT 
			-- ключи dwh
			wcontext.uk uk,
			
			-- унифицируемый атрибут
			-- в примере это атрибут - PHONE
			-- attr
			wcontext.';
	part_select_3 varchar2(1000 char) := ' ,
			ROW_NUMBER() OVER(PARTITION BY wcontext.uk ORDER BY p_a.priority) rn
		FROM (
			SELECT
				-- ключи dwh
				context.uk uk,
				
				-- метаполя
				context.DWSEID DWSEID,
				
				-- унифицируемый атрибут
				-- в примере это атрибут - PHONE
				-- attr
				context.';
	part_select_4 varchar2(1000 char) := ' FROM (
					SELECT *
					FROM STG.CLIENT_CONTEXT context
					WHERE dwsarchive IS NULL
				) context
			JOIN STG.CLIENT_WDELTA wdelta
				ON context.uk = wdelta.uk
			WHERE -- проверка на то, что все записи в контексте не удалены
		-- то есть записи в контексте могли меняться, 
		-- то есть либо вставится новая, либо измениться старая
		EXISTS(
			SELECT 1
			FROM STG.CLIENT_CONTEXT context_inner
			WHERE context_inner.uk = context.uk AND dwsarchive IS NULL
		) 
		OR 
		-- проверка на то, что есть хотя бы одна удаленая запись для uk
		EXISTS(
			SELECT 1
			FROM (
				SELECT uk, dwsarchive
				FROM STG.CLIENT_CONTEXT context_inner1
				GROUP BY uk, dwsarchive
			) context_inner
			WHERE context_inner.uk = context.uk
			GROUP BY uk
			HAVING count(*) = 2
		)
		-- attr
		AND context.';
	part_select_5 varchar2(1000 char) := ' IS NOT NULL
		) wcontext
		JOIN mike.priority_attrs p_a 
			ON p_a.DWSEID = wcontext.DWSEID
				-- attr
				AND p_a.attr = ''';
	part_select_6 varchar2(1000 char) := ''' ) wcontext
	WHERE rn = 1
) wdelta_possible_changed
JOIN STG.CLIENT_WDELTA wdelta
	ON wdelta.uk = wdelta_possible_changed.uk
WHERE (wdelta.';
	part_select_7 varchar2(1000 char) := ' <> wdelta_possible_changed.';
	part_select_8 varchar2(1000 char) := ' AND (wdelta.';
	part_select_9 varchar2(1000 char) := ' IS NOT NULL OR wdelta_possible_changed.';
	part_select_10 varchar2(1000 char) := ' IS NOT NULL))';

	result_query varchar2(10000 char);
	BEGIN
		-- открываем курсор по таблице с типами унификации
		OPEN c;

		mike.logs.log(1, 'курсор открыт', CONSTANTS.stg_title_lvl, 'create_view_wdelta_P_I', p_id_job);

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
							part_select_4 || l_attr || ' ' 
										  ||
							part_select_5 || l_attr 
										  || 
							part_select_6 || l_attr || ' '
										  ||
							part_select_7 || l_attr || ' '
										  ||
							part_select_8 || l_attr || ' '
										  ||
							part_select_9 || l_attr || ' '
										  || part_select_10;
							
										 
			mike.logs.log(2, 'запрос для создание вьюхи сформирован', CONSTANTS.stg_title_lvl, 'create_view_wdelta_P_I', p_id_job);
		
			EXECUTE IMMEDIATE result_query;
		
			mike.logs.log(3, 'запрос выполнен - вьюха создана', CONSTANTS.stg_title_lvl, 'create_view_wdelta_P_I', p_id_job);
		END LOOP;
	END;
	







CREATE OR REPLACE VIEW mike.v_wdelta_P_U_PHONE  AS 

SELECT
	wdelta_possible_changed.uk uk, wdelta_possible_changed.PHONE  FROM (
	SELECT
		-- ключи dwh
		wcontext.uk uk,
			
		-- унифицируемый атрибут
		-- в примере это атрибут - PHONE
		-- attr
		wcontext.PHONE  FROM (
		SELECT 
			-- ключи dwh
			wcontext.uk uk,
			
			-- унифицируемый атрибут
			-- в примере это атрибут - PHONE
			-- attr
			wcontext.PHONE  ,
			ROW_NUMBER() OVER(PARTITION BY wcontext.uk ORDER BY p_a.priority) rn
		FROM (
			SELECT
				-- ключи dwh
				context.uk uk,
				
				-- метаполя
				context.DWSEID DWSEID,
				
				-- унифицируемый атрибут
				-- в примере это атрибут - PHONE
				-- attr
				context.PHONE  FROM (
					SELECT *
					FROM STG.CLIENT_CONTEXT context
					WHERE dwsarchive IS NULL
				) context
			JOIN STG.CLIENT_WDELTA wdelta
				ON context.uk = wdelta.uk
			WHERE -- проверка на то, что все записи в контексте не удалены
		-- то есть записи в контексте могли меняться, 
		-- то есть либо вставится новая, либо измениться старая
		EXISTS(
			SELECT 1
			FROM STG.CLIENT_CONTEXT context_inner
			WHERE context_inner.uk = context.uk AND dwsarchive IS NULL
		) 
		OR 
		-- проверка на то, что есть хотя бы одна удаленая запись для uk
		EXISTS(
			SELECT 1
			FROM (
				SELECT uk, dwsarchive
				FROM STG.CLIENT_CONTEXT context_inner1
				GROUP BY uk, dwsarchive
			) context_inner
			WHERE context_inner.uk = context.uk
			GROUP BY uk
			HAVING count(*) = 2
		)
		-- attr
		AND context.PHONE  IS NOT NULL
		) wcontext
		JOIN mike.priority_attrs p_a 
			ON p_a.DWSEID = wcontext.DWSEID
				-- attr
				AND p_a.attr = 'PHONE' ) wcontext
	WHERE rn = 1
) wdelta_possible_changed
JOIN STG.CLIENT_WDELTA wdelta
	ON wdelta.uk = wdelta_possible_changed.uk
WHERE (wdelta.PHONE  <> wdelta_possible_changed.PHONE  AND (wdelta.PHONE  IS NOT NULL OR wdelta_possible_changed.PHONE  IS NOT NULL))
















