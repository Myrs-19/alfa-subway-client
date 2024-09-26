
DECLARE 
p_id_job NUMBER := 1;-- номер джоба

	-- курсор, который проходит по записям из таблицы с типами унификации 
		-- для каждого атрибута, у которых тип унификации = 'A' - агрегация
		CURSOR c IS 
			SELECT attr, uni_type_attr 
			FROM mike.uni_attrs;
		
		l_attr varchar2(128 char); -- название атрибута
		l_uni_type_attr varchar2(128 char); -- тип унификация для этого атрибута
	
		header_view_1 varchar2(1000 CHAR) := 'CREATE OR REPLACE VIEW mike.v_wdelta_U AS 
SELECT
	-- ключи dwh
	COALESCE( ';

	-- список для выражения coalesce
	list_coalesce varchar2(1000 CHAR) := '';

	part_select_1 varchar2(1000 CHAR) := ') uk,';
	
	-- список столбцов
	list_select_1 varchar2(1000 CHAR) := '';

	-- список фром и джойнов
	list_from varchar2(2000 CHAR) := ' FROM ';

	-- список для джойнов
	list_conditions_where varchar2(2000 CHAR) := ' WHERE ';

	-- название текущей вьюхи
	cur_view varchar2(1000 char) := '';

	-- название предыдущей вьюхи
	prev_view varchar2(1000 char) := '';

	-- флаг начала цикла
	f_st NUMBER := 1;

	-- флаг, показывающий была уже возвращена вьюха агрегации или нет
	f_attr_uni_type NUMBER := 0;

	-- итоговый запрос
	result_query varchar2(10000 char);
	BEGIN
		-- открываем курсор по таблице с типами унификации
		OPEN c;

		mike.logs.log(1, 'курсор открыт', CONSTANTS.stg_title_lvl, 'create_view_wdelta_U', p_id_job);

		-- проходим по таблицу и формируем столбцы в select 
		LOOP
			FETCH c INTO l_attr, l_uni_type_attr;
			EXIT WHEN c%NOTFOUND;
		
			-- формируем список столбцов селекта
			list_select_1 := list_select_1 || l_attr || ',';
		
			-- если тип унификации агрегация
			IF l_uni_type_attr = 'A' THEN
				IF f_attr_uni_type = 0 THEN 
					cur_view := 'mike.v_wdelta_A_U';
					f_attr_uni_type := 1;
				ELSE
				-- если вьюха агрегации была уже возвращена, пропускаем итерацию
				-- во вьюхе агрегация используется несколько столбцов, точнее все, который используют этот тип унификации
				-- поэтому ее нужно вернуть только один раз
					CONTINUE;
				END IF;
			
			-- если тип унификации приоритет
			ELSE
				cur_view := 'mike.v_wdelta_P_U_' || l_attr;
			END IF;
		
			-- формируем список выражения coalesce
			list_coalesce := list_coalesce || cur_view || '.uk,';	
		
			IF f_st <> 1 THEN
				list_from := list_from || ',' || cur_view;
			ELSE 
				list_from := list_from || cur_view;
			END IF;

			IF f_st = 0 THEN
				list_conditions_where := list_conditions_where || ' ' || cur_view || '.uk = ' || prev_view || '.uk' || ' AND ';			
			END IF;
		
			-- опускаем флаг первой итерации
			f_st := 0;
		
			prev_view := cur_view;
		END LOOP;

		-- убираем лишнюю запятую в списке выражения coalesce	
		list_coalesce := regexp_replace(list_coalesce, ',$');
	
		-- убираем лишнюю запятую в списке столбцов селекта
		list_select_1 := regexp_replace(list_select_1, ',$');
	
		-- убираем последний AND в условии фильтрации
		list_conditions_where := regexp_replace(list_conditions_where, 'AND $');
	
		result_query := header_view_1 
						|| list_coalesce
						|| part_select_1
						|| list_select_1
						|| list_from
						|| list_conditions_where;
	
		mike.logs.log(2, 'сформирован итоговый запрос', CONSTANTS.stg_title_lvl, 'create_view_wdelta_U', p_id_job);
			
		dbms_output.put_line(result_query);
	
		EXECUTE IMMEDIATE result_query;
		
		mike.logs.log(3, 'запрос выполнен - вьюха создана', CONSTANTS.stg_title_lvl, 'create_view_wdelta_U', p_id_job);
	END;
	

SELECT * FROM mike.v_wdelta_U;