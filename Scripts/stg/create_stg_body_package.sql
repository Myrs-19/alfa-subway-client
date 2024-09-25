CREATE OR REPLACE PACKAGE BODY mike.stg IS 

	-- процедура обертка над всем уровнем stg
	-- в ней генерится номер джоба и передается на след уровни обработки в рамках staging уровня
	-- она же имеет джоб, который запускается каждые 5 минут
	PROCEDURE wrap_stg
	IS
		l_id_job NUMBER := -1; -- номер джоба
		error_message VARCHAR2(256 CHAR); -- сообщение об ошибке
	BEGIN
		BEGIN -- для отлавливания ошибок
			
			-- определение номера джоба
			-- любая таблица-источник - максимальный номер джоба + 1
			SELECT nvl(min(j_n), -1)
			INTO l_id_job
			FROM (
				SELECT job_number j_n
				FROM mike.orchestrator_alfa
				WHERE staging_lvl = mike.CONSTANTS.dws_title_lvl 
					AND is_successful = 1 
					AND need_process = 1
				MINUS 
				SELECT job_number j_n
				FROM mike.orchestrator_alfa
				WHERE staging_lvl = mike.CONSTANTS.stg_title_lvl 
					AND is_successful = 1
			);
		
			mike.logs.log(1, 'запуск stg уровня', CONSTANTS.stg_title_lvl, 'wrap_stg', l_id_job);
		
			IF l_id_job <> -1 THEN 
				mike.stg.wrap_mapping(l_id_job);
			
				mike.logs.log(2, 'выполнен маппинг', CONSTANTS.stg_title_lvl, 'wrap_stg', l_id_job);
			
				mike.stg.wrap_ucontext(l_id_job);
			
				mike.logs.log(3, 'выполнена унификация записи и создание контекста', CONSTANTS.stg_title_lvl, 'wrap_stg', l_id_job);
			
				--mike.stg.uwdelta(l_id_job);
			
				mike.logs.log(4, 'выполнена унификация записи и создание контекста', CONSTANTS.stg_title_lvl, 'wrap_stg', l_id_job);
			
				mike.orchestrator.insert_into_orchestrator(
	    			CONSTANTS.dimension_client_title,
	    			1,
	    			CONSTANTS.stg_title_lvl,
	    			l_id_job,
	    			1
	    		);	
			END IF;
		
			EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
		    	mike.logs.log(5, 'ERROR = ' || error_message, CONSTANTS.stg_title_lvl, 'wrap_stg', l_id_job);
		END;
	END;

	-- обертка для маппинга
	PROCEDURE wrap_mapping(
		p_id_job NUMBER -- номер джоба
	)
	IS
	BEGIN
		mike.stg.mapping_001(p_id_job);
	
		mike.logs.log(1, 'выполнен маппинг источника с кодом 001', CONSTANTS.stg_title_lvl, 'wrap_mapping', p_id_job);
	
		mike.stg.mapping_002(p_id_job);
	
		mike.logs.log(2, 'выполнен маппинг источника с кодом 002', CONSTANTS.stg_title_lvl, 'wrap_mapping', p_id_job);
	END;

	-- маппинг источника с кодом 001
	-- звезда
	-- маппинг - сначала валидация и трансформация
	PROCEDURE mapping_001(
		p_id_job NUMBER -- номер джоба
	)
	IS
	BEGIN
		INSERT INTO STG.CLIENT_CDELTA (
			-- ключи dwh
			nk, -- целочисленный ключ dwh
			
			-- метаполя
			dwseid, -- идентификатор исходной таблицы-источника записи
			dwsjob, -- идентификатор загрузки, в рамках которой произошло изменение записи
			dwsact, -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
			dwsuniact, -- ('I', 'U', 'N' - логический ключ не изменился)
			
			-- поля атрибутов измерения
			id, -- идентификатор человека
			name, -- имя человека
			birthday, -- день рождения человека
			age, -- возраст человека
			phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
			inn, -- инн человека (формат - ххх-ххх-ххх хх)
			pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
			weight, -- вес человека 
			height -- рост человека
		)
		SELECT 
			-- ключи dwh
			nk, -- целочисленный ключ dwh
			
			-- метаполя
			dwseid, -- идентификатор исходной таблицы-источника записи
			dwsjob, -- идентификатор загрузки, в рамках которой произошло изменение записи
			dwsact, -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
			dwsuniact, -- ('I', 'U', 'N' - логический ключ не изменился)
			
			-- поля атрибутов измерения
			id, -- идентификатор человека
			name, -- имя человека
			birthday, -- день рождения человека
			age, -- возраст человека
			phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
			inn, -- инн человека (формат - ххх-ххх-ххх хх)
			pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
			weight, -- вес человека 
			height -- рост человека
		FROM mike.V_MAP_001 
		WHERE dwsjob = p_id_job;
	
		mike.logs.log(1, 'выполнен маппинг источника с кодом 001', CONSTANTS.stg_title_lvl, 'mapping_001', p_id_job);
	END;

	-- маппинг источника с кодом 002
	PROCEDURE mapping_002(
		p_id_job NUMBER -- номер джоба
	)
	IS
	BEGIN
		INSERT INTO STG.CLIENT_CDELTA (
			-- ключи dwh
			nk, -- целочисленный ключ dwh
			
			-- метаполя
			dwseid, -- идентификатор исходной таблицы-источника записи
			dwsjob, -- идентификатор загрузки, в рамках которой произошло изменение записи
			dwsact, -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
			dwsuniact, -- ('I', 'U', 'N' - логический ключ не изменился)
			
			-- поля атрибутов измерения
			id, -- идентификатор человека
			name, -- имя человека
			birthday, -- день рождения человека
			age, -- возраст человека
			phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
			inn, -- инн человека (формат - ххх-ххх-ххх хх)
			pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
			weight, -- вес человека 
			height -- рост человека
		)
		SELECT 
			-- ключи dwh
			nk, -- целочисленный ключ dwh
			
			-- метаполя
			dwseid, -- идентификатор исходной таблицы-источника записи
			dwsjob, -- идентификатор загрузки, в рамках которой произошло изменение записи
			dwsact, -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
			dwsuniact, -- ('I', 'U', 'N' - логический ключ не изменился)
			
			-- поля атрибутов измерения
			id, -- идентификатор человека
			name, -- имя человека
			birthday, -- день рождения человека
			age, -- возраст человека
			phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
			inn, -- инн человека (формат - ххх-ххх-ххх хх)
			pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
			weight, -- вес человека 
			height -- рост человека
		FROM mike.V_MAP_002 
		WHERE dwsjob = p_id_job;
	
		mike.logs.log(1, 'выполнен маппинг источника с кодом 001', CONSTANTS.stg_title_lvl, 'mapping_002', p_id_job);
	END;

	-- обертка на унификации записи и создание контекста
	PROCEDURE wrap_ucontext(
		p_id_job NUMBER -- номер джоба
	)
	IS
	BEGIN
		uni_record(p_id_job);
		
		mike.logs.log(1, 'унификация записи завершена', CONSTANTS.stg_title_lvl, 'wrap_ucontext', p_id_job);
		
		context(p_id_job);
		
		mike.logs.log(2, 'формирование контекста завершено', CONSTANTS.stg_title_lvl, 'wrap_ucontext', p_id_job);
	END;

	-- унификация записи
	-- здесь генерится новые uk
	PROCEDURE uni_record(
		p_id_job NUMBER -- номер джоба
	)
	IS
	BEGIN
		-- очищаем промежуточную таблицу
		DELETE FROM mike.staging_uni_record;
	
		mike.logs.log(1, 'очистили промежуточную таблицу', CONSTANTS.stg_title_lvl, 'uni_record', p_id_job);
		
		INSERT INTO mike.staging_uni_record(
			nk,
			uk,
			
			dwsjob,
			manual,
			
			inn
		)
		SELECT
			-- ключи dwh
			cdelta.nk,
			mike.key_dwh.nextval uk,
				
			-- метаполя
			cdelta.dwsjob,
			cdelta.manual,
				
			cdelta.inn
		FROM (
			SELECT 
				cdelta.nk, 
				cdelta.dwsjob, 
				'N' manual,
					
				cdelta.inn
			FROM (
				SELECT *
				FROM STG.CLIENT_CDELTA cdelta
				WHERE cdelta.dwsjob = p_id_job
			) cdelta
			LEFT JOIN STG.CLIENT_UKLINK uklink 
				ON uklink.nk = cdelta.nk
			WHERE uklink.uk IS NULL AND cdelta.inn IS NOT NULL 
		) cdelta;
	
		mike.logs.log(2, 'вставили записи в промежуточную таблицу', CONSTANTS.stg_title_lvl, 'uni_record', p_id_job);
	
		INSERT INTO STG.CLIENT_UKLINK (
			-- ключи dwh
			nk, -- целочисленный ключ dwh
			uk, -- унифицированный ключ измерения в dwh
			
			-- метаполя
			dwsjob, -- идентификатор загрузки, в рамках которой произошло изменение записи
			manual -- флаг ручной унификации записи ('Y' - вручную, 'N' - иначе)
		)
		SELECT 
			-- ключи dwh
			nk, -- целочисленный ключ dwh
			max(uk) OVER(PARTITION BY inn), -- унифицированный ключ измерения в dwh
			
			-- метаполя
			dwsjob, -- идентификатор загрузки, в рамках которой произошло изменение записи
			manual -- флаг ручной унификации записи ('Y' - вручную, 'N' - иначе)
		FROM 
			mike.staging_uni_record;

		mike.logs.log(3, 'вставили в новые записи в uklink', CONSTANTS.stg_title_lvl, 'uni_record', p_id_job);
		
		MERGE INTO STG.CLIENT_UKLINK uklink
		USING (
			SELECT *
			FROM STG.CLIENT_CDELTA cdelta
			WHERE dwsuniact = 'U' AND cdelta.dwsjob = p_id_job
		) cdelta
		ON (
			cdelta.nk = uklink.nk
		)
		WHEN MATCHED THEN
		UPDATE SET 
			uk = mike.key_dwh.nextval;
		
		mike.logs.log(4, 'обновили uk для dwsuniact = U, унификация записи завершена', CONSTANTS.stg_title_lvl, 'uni_record', p_id_job);
	END;

	-- создание контекста
	PROCEDURE context(
		p_id_job NUMBER -- номер джоба
	)
	IS
	BEGIN
		MERGE INTO STG.CLIENT_CONTEXT context
		USING (
			-- выбираем записи из дельты из текущей выгрузки
			SELECT cdelta.*, uklink.uk
			FROM (
				SELECT * 
				FROM STG.CLIENT_CDELTA cdelta
				WHERE dwsjob = p_id_job
			) cdelta 
			JOIN STG.CLIENT_UKLINK uklink ON uklink.nk = cdelta.nk
		) cdelta
		ON (
			cdelta.nk = context.nk
		)
		WHEN MATCHED THEN 
		UPDATE SET
			context.uk = cdelta.uk, -- унифицированный ключ измерения в dwh
			
			-- метаполя
			context.dwsjob = cdelta.dwsjob, -- идентификатор загрузки, в рамках которой произошло изменение записи
			
			context.dwsarchive = decode(
				cdelta.dwsact,
				'D', 'D',
				NULL
			), -- признак удаленной записи ('D' - удалена, NULL - иначе)
			context.dwsuniact = 'N', -- признак необходимости переунификации записи 
										-- ('I' - новая и необходима унификация,
										--	'U' - необходима переунификация,
										--	'N' - унификация не требуется)
			context.manual = 'N', -- флаг ручной унификации записи ('Y' - вручную, 'N' - иначе)
			
			-- поля атрибутов измерения
			context.id = cdelta.id, -- идентификатор человека
			context.name = cdelta.name, -- имя человека
			context.birthday = cdelta.birthday, -- день рождения человека
			context.age = cdelta.age, -- возраст человека
			context.phone = cdelta.phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
			context.inn = cdelta.inn, -- инн человека (формат - ххх-ххх-ххх хх)
			context.pasport = cdelta.pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
			context.weight = cdelta.weight, -- вес человека 
			context.height = cdelta.height -- рост человека
		WHEN NOT MATCHED THEN
		INSERT (
			-- ключи dwh
			context.nk, -- целочисленный ключ dwh
			context.uk, -- унифицированный ключ измерения в dwh
			-- метаполя
			context.dwseid, -- идентификатор исходной таблицы-источника записи
			context.dwsjob, -- идентификатор загрузки, в рамках которой произошло изменение записи
			
			context.dwsarchive, -- признак удаленной записи ('D' - удалена, NULL - иначе)
			context.dwsuniact, -- признак необходимости переунификации записи 
										-- ('I' - новая и необходима унификация,
										--	'U' - необходима переунификация,
										--	'N' - унификация не требуется)
			context.manual, -- флаг ручной унификации записи ('Y' - вручную, 'N' - иначе)
			-- поля атрибутов измерения
			context.id, -- идентификатор человека
			context.name, -- имя человека
			context.birthday, -- день рождения человека
			context.age, -- возраст человека
			context.phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
			context.inn, -- инн человека (формат - ххх-ххх-ххх хх)
			context.pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
			context.weight, -- вес человека 
			context.height -- рост человека
		)
		VALUES (
			-- ключи dwh
			cdelta.nk, -- целочисленный ключ dwh
			cdelta.uk, -- унифицированный ключ измерения в dwh
			-- метаполя
			cdelta.dwseid, -- идентификатор исходной таблицы-источника записи
			cdelta.dwsjob, -- идентификатор загрузки, в рамках которой произошло изменение записи
			
			NULL, -- признак удаленной записи ('D' - удалена, NULL - иначе)
			'N', -- признак необходимости переунификации записи 
										-- ('I' - новая и необходима унификация,
										--	'U' - необходима переунификация,
										--	'N' - унификация не требуется)
			'N', -- флаг ручной унификации записи ('Y' - вручную, 'N' - иначе)
			-- поля атрибутов измерения
			cdelta.id, -- идентификатор человека
			cdelta.name, -- имя человека
			cdelta.birthday, -- день рождения человека
			cdelta.age, -- возраст человека
			cdelta.phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
			cdelta.inn, -- инн человека (формат - ххх-ххх-ххх хх)
			cdelta.pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
			cdelta.weight, -- вес человека 
			cdelta.height -- рост человека
		);
	
		mike.logs.log(1, 'формирование контекста завершено', CONSTANTS.stg_title_lvl, 'context', p_id_job);
	END;

	-- унификация атрибутов и создание контексной дельты
	PROCEDURE uwdelta(
		p_id_job NUMBER -- номер джоба
	)
	IS
	BEGIN
		INSERT INTO STG.CLIENT_WDELTA (
			-- ключи dwh
			uk, -- унифицированный ключ измерения в dwh
			
			-- метаполя
			dwsact, -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
			dwsemix, -- код сочетания исходных таблиц (внешний ключ на таблицу DWSEMIX)
			dwsjob, -- идентификатор загрузки, в рамках которой произошло изменение записи
			
			-- поля атрибутов измерения
			id, -- идентификатор человека
			name, -- имя человека
			birthday, -- день рождения человека
			age, -- возраст человека
			phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
			inn, -- инн человека (формат - ххх-ххх-ххх хх)
			pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
			weight, -- вес человека 
			height -- рост человека
		)
		SELECT 
			-- ключи dwh
			v_w.uk, -- унифицированный ключ измерения в dwh
			
			-- метаполя
			'I', -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
			12, -- код сочетания исходных таблиц (внешний ключ на таблицу DWSEMIX)
			p_id_job, -- идентификатор загрузки, в рамках которой произошло изменение записи
			
			-- поля атрибутов измерения
			v_w.id, -- идентификатор человека
			v_w.name, -- имя человека
			v_w.birthday, -- день рождения человека
			v_w.age, -- возраст человека
			v_w.phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
			v_w.inn, -- инн человека (формат - ххх-ххх-ххх хх)
			v_w.pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
			v_w.weight, -- вес человека 
			v_w.height -- рост человека
		FROM mike.v_wdelta_I v_w;
		
		mike.logs.log(1, 'обработка для случая dwsact = I завершена', CONSTANTS.stg_title_lvl, 'uwdelta', p_id_job);

		MERGE INTO STG.CLIENT_WDELTA wdelta
		USING (
			SELECT *
			FROM v_wdelta_U
		) v_w
		ON (v_w.uk = wdelta.uk)
		WHEN MATCHED THEN UPDATE SET 
			wdelta.dwsact = 'U',
			wdelta.dwsjob = p_id_job,
			wdelta.id = v_w.id, -- идентификатор человека
			wdelta.name = v_w.name, -- имя человека
			wdelta.birthday = v_w.birthday, -- день рождения человека
			wdelta.age = v_w.age, -- возраст человека
			wdelta.phone = v_w.phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
			wdelta.inn = v_w.inn, -- инн человека (формат - ххх-ххх-ххх хх)
			wdelta.pasport = v_w.pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
			wdelta.weight = v_w.weight, -- вес человека 
			wdelta.height = v_w.height -- рост человека
		;
		
		mike.logs.log(2, 'обработка для случая dwsact = U завершена', CONSTANTS.stg_title_lvl, 'uwdelta', p_id_job);

		MERGE INTO STG.CLIENT_WDELTA wdelta
		USING (
			SELECT *
			FROM v_wdelta_D
		) v_w
		ON (v_w.uk = wdelta.uk)
		WHEN MATCHED THEN UPDATE SET 
			wdelta.dwsact = 'D',
			wdelta.dwsjob = p_id_job;
	
		mike.logs.log(3, 'обработка для случая dwsact = D завершена', CONSTANTS.stg_title_lvl, 'uwdelta', p_id_job);
		
		mike.logs.log(4, 'формирование контексной дельты завершено', CONSTANTS.stg_title_lvl, 'uwdelta', p_id_job);
	END;

	-- создание вьюхи из таблицы с типами унификации для каждого атрибута
	PROCEDURE create_view_wdelta_A_I(
		p_id_job NUMBER -- номер джоба
	)
	IS
		-- курсор, который проходит по записям из таблицы с типами унификации 
		-- для каждого атрибута, у которых тип унификации = 'A' - агрегация
		CURSOR c IS 
			SELECT attr, uni_type_attr 
			FROM mike.uni_attrs 
			WHERE uni_type_attr = 'A';
		
		l_attr varchar2(128 char); -- название атрибута
		l_uni_type_attr varchar2(128 char); -- тип унификация для этого атрибута
	
		-- заголовок запроса для создания вьюхи
		view_create_header varchar2(256 char) := 'CREATE OR REPLACE VIEW mike.v_wdelta_A_I AS ';
	
		-- список столбцов в селекте
		list_select varchar2(1024 char) := 'SELECT context.uk uk, ';
		-- статичное тело селекта
		body_select varchar2(2048 char) := '
FROM (
		SELECT *
		FROM STG.CLIENT_CONTEXT context
		WHERE dwsarchive IS NULL
	) context
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

	PROCEDURE create_view_wdelta_A_U(
		p_id_job NUMBER -- номер джоба
	)
	IS
		-- курсор, который проходит по записям из таблицы с типами унификации 
		-- для каждого атрибута, у которых тип унификации = 'A' - агрегация
		CURSOR c IS 
			SELECT attr, uni_type_attr 
			FROM mike.uni_attrs 
			WHERE uni_type_attr = 'A';
		
		l_attr varchar2(128 char); -- название атрибута
		l_uni_type_attr varchar2(128 char); -- тип унификация для этого атрибута
	
		-- заголовок запроса для создания вьюхи
		view_create_header varchar2(256 char) := 'CREATE OR REPLACE VIEW mike.v_wdelta_A_U AS 
SELECT 
	wdelta_possible_changed.uk uk ';
	
		-- список столбцов в селекте
		list_select varchar2(1024 char) := '';
		-- список столбцов в селекте в подзапросе
		list_inner_select varchar2(1024 char) := '';
		-- статичное тело селекта
		body_select varchar2(2048 char) := ' FROM (
		SELECT *
		FROM STG.CLIENT_CONTEXT context
		WHERE dwsarchive IS NULL
	) context
	JOIN STG.CLIENT_WDELTA wdelta
		ON context.uk = wdelta.uk
	WHERE 
		-- проверка на то, что все записи в контексте не удалены
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
	GROUP BY context.uk
) wdelta_possible_changed
JOIN STG.CLIENT_WDELTA wdelta
	ON wdelta.uk = wdelta_possible_changed.uk
WHERE ';
		-- условие фильтра where запроса
		condition_where_select varchar2(1024 char) := '';


		-- итоговые сформированный запрос для создание вьюхи
		result_query varchar2(10000 char);
	BEGIN
		-- открываем курсор по таблице с типами унификации
		OPEN c;

		mike.logs.log(1, 'курсор открыт', CONSTANTS.stg_title_lvl, 'create_view_wdelta_A_U', p_id_job);

		-- проходим по таблицу и формируем столбцы в select 
		LOOP
			FETCH c INTO l_attr, l_uni_type_attr;
			EXIT WHEN c%NOTFOUND;
		
			list_select := list_select || ' , wdelta_possible_changed.' || l_attr || ' ' || l_attr;
			list_inner_select := list_inner_select || ' max(context.' || l_attr || ') ' || l_attr || ',';
			condition_where_select := condition_where_select || ' ' || 'OR (wdelta.' || l_attr || ' <> wdelta_possible_changed.' || l_attr || ' AND (wdelta.' || l_attr || ' IS NOT NULL OR wdelta_possible_changed.' || l_attr || ' IS NOT NULL))';
		END LOOP;

		mike.logs.log(2, 'сформирован списки для селекта', CONSTANTS.stg_title_lvl, 'create_view_wdelta_A_U', p_id_job);

		-- убираем последнюю запятую
		list_inner_select := regexp_replace(list_inner_select, ',$', '');

		-- убираем первый OR
		condition_where_select := regexp_replace(condition_where_select, '^ OR', '');
	
		-- формируем запрос на создание вьюхи
		result_query := view_create_header 
							|| ' ' 
							|| list_select 
							|| ' FROM ( SELECT context.uk uk, ' 
							|| list_inner_select
							|| body_select
							|| condition_where_select;
	
		mike.logs.log(3, 'сформирован итоговый запрос на создание вьюхи', CONSTANTS.stg_title_lvl, 'create_view_wdelta_A_U', p_id_job);
						
		EXECUTE IMMEDIATE result_query;
	
		mike.logs.log(4, 'запрос выполнен - вьюха создана', CONSTANTS.stg_title_lvl, 'create_view_wdelta_A_U', p_id_job);
	END;

	-- создание вьюхи типа I из таблицы с типами унификации для каждого атрибута
	-- для типа унификации атрибута - приоритет
	PROCEDURE create_view_wdelta_P_I(
		p_id_job NUMBER -- номер джоба
	)
	IS
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
							part_select_4 || l_attr
										  || part_select_5;
										 
			mike.logs.log(2, 'запрос для создание вьюхи сформирован', CONSTANTS.stg_title_lvl, 'create_view_wdelta_P_I', p_id_job);
										 
			EXECUTE IMMEDIATE result_query;
		
			mike.logs.log(3, 'запрос выполнен - вьюха создана', CONSTANTS.stg_title_lvl, 'create_view_wdelta_P_I', p_id_job);
		END LOOP;
	END;

	-- создание вьюхи типа U из таблицы с типами унификации для каждого атрибута
	-- для типа унификации атрибута - приоритет
	PROCEDURE create_view_wdelta_P_U(
		p_id_job NUMBER -- номер джоба
	)
	IS
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

		mike.logs.log(1, 'курсор открыт', CONSTANTS.stg_title_lvl, 'create_view_wdelta_P_U', p_id_job);

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
							
										 
			mike.logs.log(2, 'запрос для создание вьюхи сформирован', CONSTANTS.stg_title_lvl, 'create_view_wdelta_P_U', p_id_job);
		
			EXECUTE IMMEDIATE result_query;
		
			mike.logs.log(3, 'запрос выполнен - вьюха создана', CONSTANTS.stg_title_lvl, 'create_view_wdelta_P_U', p_id_job);
		END LOOP;
	END;

END;

/*

begin
	mike.stg.create_view_wdelta_P_U(1);
end;

 */