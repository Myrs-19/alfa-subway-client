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
			
				mike.stg.uwdelta(l_id_job);
			
				mike.logs.log(4, 'выполнена унификация записи и создание контекста', CONSTANTS.stg_title_lvl, 'wrap_stg', l_id_job);
			
				mike.orchestrator.insert_into_orchestrator(
	    			CONSTANTS.dimension_client_title,
	    			1,
	    			CONSTANTS.dws_title_lvl,
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
		dbms_output.put_line('todo');
	END;

	-- унификация записи
	-- здесь генерится новые uk
	PROCEDURE uni_record(
		p_id_job NUMBER -- номер джоба
	)
	IS
	BEGIN
		dbms_output.put_line('todo');
	END;

	-- создание контекста
	PROCEDURE context(
		p_id_job NUMBER -- номер джоба
	)
	IS
	BEGIN
		dbms_output.put_line('todo');
	END;

	-- унификация атрибутов и создание контексной дельты
	PROCEDURE uwdelta(
		p_id_job NUMBER -- номер джоба
	)
	IS
	BEGIN
		dbms_output.put_line('todo');
	END;
END;

/*

begin
	mike.stg.wrap_stg();
end;

 */