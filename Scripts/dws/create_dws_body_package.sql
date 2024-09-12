CREATE OR REPLACE PACKAGE BODY mike.dws IS

	-- процедура обертка над всем уровнем dws
	-- в ней генерится номер джоба и передается на след уровни обработки в рамках интерфейсного staging уровня
	-- она же имеет джоб, который запускается каждые 5 минут
	PROCEDURE wrap_dws
	IS
		l_id_job NUMBER := -1;
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
		
			-- определение номера джоба
			-- любая таблица-источник - максимальный номер джоба + 1
			SELECT nvl(min(j_n), -1)
			INTO l_id_job
			FROM (
				SELECT job_number j_n
				FROM mike.orchestrator_alfa
				WHERE staging_lvl = mike.CONSTANTS.dwi_title_lvl AND is_successful = 1 AND need_process = 1
				MINUS 
				SELECT job_number j_n
				FROM mike.orchestrator_alfa
				WHERE staging_lvl = mike.CONSTANTS.dws_title_lvl
			);
			
			mike.logs.log(1, 'запуск dws уровня', CONSTANTS.dws_title_lvl, 'wrap_dws', l_id_job);	
			
			IF l_id_job <> -1 THEN 
				mike.dws.wrap_clnt_001_dws(l_id_job);
			
				mike.logs.log(2, 'обработан источник clnt_001', CONSTANTS.dws_title_lvl, 'wrap_dws', l_id_job);	
				
				mike.dws.wrap_PI_002_dws(l_id_job);
			
				mike.logs.log(3, 'обработан источник PI_002', CONSTANTS.dws_title_lvl, 'wrap_dws', l_id_job);	
			
				mike.dws.wrap_PN_002_dws(l_id_job);
			
				mike.logs.log(4, 'обработан источник PN_002', CONSTANTS.dws_title_lvl, 'wrap_dws', l_id_job);	
			
				mike.dws.wrap_docs_002_dws(l_id_job);
			
				mike.logs.log(5, 'обработан источник docs_002', CONSTANTS.dws_title_lvl, 'wrap_dws', l_id_job);	
			
				mike.dws.wrap_hp_002_dws(l_id_job);
			
				mike.logs.log(6, 'обработан источник hp_002', CONSTANTS.dws_title_lvl, 'wrap_dws', l_id_job);	
			END IF;
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	
	    		mike.logs.log(7, 'error = ' || error_message, CONSTANTS.dws_title_lvl, 'wrap_dws', l_id_job);	    	
		END;
	END;

	---------------- таблица clnt, источник звезда ----------------
	-- обертка для таблицы-источника clnt
	PROCEDURE wrap_clnt_001_dws(
		p_id_job NUMBER --номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			
			mike.logs.log(1, 'запуск уровня dws для таблицы clnt_001', CONSTANTS.dws_title_lvl, 'wrap_clnt_001_dws', p_id_job);
		
			mike.dws.clnt_001_dws_double(p_id_job);
		
			mike.logs.log(2, 'завершена обработка дублей', CONSTANTS.dws_title_lvl, 'wrap_clnt_001_dws', p_id_job);
		
			mike.dws.clnt_001_dws_pk_to_nk(p_id_job);
		
			mike.logs.log(3, 'завершено формирование nk', CONSTANTS.dws_title_lvl, 'wrap_clnt_001_dws', p_id_job);
		
			mike.dws.clnt_001_dws_delta(p_id_job);
		
			mike.logs.log(4, 'завершено формирование delta', CONSTANTS.dws_title_lvl, 'wrap_clnt_001_dws', p_id_job);
		
			mike.dws.clnt_001_dws_mirror(p_id_job);
		
			mike.logs.log(5, 'завершено формирование mirror', CONSTANTS.dws_title_lvl, 'wrap_clnt_001_dws', p_id_job);
		
			mike.orchestrator.insert_into_orchestrator(
	    			CONSTANTS.table_source_title_star_clnt,
	    			1,
	    			CONSTANTS.dws_title_lvl,
	    			p_id_job,
	    			1
	    		);	
	    	
	    	mike.logs.log(6, 'запись в оркестратор вставлена', CONSTANTS.dws_title_lvl, 'wrap_clnt_001_dws', p_id_job);
		
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	
	    		mike.logs.log(7, 'error = ' || error_message, CONSTANTS.dws_title_lvl, 'wrap_clnt_001_dws', p_id_job);
	    	    	
	    		mike.orchestrator.insert_into_orchestrator(
	    			CONSTANTS.table_source_title_star_clnt,
	    			-1,
	    			CONSTANTS.dws_title_lvl,
	    			p_id_job,
	    			1
	    		);	
		END;
	END;

	-- процедура очистки дублей таблицы clnt 
	PROCEDURE clnt_001_dws_double(
		p_id_job NUMBER --номер джоба
	)
	IS
	BEGIN
		mike.logs.log(1, 'определение дублей', CONSTANTS.dws_title_lvl, 'clnt_001_dws_double', p_id_job);
	    	    
		INSERT INTO DWS001_CLNT.CLIENT001_DOUBLE(
			-- метаполя
			dwsjob,
			as_of_day, -- дата актуализации DWH
			effective_flag, -- 'Y' - если запись эфективна в группе дублей
				
			-- поля таблицы-источника
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
			-- метаполя
			p_id_job dwsjob,
			sysdate as_of_day, -- дата актуализации DWH
			decode(
				rn, 
				1, 'Y',
				NULL ) effective_flag, -- 'Y' - если запись эфективна в группе дублей
				
			-- поля таблицы-источника
			id, -- идентификатор человека
			name, -- имя человека
			birthday, -- день рождения человека
			age, -- возраст человека
			phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
			inn, -- инн человека (формат - ххх-ххх-ххх хх)
			pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
			weight, -- вес человека 
			height -- рост человека
		FROM (
			SELECT 
				dsrc.*, 
				ROW_NUMBER() OVER(PARTITION BY id ORDER BY id, name, BIRTHDAY, age, phone, inn, pasport, weight, height) rn,
				COUNT(id) OVER(PARTITION BY id) cnt
			FROM DWI001_STAR.client001_DSRC dsrc
		) s
		WHERE cnt > 1;
		
	
		mike.logs.log(2, 'определение дублей завершено', CONSTANTS.dws_title_lvl, 'clnt_001_dws_double', p_id_job);
	END;

	-- процедура преобразования pk в nk таблицы clnt
	PROCEDURE clnt_001_dws_pk_to_nk(
		p_id_job NUMBER --номер джоба
	)
	IS
	BEGIN
		mike.logs.log(1, 'формирование nk', CONSTANTS.dws_title_lvl, 'clnt_001_dws_pk_to_nk', p_id_job);
		
		INSERT INTO DWS001_CLNT.CLIENT001_NKLINK(
			nk,
	
			-- pk исходной системы
			id, -- идентификатор человека
			
			-- метаполя
			dwsjob,
			dwsauto -- признак записи сгенерированной автоматически при загрузке фактов, Y - автоматически, NULL - нет
		)
		SELECT 
			mike.key_dwh.nextval nk,
		
			id,
			
			dwsjob,
			dwsauto
		FROM (
			SELECT 
				id,
				
				p_id_job dwsjob,
				NULL dwsauto
			FROM (
			-- выбираем записи из интерфейсного уровня на этом джобе
				SELECT *
				FROM DWI001_STAR.client001_DSRC dsrc
				WHERE dwsjob = p_id_job
			) dsrc
			-- смотрим есть ли такой pk в nklink 
			LEFT JOIN DWS001_CLNT.CLIENT001_NKLINK nklink
				ON nklink.id = dsrc.id
			RIGHT JOIN (
				SELECT *
				FROM DWS001_CLNT.CLIENT001_DOUBLE double
				WHERE dwsjob = p_id_job AND effective_flag = 'Y'
			) double
				ON double.id = dsrc.id
					AND 
					double.name = dsrc.name
					AND 
					double.birthday = dsrc.birthday
					AND
					double.age = dsrc.age
					AND
					double.phone = dsrc.phone
					AND
					double.inn = dsrc.inn
					AND
					double.pasport = dsrc.pasport
					AND
					double.weight = dsrc.weight
					AND
					double.height = dsrc.height
			WHERE nklink.id IS NULL
			-- для строк из dsrc у которых все поля одинаковы
			GROUP BY id
		) s;
		
	
		mike.logs.log(2, 'формирование nk завершено', CONSTANTS.dws_title_lvl, 'clnt_001_dws_pk_to_nk', p_id_job);
	END;

	PROCEDURE clnt_001_dws_delta(
		p_id_job NUMBER --номер джоба
	)
	IS
	BEGIN
		mike.logs.log(1, 'формирование delta', CONSTANTS.dws_title_lvl, 'clnt_001_dws_delta', p_id_job);
		
		-- случай dwsact = 'I' - новые строки для миро
		INSERT INTO DWS001_CLNT.CLIENT001_DELTA(
			nk,	
	
			-- метаполя
			dwsjob,
			dwsact, -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
			dwsuniact, -- ('I', 'U', 'N' - логический ключ не изменился)
				
			-- поля таблицы-источника
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
			nklink.nk,	
	
			-- метаполя
			p_id_job dwsjob,
			'I' dwsact, -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
			'I' dwsuniact, -- ('I', 'U', 'N' - логический ключ не изменился)
				
			-- поля таблицы-источника
			dsrc.id, -- идентификатор человека
			dsrc.name, -- имя человека
			dsrc.birthday, -- день рождения человека
			dsrc.age, -- возраст человека
			dsrc.phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
			dsrc.inn, -- инн человека (формат - ххх-ххх-ххх хх)
			dsrc.pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
			dsrc.weight, -- вес человека 
			dsrc.height -- рост человека
		FROM (
			-- выбираем записи из интерфейсного уровня на этом джобе
			SELECT 
				dsrc.id,
				dsrc.name,
				dsrc.birthday,
				dsrc.age,
				dsrc.phone, 
				dsrc.inn,
				dsrc.pasport,
				dsrc.weight,
				dsrc.height
			FROM DWI001_STAR.client001_DSRC dsrc
			LEFT JOIN DWS001_CLNT.CLIENT001_DOUBLE double
				ON double.id = dsrc.id
			WHERE dsrc.dwsjob = p_id_job AND double.effective_flag = 'Y' OR double.id IS null
			-- для тех записей у которых все поля одинаковы
			GROUP BY
				dsrc.id,
				dsrc.name,
				dsrc.birthday,
				dsrc.age,
				dsrc.phone, 
				dsrc.inn,
				dsrc.pasport,
				dsrc.weight,
				dsrc.height
		) dsrc
		-- join с миро для сравнения
		LEFT JOIN DWS001_CLNT.CLIENT001_MIRROR mirror
			ON dsrc.id = mirror.id
		JOIN DWS001_CLNT.CLIENT001_NKLINK nklink
			ON nklink.id = dsrc.id
		-- выбираем те записи, которых еще нет в миро
		WHERE mirror.id IS NULL;
	
		mike.logs.log(2, 'случай dwsact = I обработан', CONSTANTS.dws_title_lvl, 'clnt_001_dws_delta', p_id_job);
	
		-- случай dwsact = 'U' - новые строки для миро
		INSERT INTO DWS001_CLNT.CLIENT001_DELTA(
			nk,	
	
			-- метаполя
			dwsjob,
			dwsact, -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
			dwsuniact, -- ('I', 'U', 'N' - логический ключ не изменился)
				
			-- поля таблицы-источника
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
			nklink.nk,	
	
			-- метаполя
			p_id_job dwsjob,
			'U' dwsact, -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
			CASE 
				WHEN dsrc.inn <> mirror.inn THEN 'U'
				ELSE 'N'
			END dwsuniact, -- ('I', 'U', 'N' - логический ключ не изменился)
				
			-- поля таблицы-источника
			dsrc.id, -- идентификатор человека
			dsrc.name, -- имя человека
			dsrc.birthday, -- день рождения человека
			dsrc.age, -- возраст человека
			dsrc.phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
			dsrc.inn, -- инн человека (формат - ххх-ххх-ххх хх)
			dsrc.pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
			dsrc.weight, -- вес человека 
			dsrc.height -- рост человека
		FROM (
			-- выбираем записи из интерфейсного уровня на этом джобе
			SELECT 
				dsrc.id,
				dsrc.name,
				dsrc.birthday,
				dsrc.age,
				dsrc.phone, 
				dsrc.inn,
				dsrc.pasport,
				dsrc.weight,
				dsrc.height
			FROM DWI001_STAR.client001_DSRC dsrc
			LEFT JOIN DWS001_CLNT.CLIENT001_DOUBLE double
				ON double.id = dsrc.id
			WHERE dsrc.dwsjob = p_id_job AND double.effective_flag = 'Y' OR double.id IS null
			-- для тех записей у которых все поля одинаковы
			GROUP BY
				dsrc.id,
				dsrc.name,
				dsrc.birthday,
				dsrc.age,
				dsrc.phone, 
				dsrc.inn,
				dsrc.pasport,
				dsrc.weight,
				dsrc.height
		) dsrc
		-- join с миро для сравнения
		LEFT JOIN DWS001_CLNT.CLIENT001_MIRROR mirror
			ON dsrc.id = mirror.id
		JOIN DWS001_CLNT.CLIENT001_NKLINK nklink
			ON nklink.id = dsrc.id
		-- выбираем те записи, которых еще нет в миро
		WHERE mirror.id IS NOT NULL AND (
			dsrc.name <> mirror.name 
			OR
			dsrc.birthday <> mirror.birthday
			OR 
			dsrc.age <> mirror.age
			OR
			dsrc.phone <> mirror.phone
			OR 
			dsrc.inn <> mirror.inn
			OR
			dsrc.pasport <> mirror.pasport
			OR
			dsrc.weight <> mirror.weight
			OR
			dsrc.height <> mirror.height
		);
	
		mike.logs.log(3, 'случай dwsact = U обработан', CONSTANTS.dws_title_lvl, 'clnt_001_dws_delta', p_id_job);
		
		-- случай dwsact = 'D' - новые строки для миро
		INSERT INTO DWS001_CLNT.CLIENT001_DELTA(
			nk,	
	
			-- метаполя
			dwsjob,
			dwsact, -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
			dwsuniact, -- ('I', 'U', 'N' - логический ключ не изменился)
				
			-- поля таблицы-источника
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
			nklink.nk,	
	
			-- метаполя
			p_id_job dwsjob,
			'D' dwsact, -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
			'N' dwsuniact, -- ('I', 'U', 'N' - логический ключ не изменился)
				
			-- поля таблицы-источника
			dsrc.id, -- идентификатор человека
			dsrc.name, -- имя человека
			dsrc.birthday, -- день рождения человека
			dsrc.age, -- возраст человека
			dsrc.phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
			dsrc.inn, -- инн человека (формат - ххх-ххх-ххх хх)
			dsrc.pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
			dsrc.weight, -- вес человека 
			dsrc.height -- рост человека
		FROM (
			-- выбираем записи из интерфейсного уровня на этом джобе
			SELECT 
				dsrc.id,
				dsrc.name,
				dsrc.birthday,
				dsrc.age,
				dsrc.phone, 
				dsrc.inn,
				dsrc.pasport,
				dsrc.weight,
				dsrc.height
			FROM DWI001_STAR.client001_DSRC dsrc
			LEFT JOIN DWS001_CLNT.CLIENT001_DOUBLE double
				ON double.id = dsrc.id
			WHERE dsrc.dwsjob = p_id_job AND double.effective_flag = 'Y' OR double.id IS null
			-- для тех записей у которых все поля одинаковы
			GROUP BY
				dsrc.id,
				dsrc.name,
				dsrc.birthday,
				dsrc.age,
				dsrc.phone, 
				dsrc.inn,
				dsrc.pasport,
				dsrc.weight,
				dsrc.height
		) dsrc
		-- join с миро для сравнения
		RIGHT JOIN DWS001_CLNT.CLIENT001_MIRROR mirror
			ON dsrc.id = mirror.id
		JOIN DWS001_CLNT.CLIENT001_NKLINK nklink
			ON nklink.id = dsrc.id
		-- выбираем те записи, которых еще нет в миро
		WHERE dsrc.id IS NULL;
	
		mike.logs.log(3, 'случай dwsact = D обработан', CONSTANTS.dws_title_lvl, 'clnt_001_dws_delta', p_id_job);
	
		mike.logs.log(4, 'формирование delta завершено', CONSTANTS.dws_title_lvl, 'clnt_001_dws_delta', p_id_job);
	END;

	PROCEDURE clnt_001_dws_mirror(
		p_id_job NUMBER --номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		mike.logs.log(1, 'актуализация mirror', CONSTANTS.dws_title_lvl, 'clnt_001_dws_mirror', p_id_job);
		
		MERGE INTO DWS001_CLNT.CLIENT001_MIRROR mirror
		USING (
			-- выбираем записи из дельты из текущей выгрузки
			SELECT *
			FROM DWS001_CLNT.CLIENT001_DELTA delta
			WHERE dwsjob = p_id_job
		) delta
		ON (
			delta.nk = mirror.nk
		)
		WHEN MATCHED THEN 
		UPDATE SET 
			mirror.dwsjob = p_id_job,
			mirror.dwsauto = NULL,
			mirror.dwsarchive = delta.dwsact,
			mirror.id = delta.id,
			mirror.name = delta.name,
			mirror.birthday = delta.birthday,
			mirror.age = delta.age,
			mirror.phone = delta.phone,
			mirror.inn = delta.inn,
			mirror.pasport = delta.pasport,
			mirror.weight = delta.weight,
			mirror.height = delta.height
		WHEN NOT MATCHED THEN
		INSERT (
			mirror.nk,
			mirror.dwsjob,
			mirror.dwsauto,
			mirror.dwsarchive,
			mirror.id,
			mirror.name,
			mirror.birthday,
			mirror.age,
			mirror.phone,
			mirror.inn,
			mirror.pasport,
			mirror.weight,
			mirror.height
		)
		VALUES (
			delta.nk,
			p_id_job,
			NULL,
			delta.dwsact,
			delta.id,
			delta.name,
			delta.birthday,
			delta.age,
			delta.phone,
			delta.inn,
			delta.pasport,
			delta.weight,
			delta.height
		);
		
		mike.logs.log(2, 'актуализация mirror завершена', CONSTANTS.dws_title_lvl, 'clnt_001_dws_mirror', p_id_job);
	END;
	---------------- таблица PI, источник 3НФ ----------------

	PROCEDURE wrap_PI_002_dws(
		p_id_job NUMBER --номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	-- процедура очистки дублей таблицы PI 
	PROCEDURE PI_002_dws_double(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	-- процедура преобразования pk в nk таблицы PI
	PROCEDURE PI_002_dws_pk_to_nk(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	-- процедура для формировании delta таблицы PI
	PROCEDURE PI_002_dws_delta(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	-- процедура для актуализации mirror таблицы PI
	PROCEDURE PI_002_dws_mirror(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	---------------- таблица PN, источник 3НФ ----------------

	PROCEDURE wrap_PN_002_dws(
		p_id_job NUMBER --номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		dbms_output.put_line('todo');
	END;

	-- процедура очистки дублей таблицы PN 
	PROCEDURE PN_002_dws_double(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	-- процедура преобразования pk в nk таблицы PN
	PROCEDURE PN_002_dws_pk_to_nk(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	-- процедура для формировании delta таблицы PN
	PROCEDURE PN_002_dws_delta(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	-- процедура для актуализации mirror таблицы PN
	PROCEDURE PN_002_dws_mirror(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	---------------- таблица docs, источник 3НФ ----------------

	PROCEDURE wrap_docs_002_dws(
		p_id_job NUMBER --номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	-- процедура очистки дублей таблицы docs 
	PROCEDURE docs_002_dws_double(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	-- процедура преобразования pk в nk таблицы docs
	PROCEDURE docs_002_dws_pk_to_nk(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	-- процедура для формировании delta таблицы docs
	PROCEDURE docs_002_dws_delta(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	-- процедура для актуализации mirror таблицы docs
	PROCEDURE docs_002_dws_mirror(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	---------------- таблица hp, источник 3НФ ----------------

	PROCEDURE wrap_hp_002_dws(
		p_id_job NUMBER --номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	-- процедура очистки дублей таблицы hp 
	PROCEDURE hp_002_dws_double(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	-- процедура преобразования pk в nk таблицы hp
	PROCEDURE hp_002_dws_pk_to_nk(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	-- процедура для формировании delta таблицы hp
	PROCEDURE hp_002_dws_delta(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;

	-- процедура для актуализации mirror таблицы hp
	PROCEDURE hp_002_dws_mirror(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			dbms_output.put_line('todo');
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	    	
		END;
	END;
END;




/*

begin
	mike.dws.wrap_dws();
end; 

 */







