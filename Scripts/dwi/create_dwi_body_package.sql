CREATE OR REPLACE PACKAGE BODY mike.dwi IS 

	-- процедура обертка
	-- она имеет джоб
	PROCEDURE wrap_dwi
	IS 
		error_message varchar2(256 CHAR);
		l_id_job NUMBER;
	BEGIN 
		BEGIN -- для отлавливания ошибок
			
			-- определение номера джоба
			-- максимальный + 1
			SELECT nvl(max(job_number), 0) + 1
			INTO l_id_job
			FROM mike.orchestrator_alfa
			WHERE 
				title_object = CONSTANTS.table_source_title_star_clnt 
				AND 
				staging_lvl = CONSTANTS.dwi_title_lvl;
			
			mike.logs.log(1, 'запуск dwi', CONSTANTS.dwi_title_lvl, 'wrap_dwi', l_id_job);
			
			mike.dwi.star_clnt_dwi(l_id_job);
		
			mike.logs.log(2, 'обработка завершена для = ' || CONSTANTS.table_source_title_star_clnt, CONSTANTS.dwi_title_lvl, 'wrap_dwi', l_id_job);
			
			mike.dwi.wrap_3nf_dwi(l_id_job);
		
			mike.logs.log(3, 'обработка завершена для 3 НФ', CONSTANTS.dwi_title_lvl, 'wrap_dwi', l_id_job);
			
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    		
	    		mike.logs.log(4, 'ОШИБКА = ' || error_message, CONSTANTS.dwi_title_lvl, 'wrap_dwi', l_id_job);
			
		END;
	END;

	-- загрузка в DSRC из звезды
	PROCEDURE star_clnt_dwi(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- обработка ошибок
			mike.logs.log(1, 'загрузка в DSRC из ' || CONSTANTS.table_source_title_star_clnt, CONSTANTS.dwi_title_lvl, 'star_clnt_dwi', p_id_job);
		
			-- загрузка в DSRC
			INSERT INTO DWI001_STAR.client001_DSRC (
				-- метаполя
				dwsjob, -- идентификатор загрузки
				dwsact, -- тип операции над записью. Если полная выгрузка - NULL
				dwssrcstamp, -- временая метка изменения записи. Если полная выгрузка - NULL
			
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
				p_id_job, -- идентификатор загрузки
				NULL, -- тип операции над записью. Если полная выгрузка - NULL
				NULL, -- временая метка изменения записи. Если полная выгрузка - NULL
				
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
			FROM mike.star_client;
			
			mike.logs.log(2, 'загрузка в DSRC из ' || CONSTANTS.table_source_title_star_clnt || ' завершена', CONSTANTS.dwi_title_lvl, 'star_clnt_dwi', p_id_job);
		
			mike.orchestrator.insert_into_orchestrator(
	    			CONSTANTS.table_source_title_star_clnt,
	    			1,
	    			CONSTANTS.dwi_title_lvl,
	    			p_id_job,
	    			1
	    		);
	    	
	    	mike.logs.log(3, 'запись в оркестратор вставлена', CONSTANTS.dwi_title_lvl, 'star_clnt_dwi', p_id_job);
		
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	
	    		mike.logs.log(4, 'error = ' || error_message, CONSTANTS.dwi_title_lvl, 'star_clnt_dwi', p_id_job);
	    	
	    		mike.orchestrator.insert_into_orchestrator(
	    			CONSTANTS.table_source_title_star_clnt,
	    			-1,
	    			CONSTANTS.dwi_title_lvl,
	    			p_id_job,
	    			1
	    		);
		END;
	END;

	
	PROCEDURE wrap_3nf_dwi(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- обработка ошибок
			mike.logs.log(1, 'запуск загрузки из исходных таблиц в 3нф в DSRC', CONSTANTS.dwi_title_lvl, 'wrap_3nf_dwi', p_id_job);
					
			nf3_pi_dwi(p_id_job);
		
			mike.logs.log(2, ' загрузка из ' || CONSTANTS.table_source_title_3nf_pi || 'завершена', CONSTANTS.dwi_title_lvl, 'wrap_3nf_dwi', p_id_job);
		
			nf3_pn_dwi(p_id_job);
	
			mike.logs.log(3, ' загрузка из ' || CONSTANTS.table_source_title_3nf_pn || 'завершена', CONSTANTS.dwi_title_lvl, 'wrap_3nf_dwi', p_id_job);

			nf3_docs_dwi(p_id_job);
		
			mike.logs.log(4, ' загрузка из ' || CONSTANTS.table_source_title_3nf_docs || 'завершена', CONSTANTS.dwi_title_lvl, 'wrap_3nf_dwi', p_id_job);
		
			nf3_hp_dwi(p_id_job);
		
			mike.logs.log(5, ' загрузка из ' || CONSTANTS.table_source_title_3nf_hp || 'завершена', CONSTANTS.dwi_title_lvl, 'wrap_3nf_dwi', p_id_job);
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	
	    		mike.logs.log(6, 'error = ' || error_message, CONSTANTS.dwi_title_lvl, 'wrap_3nf_dwi', p_id_job);
		END;
	END;

	-- процедура, которая загружает таблицу pi в свою DSRC
	PROCEDURE nf3_pi_dwi(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- обработка ошибок
			mike.logs.log(1, 'загрузка pi в DSRC', CONSTANTS.dwi_title_lvl, 'nf3_pi_dwi', p_id_job);
		
			INSERT INTO DWI002_3NF.personal_information002_DSRC (
				-- метаполя
				dwsjob, -- идентификатор загрузки
				dwsact, -- тип операции над записью. Если полная выгрузка - NULL
				dwssrcstamp, -- временая метка изменения записи. Если полная выгрузка - NULL
				
				-- поля таблицы-источника
				id, -- идентификатор человека
				name, -- имя человека
				birthday, -- день рождения человека
				age -- возраст человека
			)
			SELECT
				-- метаполя
				p_id_job, -- идентификатор загрузки
				NULL, -- тип операции над записью. Если полная выгрузка - NULL
				NULL, -- временая метка изменения записи. Если полная выгрузка - NULL
			
				-- поля таблицы-источника
				id, -- идентификатор человека
				name, -- имя человека
				birthday, -- день рождения человека
				age -- возраст человека
			FROM mike.personal_information;
		
			mike.logs.log(2, 'загрузка pi в DSRC завершена', CONSTANTS.dwi_title_lvl, 'nf3_pi_dwi', p_id_job);
		
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	
	    		mike.logs.log(3, 'error = ' || error_message, CONSTANTS.dwi_title_lvl, 'nf3_pi_dwi', p_id_job);
		END;
	END;
	
	-- процедура, которая загружает таблицу pn в свою DSRC
	PROCEDURE nf3_pn_dwi(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- обработка ошибок
			mike.logs.log(1, 'загрузка pn в DSRC', CONSTANTS.dwi_title_lvl, 'nf3_pn_dwi', p_id_job);
		
			INSERT INTO DWI002_3NF.phone_numbers002_DSRC (
				-- метаполя
				dwsjob, -- идентификатор загрузки
				dwsact, -- тип операции над записью. Если полная выгрузка - NULL
				dwssrcstamp, -- временая метка изменения записи. Если полная выгрузка - NULL
				
				-- поля таблицы-источника
				id, -- идентификатор человека
				phone -- номер телефона человека
			)
			SELECT
				-- метаполя
				p_id_job, -- идентификатор загрузки
				NULL, -- тип операции над записью. Если полная выгрузка - NULL
				NULL, -- временая метка изменения записи. Если полная выгрузка - NULL
			
				-- поля таблицы-источника
				id, -- идентификатор человека
				phone -- номер телефона человека
			FROM mike.phone_numbers;
		
			mike.logs.log(2, 'загрузка pn в DSRC завершена', CONSTANTS.dwi_title_lvl, 'nf3_pn_dwi', p_id_job);
		
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	
	    		mike.logs.log(3, 'error = ' || error_message, CONSTANTS.dwi_title_lvl, 'nf3_pn_dwi', p_id_job);
		END;
	END;

	-- процедура, которая загружает таблицу docs в свою DSRC
	PROCEDURE nf3_docs_dwi(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- обработка ошибок
			mike.logs.log(1, 'загрузка docs в DSRC', CONSTANTS.dwi_title_lvl, 'nf3_docs_dwi', p_id_job);

			INSERT INTO DWI002_3NF.documents002_DSRC (
				-- метаполя
				dwsjob, -- идентификатор загрузки
				dwsact, -- тип операции над записью. Если полная выгрузка - NULL
				dwssrcstamp, -- временая метка изменения записи. Если полная выгрузка - NULL
				
				-- поля таблицы-источника
				id, -- идентификатор человека
				inn, -- инн человека (набор цифр)
				pasport -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
			)
			SELECT
				-- метаполя
				p_id_job, -- идентификатор загрузки
				NULL, -- тип операции над записью. Если полная выгрузка - NULL
				NULL, -- временая метка изменения записи. Если полная выгрузка - NULL
			
				-- поля таблицы-источника
				id, -- идентификатор человека
				inn, -- инн человека (набор цифр)
				pasport -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
			FROM mike.documents;
		
		
			mike.logs.log(2, 'загрузка docs в DSRC завершена', CONSTANTS.dwi_title_lvl, 'nf3_docs_dwi', p_id_job);	
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	
	    		mike.logs.log(3, 'error = ' || error_message, CONSTANTS.dwi_title_lvl, 'nf3_docs_dwi', p_id_job);
		END;
	END;

	PROCEDURE nf3_hp_dwi(
		p_id_job NUMBER -- номер джоба
	)
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- обработка ошибок
			mike.logs.log(1, 'загрузка hp в DSRC', CONSTANTS.dwi_title_lvl, 'nf3_hp_dwi', p_id_job);

			INSERT INTO DWI002_3NF.human_params002_DSRC (
				-- метаполя
				dwsjob, -- идентификатор загрузки
				dwsact, -- тип операции над записью. Если полная выгрузка - NULL
				dwssrcstamp, -- временая метка изменения записи. Если полная выгрузка - NULL
				
				-- поля таблицы-источника
				id, -- идентификатор человека
				weight, -- вес человека 
				height -- рост человека
			)
			SELECT
				-- метаполя
				p_id_job, -- идентификатор загрузки
				NULL, -- тип операции над записью. Если полная выгрузка - NULL
				NULL, -- временая метка изменения записи. Если полная выгрузка - NULL
			
				-- поля таблицы-источника
				id, -- идентификатор человека
				weight, -- вес человека 
				height -- рост человека
			FROM mike.human_params;
		
			mike.logs.log(2, 'загрузка hp в DSRC завершена', CONSTANTS.dwi_title_lvl, 'nf3_hp_dwi', p_id_job);	
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	
	    		mike.logs.log(3, 'error = ' || error_message, CONSTANTS.dwi_title_lvl, 'nf3_hp_dwi', p_id_job);
		END;
	END;
END;

-- тест

/*

begin
	mike.dwi.wrap_dwi();
end;

*/