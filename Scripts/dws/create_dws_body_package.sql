CREATE OR REPLACE PACKAGE BODY mike.dws IS

	-- процедура обертка над всем уровнем dws
	-- в ней генерится номер джоба и передается на след уровни обработки в рамках интерфейсного staging уровня
	-- она же имеет джоб, который запускается каждые 5 минут
	PROCEDURE wrap_dws
	IS
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
		
			-- определение номера джоба
			-- любая таблица-источник - максимальный номер джоба + 1
			SELECT nvl(max(job_number), 0) + 1
			INTO l_id_job
			FROM mike.orchestrator_alfa
			WHERE 
				title_object = CONSTANTS.table_source_title_star_clnt 
				AND 
				staging_lvl = CONSTANTS.dws_title_lvl;
			
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    	
		END;
	END;

END;