
-- создание таблицы оркестратора
DROP TABLE mike.orchestrator_alfa;

CREATE TABLE mike.orchestrator_alfa (
	title_object varchar2(256 char), -- название объекта job
	job_number NUMBER, -- номер job
	is_successful NUMBER, -- были ли ошибки при выполнении уровня для этого номера job (-1 - не успешно, 1 - успешно)
	staging_lvl varchar2(256 char), --(выгрузка, интерфейсный, интерфейсный staging, staging)
	need_process NUMBER -- требует ли обработки на след уровне? (-1 - не требует, 1 - требует)
);

-- создание пакета, который будет хранить процедуру для оркестратора
CREATE OR REPLACE PACKAGE mike.orchestrator AS 
	PROCEDURE insert_into_orchestrator(
		p_title_object varchar2, -- название объекта
		p_is_successful NUMBER, -- успешно ли выполнился
		p_staging_lvl varchar2, --(выгрузка, интерфейсный, интерфейсный staging, staging)
		p_job_number NUMBER, -- номер джоба обработки
		p_need_process NUMBER -- требуется дальнейшая обработка?
	);
END;

CREATE OR REPLACE PACKAGE BODY mike.orchestrator IS 
	PROCEDURE insert_into_orchestrator (
		p_title_object varchar2, -- название объекта
		p_is_successful NUMBER, -- успешно ли выполнился
		p_staging_lvl varchar2, --(выгрузка, интерфейсный, интерфейсный staging, staging)
		p_job_number NUMBER, -- номер джоба обработки
		p_need_process NUMBER -- требуется дальнейшая обработка?
	)
	IS
		error_message varchar2(256 CHAR);
		check_update NUMBER;
	BEGIN 
		BEGIN -- для отлавливания ошибок
			-- проверка на присутствие записи в таблице оркестраторе
			SELECT nvl(max(1), -1)
			INTO check_update
			FROM mike.orchestrator_alfa o_a
			WHERE p_title_object = title_object
				AND p_staging_lvl = staging_lvl
				AND p_job_number = job_number;
			
			IF check_update = 1 THEN 
				UPDATE mike.orchestrator_alfa SET
					title_object = p_title_object,
					job_number = p_job_number,
					is_successful = p_is_successful,
					staging_lvl = p_staging_lvl,
					need_process = p_need_process
				WHERE title_object = p_title_object
					AND staging_lvl = p_staging_lvl
					AND job_number = p_job_number;
			ELSE 
				INSERT INTO mike.orchestrator_alfa (
					title_object, -- название объекта job
					job_number, -- номер job
					is_successful, -- успешно ли выполнился job
					staging_lvl, --(выгрузка, интерфейсный, интерфейсный staging, staging)
					need_process -- требуется дальнейшая обработка?
				) VALUES (
					p_title_object, 
					p_job_number,
					p_is_successful,
					p_staging_lvl,
					1
				);
			END IF;
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    		INSERT INTO mike.orchestrator_alfa (
					title_object, -- название объекта job
					job_number, -- номер job
					is_successful, -- успешно ли выполнился job
					staging_lvl, --(выгрузка, интерфейсный, интерфейсный staging, staging)
					need_process -- требуется дальнейшая обработка?
				) VALUES (
					p_title_object, 
					p_job_number,
					-1,
					p_staging_lvl,
					1
				);
		END;
	END;
END;

-- тест
/*
BEGIN
	mike.orchestrator.insert_into_orchestrator('test', 1, 'test', 1, 1);
END;

SELECT * FROM mike.orchestrator_alfa;
*/