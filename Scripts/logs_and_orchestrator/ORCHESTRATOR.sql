
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
		title_object varchar2, -- название объекта
		is_successful NUMBER, -- успешно ли выполнился
		staging_lvl varchar2, --(выгрузка, интерфейсный, интерфейсный staging, staging)
		job_number NUMBER, -- номер джоба обработки
		need_process NUMBER -- требуется дальнейшая обработка?
	);
END;

CREATE OR REPLACE PACKAGE BODY mike.orchestrator IS 
	PROCEDURE insert_into_orchestrator (
		title_object varchar2, -- название объекта
		is_successful NUMBER, -- успешно ли выполнился
		staging_lvl varchar2, --(выгрузка, интерфейсный, интерфейсный staging, staging)
		job_number NUMBER, -- номер джоба обработки
		need_process NUMBER -- требуется дальнейшая обработка?
	)
	IS
		result_query varchar2(256 CHAR);
	BEGIN 
		BEGIN -- для отлавливания ошибок
			INSERT INTO mike.orchestrator_alfa (
				title_object, -- название объекта job
				job_number, -- номер job
				is_successful, -- успешно ли выполнился job
				staging_lvl, --(выгрузка, интерфейсный, интерфейсный staging, staging)
				need_process -- требуется дальнейшая обработка?
			) VALUES (
				title_object, 
				job_number,
				is_successful,
				staging_lvl,
				1
			);
		EXCEPTION
	    	WHEN OTHERS THEN
	    		result_query := SUBSTR(SQLERRM, 1, 256);
	    		INSERT INTO mike.orchestrator_alfa (
					title_object, -- название объекта job
					job_number, -- номер job
					is_successful, -- успешно ли выполнился job
					staging_lvl, --(выгрузка, интерфейсный, интерфейсный staging, staging)
					need_process -- требуется дальнейшая обработка?
				) VALUES (
					title_object, 
					job_number,
					-1,
					staging_lvl,
					1
				);
		END;
	END;
END;

-- тест
/*
BEGIN
	orchestrator.insert_into_orchestrator('test', 1, 'test', 1, 1);
END;

SELECT * FROM mike.orchestrator_alfa;
*/