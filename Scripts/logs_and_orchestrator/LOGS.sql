
--создание таблицы с логами
DROP TABLE mike.logs_alfa;

CREATE TABLE mike.logs_alfa (
	dt DATE, -- дата лога
	num NUMBER, -- номер лога в процедуре
	description VARCHAR2(1024 char), -- описание лога
	staging_dwh_lvl VARCHAR2(1024 char), -- название уровня обработки
	program_title VARCHAR2(1024 char), -- название программы, в которой записывается лог
	id_job NUMBER -- номер джоба, в котором записывался лог
);

-- создание пакета, который будет хранить процедуру для логирования
CREATE OR REPLACE PACKAGE mike.LOGS AS 
	PROCEDURE log(
		num NUMBER, -- номер лога в процедуре
		description VARCHAR2, -- описание лога
		staging_dwh_lvl VARCHAR2, -- название уровня обработки
		program_title VARCHAR2, -- название программы, в которой записывается лог
		id_job NUMBER -- номер джоба, в котором записывался лог
	);
END;

-- создание тела пакета
CREATE OR REPLACE PACKAGE BODY mike.LOGS IS 
	PROCEDURE log(
		num NUMBER, -- номер лога в процедуре
		description VARCHAR2, -- описание лога
		staging_dwh_lvl VARCHAR2, -- название уровня обработки
		program_title VARCHAR2, -- название программы, в которой записывается лог
		id_job NUMBER -- номер джоба, в котором записывался лог
	)
	IS 
		error_message varchar2(256 CHAR);
	BEGIN
		BEGIN -- для отлавливания ошибок
			INSERT INTO mike.logs_alfa (
				dt, -- дата лога
				num, -- номер лога в процедуре
				description, -- описание лога
				staging_dwh_lvl, -- название уровня обработки
				program_title, -- название программы, в которой записывается лог
				id_job -- номер джоба, в котором записывался лог
			) VALUES (
				sysdate,
				num,
				description,
				staging_dwh_lvl,
				program_title,
				id_job
			);
		EXCEPTION
	    	WHEN OTHERS THEN
	    		error_message := SUBSTR(SQLERRM, 1, 256);
	    		INSERT INTO mike.logs_alfa (
					dt, -- дата лога
					num, -- номер лога в процедуре
					description, -- описание лога
					staging_dwh_lvl, -- название уровня обработки
					program_title, -- название программы, в которой записывается лог
					id_job -- номер джоба, в котором записывался лог
				) VALUES (
					sysdate,
					num,
					error_message,
					staging_dwh_lvl,
					program_title,
					id_job
				);
	    END;
	END;
END;

-- тест
/*
BEGIN
	mike.logs.log(1, 'test', 'test', 'test', 1);
END;

SELECT * FROM mike.LOGS_ALFA la;
*/
