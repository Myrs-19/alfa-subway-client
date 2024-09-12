-- создание пакета dwi
CREATE OR REPLACE PACKAGE mike.dwi AS
	-- процедура обертка над всем уровнем dwi
	-- в ней генерится номер джоба и передается на след уровни обработки в рамках интерфейсного уровня
	-- она же имеет джоб, который запускается каждые 5 минут
	PROCEDURE wrap_dwi;
	
--	 процедура, которая загружает данные в свою таблицу DSRC
	PROCEDURE star_clnt_dwi(
		p_id_job NUMBER -- номер джоба
	);	
	
--	 процедура-обертка, которая вызывает все загрузки каждой таблицы 3НФ в свои таблицы DSRC
	PROCEDURE wrap_3nf_dwi(
		p_id_job NUMBER -- номер джоба
	);	
	
--	 процедура, которая загружает таблицу pi в свою DSRC
	PROCEDURE nf3_pi_dwi(
		p_id_job NUMBER -- номер джоба
	);

--	 процедура, которая загружает таблицу pn в свою DSRC
	PROCEDURE nf3_pn_dwi(
		p_id_job NUMBER -- номер джоба
	);

--	 процедура, которая загружает таблицу docs в свою DSRC
	PROCEDURE nf3_docs_dwi(
		p_id_job NUMBER -- номер джоба
	);

--	 процедура, которая загружает таблицу hp в свою DSRC
	PROCEDURE nf3_hp_dwi(
		p_id_job NUMBER -- номер джоба
	);
END;