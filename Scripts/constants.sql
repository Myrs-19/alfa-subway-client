
-- создание пакета, который хранит константы
CREATE OR REPLACE PACKAGE CONSTANTS AS
	min_balance CONSTANT REAL := 10.00;
	max_balance CONSTANT REAL := 10.00;
END;

