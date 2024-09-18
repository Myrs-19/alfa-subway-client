-- создание промежуточной таблицы для унификации записи
-- таблица для унификации записи - для нее генерим uk, а потом select`ом из нее выбираем максимальный
-- uk для записей, у которых одинаковое значение inn столбца

-- выполнять под mike

-- drop table mike.staging_uni_record;

CREATE TABLE mike.staging_uni_record (
	nk NUMBER,
	uk NUMBER,
	
	dwsjob NUMBER,
	manual VARCHAR2(1 CHAR),
	
	inn NUMBER
);

SELECT * FROM mike.staging_uni_record;