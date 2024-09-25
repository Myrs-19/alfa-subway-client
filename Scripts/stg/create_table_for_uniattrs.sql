-- таблица для хранения типов унификации для атрибутов
/*

коды для типов унификации (английские буквы):
	1) Приоритет - "P"
	2) Агрегация - "A"
	
*/

-- drop table mike.uni_attrs;

CREATE TABLE mike.uni_attrs(
	attr VARCHAR2(128 CHAR),
	uni_attr VARCHAR2(1 CHAR)
);