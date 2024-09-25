-- создание таблицы с приоритетами исходных систем

-- drop table mike.priority_attrs;

CREATE TABLE mike.priority_attrs (
	attr VARCHAR2(256 CHAR), -- название атрибута
	dwseid NUMBER, -- код системы источника
	priority NUMBER -- приоритет системы источникаы
);