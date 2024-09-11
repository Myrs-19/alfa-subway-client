-- создание таблиц источников в звезде

DROP TABLE mike.star_client;

CREATE TABLE mike.star_client(
	id NUMBER, -- идентификатор человека
	name varchar2(128 CHAR), -- имя человека
	birthday DATE, -- день рождения человека
	age NUMBER, -- возраст человека
	phone VARCHAR2(16 CHAR), -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
	inn VARCHAR2(14 CHAR), -- инн человека (формат - ххх-ххх-ххх хх)
	pasport NUMBER, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
	weight REAL, -- вес человека 
	height REAL-- рост человека
);

-- просмотр таблиц
SELECT * FROM mike.star_client;