-- создание таблиц источников в 3НФ

-- таблица с личной информацей клиента
DROP TABLE mike.personal_information;

CREATE TABLE mike.personal_information (
	id NUMBER, -- идентификатор человека
	name VARCHAR2(256 CHAR), -- имя человека
	birthday DATE, -- день рождения человека
	age NUMBER -- возраст человека
);

-- таблица с номерами телефонов клиентов
DROP TABLE mike.phone_numbers;

CREATE TABLE mike.phone_numbers (
	id NUMBER, -- идентификатор человека
	phone VARCHAR2(11 CHAR) -- номер телефона человека
);

-- таблица с документами клиентов
DROP TABLE mike.documents;

CREATE TABLE mike.documents(
	id NUMBER, -- идентификатор человека
	inn NUMBER, -- инн человека (набор цифр)
	pasport NUMBER-- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
);

-- таблица с параметрами человека
DROP TABLE mike.human_params;

CREATE TABLE mike.human_params(
	id NUMBER, -- идентификатор человека
	weight REAL, -- вес человека 
	height REAL -- рост человека
);

-- просмотр таблиц
SELECT * FROM mike.personal_information;
SELECT * FROM mike.phone_numbers;
SELECT * FROM mike.documents;
SELECT * FROM mike.human_params;