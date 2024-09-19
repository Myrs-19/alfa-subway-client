-- создание таблицы и схемы для stagin уровня

-- через пользователя test
-- DROP USER STG CASCADE;
CREATE USER STG IDENTIFIED BY 123;
GRANT CREATE SESSION TO STG;
ALTER USER STG quota unlimited ON system;

-- CDELTA
-- выполнять под test
-- drop table STG.CLIENT_CDELTA;

CREATE TABLE STG.CLIENT_CDELTA (
	-- ключи dwh
	nk NUMBER, -- целочисленный ключ dwh
	
	-- метаполя
	dwseid NUMBER, -- идентификатор исходной таблицы-источника записи
	dwsjob NUMBER, -- идентификатор загрузки, в рамках которой произошло изменение записи
	dwsact varchar2(1 char), -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
	dwsuniact varchar2(1 char), -- ('I', 'U', 'N' - логический ключ не изменился)
	
	-- поля атрибутов измерения
	id NUMBER, -- идентификатор человека
	name varchar2(128 CHAR), -- имя человека
	birthday DATE, -- день рождения человека
	age NUMBER, -- возраст человека
	phone NUMBER, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
	inn NUMBER, -- инн человека (формат - ххх-ххх-ххх хх)
	pasport NUMBER, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
	weight REAL, -- вес человека 
	height REAL-- рост человека
);

-- выполнять под STG
GRANT INSERT, DELETE, SELECT, UPDATE ON STG.CLIENT_CDELTA TO mike;

-- выполнять под mike
SELECT * FROM STG.CLIENT_CDELTA;

-- uklink
-- выполнять под test
-- drop table STG.CLIENT_UKLINK;

CREATE TABLE STG.CLIENT_UKLINK (
		-- ключи dwh
	nk NUMBER, -- целочисленный ключ dwh
	uk NUMBER, -- унифицированный ключ измерения в dwh
	
	-- метаполя
	dwsjob NUMBER, -- идентификатор загрузки, в рамках которой произошло изменение записи
	manual varchar2(1 char) -- флаг ручной унификации записи ('Y' - вручную, 'N' - иначе)
);

-- выполнять под STG
GRANT INSERT, DELETE, SELECT, UPDATE ON STG.CLIENT_UKLINK TO mike;

-- выполнять под mike
SELECT * FROM STG.CLIENT_UKLINK;

-- context
-- выполнять под test
-- drop table STG.CLIENT_CONTEXT;
CREATE TABLE STG.CLIENT_CONTEXT (
	-- ключи dwh
	nk NUMBER, -- целочисленный ключ dwh
	uk NUMBER, -- унифицированный ключ измерения в dwh
	
	-- метаполя
	dwseid NUMBER, -- идентификатор исходной таблицы-источника записи
	dwsjob NUMBER, -- идентификатор загрузки, в рамках которой произошло изменение записи
	
	dwsarchive varchar2(1 char), -- признак удаленной записи ('D' - удалена, NULL - иначе)
	dwsuniact varchar2(1 char), -- признак необходимости переунификации записи 
								-- ('I' - новая и необходима унификация,
								--	'U' - необходима переунификация,
								--	'N' - унификация не требуется)
	manual varchar2(1 char), -- флаг ручной унификации записи ('Y' - вручную, 'N' - иначе)
	
	-- поля атрибутов измерения
	id NUMBER, -- идентификатор человека
	name varchar2(128 CHAR), -- имя человека
	birthday DATE, -- день рождения человека
	age NUMBER, -- возраст человека
	phone NUMBER, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
	inn NUMBER, -- инн человека (формат - ххх-ххх-ххх хх)
	pasport NUMBER, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
	weight REAL, -- вес человека 
	height REAL-- рост человека
);

-- выполнять под STG
GRANT INSERT, DELETE, SELECT, UPDATE ON STG.CLIENT_CONTEXT TO mike;

-- выполнять под mike
SELECT * FROM STG.CLIENT_CONTEXT;

-- wdelta
-- выполнять под test
-- drop table STG.CLIENT_WDELTA;

CREATE TABLE STG.CLIENT_WDELTA (
	-- ключи dwh
	uk NUMBER, -- унифицированный ключ измерения в dwh
	
	-- метаполя
	dwsact varchar2(1 char), -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
	dwsemix NUMBER, -- код сочетания исходных таблиц (внешний ключ на таблицу DWSEMIX)
	dwsjob NUMBER, -- идентификатор загрузки, в рамках которой произошло изменение записи
	
	-- поля атрибутов измерения
	id NUMBER, -- идентификатор человека
	name varchar2(128 CHAR), -- имя человека
	birthday DATE, -- день рождения человека
	age NUMBER, -- возраст человека
	phone NUMBER, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
	inn NUMBER, -- инн человека (формат - ххх-ххх-ххх хх)
	pasport NUMBER, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
	weight REAL, -- вес человека 
	height REAL-- рост человека
);

-- даем доступ mike - выполнять команду под STG
GRANT INSERT, DELETE, SELECT, UPDATE ON STG.CLIENT_WDELTA TO mike;

SELECT * FROM STG.CLIENT_WDELTA;



