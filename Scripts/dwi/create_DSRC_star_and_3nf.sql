-- создание схемы для интерфейсного уровня для источника звезда - 001
-- выполнять под test
--DROP USER DWI001_STAR CASCADE;
CREATE USER DWI001_STAR IDENTIFIED BY 123;
-- через пользователя test
GRANT CREATE SESSION TO DWI001_STAR;
ALTER USER DWI001_STAR quota unlimited ON system;

-- создание таблицы dsrc для источника - звезда
-- создавать под test
-- DROP TABLE DWI001_STAR.client001_DSRC;

CREATE TABLE DWI001_STAR.client001_DSRC (
	-- метаполя
	dwsjob NUMBER, -- идентификатор загрузки
	dwsact varchar2(1 char), -- тип операции над записью. Если полная выгрузка - NULL
	dwssrcstamp DATE, -- временая метка изменения записи. Если полная выгрузка - NULL

	-- поля таблицы-источника
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

-- даем доступ mike - выполнять команду под DWI001_STAR
GRANT INSERT, DELETE, SELECT, UPDATE ON DWI001_STAR.client001_DSRC TO mike;

-- просмотр таблицы
SELECT * FROM DWI001_STAR.client001_DSRC;

-- создание схемы для интерфейсного уровня для источника 3нф - 002
-- выполнять под test
--DROP USER DWI002_3NF CASCADE;
CREATE USER DWI002_3NF IDENTIFIED BY 123;
-- через пользователя test
GRANT CREATE SESSION TO DWI002_3NF;
ALTER USER DWI002_3NF quota unlimited ON system;

-- создание dsrc для pi
-- выполнять под test
-- DROP TABLE DWI002_3NF.personal_information002_DSRC;

CREATE TABLE DWI002_3NF.personal_information002_DSRC(
	-- метаполя
	dwsjob NUMBER, -- идентификатор загрузки
	dwsact varchar2(1 char), -- тип операции над записью. Если полная выгрузка - NULL
	dwssrcstamp DATE, -- временая метка изменения записи. Если полная выгрузка - NULL
	
	-- поля таблицы-источника
	id NUMBER, -- идентификатор человека
	name VARCHAR2(256 CHAR), -- имя человека
	birthday DATE, -- день рождения человека
	age NUMBER -- возраст человека
);

-- даем доступ mike - выполнять команду под DWI002_3NF
GRANT INSERT, DELETE, SELECT, UPDATE ON DWI002_3NF.personal_information002_DSRC TO mike;

-- просмотр таблицы
SELECT * FROM DWI002_3NF.personal_information002_DSRC;

-- создание dsrc для pn
-- выполнять под test
-- DROP TABLE DWI002_3NF.phone_numbers002_DSRC;

CREATE TABLE DWI002_3NF.phone_numbers002_DSRC(
	-- метаполя
	dwsjob NUMBER, -- идентификатор загрузки
	dwsact varchar2(1 char), -- тип операции над записью. Если полная выгрузка - NULL
	dwssrcstamp DATE, -- временая метка изменения записи. Если полная выгрузка - NULL
	
	-- поля таблицы-источника
	id NUMBER, -- идентификатор человека
	phone VARCHAR2(11 CHAR) -- номер телефона человека
);

-- даем доступ mike - выполнять команду под DWI002_3NF
GRANT INSERT, DELETE, SELECT, UPDATE ON DWI002_3NF.phone_numbers002_DSRC TO mike;

-- просмотр таблицы
SELECT * FROM DWI002_3NF.phone_numbers002_DSRC;

-- создание dsrc для docs
-- выполнять под test
-- DROP TABLE DWI002_3NF.documents002_DSRC;

CREATE TABLE DWI002_3NF.documents002_DSRC(
	-- метаполя
	dwsjob NUMBER, -- идентификатор загрузки
	dwsact varchar2(1 char), -- тип операции над записью. Если полная выгрузка - NULL
	dwssrcstamp DATE, -- временая метка изменения записи. Если полная выгрузка - NULL
	
	-- поля таблицы-источника
	id NUMBER, -- идентификатор человека
	inn NUMBER, -- инн человека (набор цифр)
	pasport NUMBER-- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
);

-- даем доступ mike - выполнять команду под DWI002_3NF
GRANT INSERT, DELETE, SELECT, UPDATE ON DWI002_3NF.documents002_DSRC TO mike;

-- просмотр таблицы
SELECT * FROM DWI002_3NF.documents002_DSRC;

-- создание dsrc для hp
-- выполнять под test
-- DROP TABLE DWI002_3NF.human_params002_DSRC;

CREATE TABLE DWI002_3NF.human_params002_DSRC(
	-- метаполя
	dwsjob NUMBER, -- идентификатор загрузки
	dwsact varchar2(1 char), -- тип операции над записью. Если полная выгрузка - NULL
	dwssrcstamp DATE, -- временая метка изменения записи. Если полная выгрузка - NULL
	
	-- поля таблицы-источника
	id NUMBER, -- идентификатор человека
	weight REAL, -- вес человека 
	height REAL -- рост человека
);

-- даем доступ mike - выполнять команду под DWI002_3NF
GRANT INSERT, DELETE, SELECT, UPDATE ON DWI002_3NF.human_params002_DSRC TO mike;

-- просмотр таблицы
SELECT * FROM DWI002_3NF.human_params002_DSRC;












