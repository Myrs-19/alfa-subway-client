-- создание схем для интерфейсного уровня staging

-- ИСТОЧНИК-ЗВЕЗДА

-- через пользователя test
--DROP USER DWS001_CLNT CASCADE;
CREATE USER DWS001_CLNT IDENTIFIED BY 123;
GRANT CREATE SESSION TO DWS001_CLNT;
ALTER USER DWS001_CLNT quota unlimited ON system;

-- создание nklink для client
CREATE TABLE DWS001_CLNT.CLIENT001_NKLINK (
	nk NUMBER,
	
	-- pk исходной системы
	id number, -- идентификатор человека
	
	-- метаполя
	dwsjob NUMBER,
	dwsauto varchar2(1 char) -- признак записи сгенерированной автоматически при загрузке фактов, Y - автоматически, NULL - нет
);

-- даем доступ test - выполнять команду под DWS001_CLNT
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS001_CLNT.CLIENT001_NKLINK TO mike;

-- выполнять под mike
SELECT * FROM DWS001_CLNT.CLIENT001_NKLINK;


-- создание mirror для client
CREATE TABLE DWS001_CLNT.CLIENT001_MIRROR (
	nk NUMBER,	
	
	-- метаполя
	dwsjob NUMBER,
	dwsauto varchar2(1 char), -- признак записи сгенерированной автоматически при загрузке фактов, Y - автоматически, NULL - нет
	dwsarchive varchar2(1 char), -- признак удаленной записи , D - удалена, NULL - нет
	
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

-- даем доступ test - выполнять команду под DWS001_CLNT
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS001_CLNT.CLIENT001_MIRROR TO mike;

-- выполнять под mike
SELECT * FROM DWS001_CLNT.CLIENT001_MIRROR;


-- создание delta для client
CREATE TABLE DWS001_CLNT.CLIENT001_DELTA (
	nk NUMBER,	
	
	-- метаполя
	dwsjob NUMBER,
	dwsact varchar2(1 char), -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
	dwsuniact varchar2(1 char), -- ('I', 'U', 'N' - логический ключ не изменился)
		
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

-- даем доступ test - выполнять команду под DWS001_CLNT
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS001_CLNT.CLIENT001_DELTA TO mike;

-- выполнять под mike
SELECT * FROM DWS001_CLNT.CLIENT001_DELTA;


-- создание double для client
-- drop table DWS001_CLNT.CLIENT001_DOUBLE;

CREATE TABLE DWS001_CLNT.CLIENT001_DOUBLE (
	-- метаполя
	dwsjob NUMBER,
	as_of_day DATE, -- дата актуализации DWH
	effective_flag varchar2(1 char), -- 'Y' - если запись эфективна в группе дублей
		
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

-- даем доступ test - выполнять команду под DWS001_CLNT
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS001_CLNT.CLIENT001_DOUBLE TO mike;

-- выполнять под mike
SELECT * FROM DWS001_CLNT.CLIENT001_DOUBLE;

-- ИСТОЧНИК-3НФ

-- через пользователя test
--DROP USER DWS002_3NF CASCADE;
CREATE USER DWS002_3NF IDENTIFIED BY 123;
GRANT CREATE SESSION TO DWS002_3NF;
ALTER USER DWS002_3NF quota unlimited ON system;

------------------ PI ------------------------

-- создание nklink для pi
CREATE TABLE DWS002_3NF.PI001_NKLINK (
	nk NUMBER,
	
	-- pk исходной системы
	id number, -- идентификатор человека
	
	-- метаполя
	dwsjob NUMBER,
	dwsauto varchar2(1 char) -- признак записи сгенерированной автоматически при загрузке фактов, Y - автоматически, NULL - нет
);

-- даем доступ test - выполнять команду под DWS002_3NF
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS002_3NF.PI001_NKLINK TO mike;

-- выполнять под mike
SELECT * FROM DWS002_3NF.PI001_NKLINK;


-- создание mirror для pi
-- drop table DWS002_3NF.PI001_MIRROR;

CREATE TABLE DWS002_3NF.PI001_MIRROR (
	nk NUMBER,	
	
	-- метаполя
	dwsjob NUMBER,
	dwsauto varchar2(1 char), -- признак записи сгенерированной автоматически при загрузке фактов, Y - автоматически, NULL - нет
	dwsarchive varchar2(1 char), -- признак удаленной записи , D - удалена, NULL - нет
	
	-- поля таблицы-источника
	id NUMBER, -- идентификатор человека
	name VARCHAR2(256 CHAR), -- имя человека
	birthday DATE, -- день рождения человека
	age NUMBER, -- возраст человека
	inn NUMBER -- инн человека (набор цифр)
);

-- даем доступ test - выполнять команду под DWS002_3NF
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS002_3NF.PI001_MIRROR TO mike;

-- выполнять под mike
SELECT * FROM DWS002_3NF.PI001_MIRROR;


-- создание delta для pi
-- drop table DWS002_3NF.PI001_DELTA;

CREATE TABLE DWS002_3NF.PI001_DELTA (
	nk NUMBER,	
	
	-- метаполя
	dwsjob NUMBER,
	dwsact varchar2(1 char), -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
	dwsuniact varchar2(1 char), -- ('I', 'U', 'N' - логический ключ не изменился)
		
	-- поля таблицы-источника
	id NUMBER, -- идентификатор человека
	name VARCHAR2(256 CHAR), -- имя человека
	birthday DATE, -- день рождения человека
	age NUMBER, -- возраст человека
	inn NUMBER -- инн человека (набор цифр)
);

-- даем доступ test - выполнять команду под DWS001_CLNT
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS002_3NF.PI001_DELTA TO mike;

-- выполнять под mike
SELECT * FROM DWS002_3NF.PI001_DELTA;


-- создание double для pi
-- drop table DWS002_3NF.PI001_DOUBLE;

CREATE TABLE DWS002_3NF.PI001_DOUBLE (
	-- метаполя
	dwsjob NUMBER,
	as_of_day DATE, -- дата актуализации DWH
	effective_flag varchar2(1 char), -- 'Y' - если запись эфективна в группе дублей
		
	-- поля таблицы-источника
	id NUMBER, -- идентификатор человека
	name VARCHAR2(256 CHAR), -- имя человека
	birthday DATE, -- день рождения человека
	age NUMBER -- возраст человека
);

-- даем доступ test - выполнять команду под DWS002_3NF
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS002_3NF.PI001_DOUBLE TO mike;

-- выполнять под mike
SELECT * FROM DWS002_3NF.PI001_DOUBLE;

------------------ PI ------------------------


------------------ PN ------------------------

-- создание nklink для PN
CREATE TABLE DWS002_3NF.PN001_NKLINK (
	nk NUMBER,
	
	-- pk исходной системы
	id number, -- идентификатор человека
	
	-- метаполя
	dwsjob NUMBER,
	dwsauto varchar2(1 char) -- признак записи сгенерированной автоматически при загрузке фактов, Y - автоматически, NULL - нет
);

-- даем доступ test - выполнять команду под DWS002_3NF
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS002_3NF.PN001_NKLINK TO mike;

-- выполнять под mike
SELECT * FROM DWS002_3NF.PN001_NKLINK;


-- создание mirror для PN
-- drop table DWS002_3NF.PN001_MIRROR;

CREATE TABLE DWS002_3NF.PN001_MIRROR (
	nk NUMBER,	
	
	-- метаполя
	dwsjob NUMBER,
	dwsauto varchar2(1 char), -- признак записи сгенерированной автоматически при загрузке фактов, Y - автоматически, NULL - нет
	dwsarchive varchar2(1 char), -- признак удаленной записи , D - удалена, NULL - нет
	
	-- поля таблицы-источника
	id NUMBER, -- идентификатор человека
	phone VARCHAR2(11 CHAR), -- номер телефона человека
	inn NUMBER -- инн человека (набор цифр)
);

-- даем доступ test - выполнять команду под DWS002_3NF
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS002_3NF.PN001_MIRROR TO mike;

-- выполнять под mike
SELECT * FROM DWS002_3NF.PN001_MIRROR;


-- создание delta для PN
-- drop table DWS002_3NF.PN001_DELTA;

CREATE TABLE DWS002_3NF.PN001_DELTA (
	nk NUMBER,	
	
	-- метаполя
	dwsjob NUMBER,
	dwsact varchar2(1 char), -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
	dwsuniact varchar2(1 char), -- ('I', 'U', 'N' - логический ключ не изменился)
		
	-- поля таблицы-источника
	id NUMBER, -- идентификатор человека
	phone VARCHAR2(11 CHAR), -- номер телефона человека
	inn NUMBER -- инн человека (набор цифр)
);

-- даем доступ test - выполнять команду под DWS001_CLNT
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS002_3NF.PN001_DELTA TO mike;

-- выполнять под mike
SELECT * FROM DWS002_3NF.PN001_DELTA;


-- создание double для PN
-- drop table DWS002_3NF.PN001_DOUBLE;

CREATE TABLE DWS002_3NF.PN001_DOUBLE (
	-- метаполя
	dwsjob NUMBER,
	as_of_day DATE, -- дата актуализации DWH
	effective_flag varchar2(1 char), -- 'Y' - если запись эфективна в группе дублей
		
	-- поля таблицы-источника
	id NUMBER, -- идентификатор человека
	phone VARCHAR2(11 CHAR) -- номер телефона человека
);

-- даем доступ test - выполнять команду под DWS002_3NF
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS002_3NF.PN001_DOUBLE TO mike;

-- выполнять под mike
SELECT * FROM DWS002_3NF.PN001_DOUBLE;

------------------ PN ------------------------


------------------ DOCS ------------------------

-- создание nklink для DOCS
CREATE TABLE DWS002_3NF.DOCS001_NKLINK (
	nk NUMBER,
	
	-- pk исходной системы
	id number, -- идентификатор человека
	
	-- метаполя
	dwsjob NUMBER,
	dwsauto varchar2(1 char) -- признак записи сгенерированной автоматически при загрузке фактов, Y - автоматически, NULL - нет
);

-- даем доступ test - выполнять команду под DWS002_3NF
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS002_3NF.DOCS001_NKLINK TO mike;

-- выполнять под mike
SELECT * FROM DWS002_3NF.DOCS001_NKLINK;


-- создание mirror для DOCS
-- drop table DWS002_3NF.DOCS001_MIRROR;

CREATE TABLE DWS002_3NF.DOCS001_MIRROR (
	nk NUMBER,	
	
	-- метаполя
	dwsjob NUMBER,
	dwsauto varchar2(1 char), -- признак записи сгенерированной автоматически при загрузке фактов, Y - автоматически, NULL - нет
	dwsarchive varchar2(1 char), -- признак удаленной записи , D - удалена, NULL - нет
	
	-- поля таблицы-источника
	id NUMBER, -- идентификатор человека
	inn NUMBER, -- инн человека (набор цифр)
	pasport NUMBER -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
);

-- даем доступ test - выполнять команду под DWS002_3NF
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS002_3NF.DOCS001_MIRROR TO mike;

-- выполнять под mike
SELECT * FROM DWS002_3NF.DOCS001_MIRROR;


-- создание delta для DOCS
-- drop table DWS002_3NF.DOCS001_DELTA;

CREATE TABLE DWS002_3NF.DOCS001_DELTA (
	nk NUMBER,	
	
	-- метаполя
	dwsjob NUMBER,
	dwsact varchar2(1 char), -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
	dwsuniact varchar2(1 char), -- ('I', 'U', 'N' - логический ключ не изменился)
		
	-- поля таблицы-источника
	id NUMBER, -- идентификатор человека
	inn NUMBER, -- инн человека (набор цифр)
	pasport NUMBER, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
	inn NUMBER -- инн человека (набор цифр)
);

-- даем доступ test - выполнять команду под DWS001_CLNT
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS002_3NF.DOCS001_DELTA TO mike;

-- выполнять под mike
SELECT * FROM DWS002_3NF.DOCS001_DELTA;


-- создание double для DOCS
-- drop table DWS002_3NF.DOCS001_DOUBLE;

CREATE TABLE DWS002_3NF.DOCS001_DOUBLE (
	-- метаполя
	dwsjob NUMBER,
	as_of_day DATE, -- дата актуализации DWH
	effective_flag varchar2(1 char), -- 'Y' - если запись эфективна в группе дублей
		
	-- поля таблицы-источника
	id NUMBER, -- идентификатор человека
	inn NUMBER, -- инн человека (набор цифр)
	pasport NUMBER-- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
);

-- даем доступ test - выполнять команду под DWS002_3NF
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS002_3NF.DOCS001_DOUBLE TO mike;

-- выполнять под mike
SELECT * FROM DWS002_3NF.DOCS001_DOUBLE;

------------------ DOCS ------------------------


------------------ HP ------------------------

-- создание nklink для HP
CREATE TABLE DWS002_3NF.HP001_NKLINK (
	nk NUMBER,
	
	-- pk исходной системы
	id number, -- идентификатор человека
	
	-- метаполя
	dwsjob NUMBER,
	dwsauto varchar2(1 char) -- признак записи сгенерированной автоматически при загрузке фактов, Y - автоматически, NULL - нет
);

-- даем доступ test - выполнять команду под DWS002_3NF
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS002_3NF.HP001_NKLINK TO mike;

-- выполнять под mike
SELECT * FROM DWS002_3NF.HP001_NKLINK;


-- создание mirror для HP
-- drop table DWS002_3NF.HP001_MIRROR;

CREATE TABLE DWS002_3NF.HP001_MIRROR (
	nk NUMBER,	
	
	-- метаполя
	dwsjob NUMBER,
	dwsauto varchar2(1 char), -- признак записи сгенерированной автоматически при загрузке фактов, Y - автоматически, NULL - нет
	dwsarchive varchar2(1 char), -- признак удаленной записи , D - удалена, NULL - нет
	
	-- поля таблицы-источника
	id NUMBER, -- идентификатор человека
	weight REAL, -- вес человека 
	height REAL, -- рост человека
	inn NUMBER -- инн человека (набор цифр)
);

-- даем доступ test - выполнять команду под DWS002_3NF
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS002_3NF.HP001_MIRROR TO mike;

-- выполнять под mike
SELECT * FROM DWS002_3NF.HP001_MIRROR;


-- создание delta для HP
-- drop table DWS002_3NF.HP001_DELTA;

CREATE TABLE DWS002_3NF.HP001_DELTA (
	nk NUMBER,	
	
	-- метаполя
	dwsjob NUMBER,
	dwsact varchar2(1 char), -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
	dwsuniact varchar2(1 char), -- ('I', 'U', 'N' - логический ключ не изменился)
		
	-- поля таблицы-источника
	id NUMBER, -- идентификатор человека
	weight REAL, -- вес человека 
	height REAL, -- рост человека
	inn NUMBER -- инн человека (набор цифр)
);

-- даем доступ test - выполнять команду под DWS001_CLNT
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS002_3NF.HP001_DELTA TO mike;

-- выполнять под mike
SELECT * FROM DWS002_3NF.HP001_DELTA;


-- создание double для HP
-- drop table DWS002_3NF.HP001_DOUBLE;

CREATE TABLE DWS002_3NF.HP001_DOUBLE (
	-- метаполя
	dwsjob NUMBER,
	as_of_day DATE, -- дата актуализации DWH
	effective_flag varchar2(1 char), -- 'Y' - если запись эфективна в группе дублей
		
	-- поля таблицы-источника
	id NUMBER, -- идентификатор человека
	weight REAL, -- вес человека 
	height REAL -- рост человека
);

-- даем доступ test - выполнять команду под DWS002_3NF
GRANT INSERT, DELETE, SELECT, UPDATE ON DWS002_3NF.HP001_DOUBLE TO mike;

-- выполнять под mike
SELECT * FROM DWS002_3NF.HP001_DOUBLE;

------------------ HP ------------------------
