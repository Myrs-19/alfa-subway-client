-- дубли - все поля одинаковые
INSERT INTO mike.star_client(
	id, -- идентификатор человека
	name, -- имя человека
	birthday, -- день рождения человека
	age, -- возраст человека
	phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
	inn, -- инн человека (формат - ххх-ххх-ххх хх)
	pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
	weight, -- вес человека 
	height -- рост человека
)
SELECT 
	1,
	't',
	sysdate,
	1,
	'1',
	'1',
	1,
	1.0,
	1.0
FROM dual
CONNECT BY LEVEL <= 1;

-- запись для изменений
INSERT INTO mike.star_client(
	id, -- идентификатор человека
	name, -- имя человека
	birthday, -- день рождения человека
	age, -- возраст человека
	phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
	inn, -- инн человека (формат - ххх-ххх-ххх хх)
	pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
	weight, -- вес человека 
	height -- рост человека
)
SELECT 
	1,
	't',
	DATE '2021-01-01',
	1,
	'1',
	'11',
	1,
	1.0,
	1.0
FROM dual;

/*

-- выполнять для другого джоба (следующего)
update mike.star_client set
name = 'mike'
where id = 2;

 */

SELECT * FROM mike.star_client;

/*

-- меняю лог ключ у id = 1
update mike.star_client set
inn = '123'
where id = 2;

delete from mike.star_client
where rowid = (
	select min(rowid)
	from mike.star_client
	where id = 1
)

*/

SELECT * FROM mike.star_client;

-- +7(ххх)ххх-хх-хх
-- ххх-ххх-ххх хх
UPDATE mike.star_client SET 
	phone = '+7(999)655-11-11',
	inn = '123-123-111 11';

UPDATE mike.star_client SET 
	birthday = DATE '2020-01-01'
WHERE id = 1;

UPDATE mike.star_client SET 
	birthday = DATE '2003-01-25'
WHERE id = 2;



