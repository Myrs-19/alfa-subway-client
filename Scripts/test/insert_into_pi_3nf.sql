-- delete from mike.personal_information;

INSERT INTO mike.personal_information (
	id, -- идентификатор человека
	name, -- имя человека
	birthday, -- день рождения человека
	age -- возраст человека
) VALUES (
	2,
	'name',
	DATE '2019-01-01',
	21
);

INSERT INTO mike.personal_information (
	id, -- идентификатор человека
	name, -- имя человека
	birthday, -- день рождения человека
	age -- возраст человека
) VALUES (
	2,
	'name_test_double',
	DATE '2019-01-01',
	21
);

INSERT INTO mike.personal_information (
	id, -- идентификатор человека
	name, -- имя человека
	birthday, -- день рождения человека
	age -- возраст человека
) VALUES (
	1,
	'mike',
	DATE '2003-06-12',
	22
);

SELECT * FROM mike.personal_information;

UPDATE mike.personal_information 
	SET name = 'mike change'
	WHERE id = 1;

DELETE FROM mike.personal_information 
	WHERE id = 1;
	
