-- delete from mike.personal_information;

INSERT INTO mike.personal_information (
	id, -- идентификатор человека
	name, -- имя человека
	birthday, -- день рождения человека
	age -- возраст человека
) VALUES (
	1,
	'123123123',
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
	3,
	'mike',
	DATE '2003-06-12',
	2222
);

SELECT * FROM mike.personal_information;

UPDATE mike.personal_information 
	SET name = 'z'
	WHERE id = 2;

DELETE FROM mike.personal_information 
	WHERE id = 1;
	
UPDATE mike.personal_information 
	SET name = 'tetete'
	WHERE id = 3;