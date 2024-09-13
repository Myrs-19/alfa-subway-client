INSERT INTO mike.personal_information (
	id, -- идентификатор человека
	name, -- имя человека
	birthday, -- день рождения человека
	age -- возраст человека
) VALUES (
	1,
	'name',
	sysdate,
	21
);

SELECT * FROM mike.personal_information;