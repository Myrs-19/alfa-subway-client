INSERT INTO mike.phone_numbers (
	id, -- идентификатор человека
	phone -- номер телефона человека
) VALUES (
	1, 
	'123'
);

INSERT INTO mike.phone_numbers (
	id, -- идентификатор человека
	phone -- номер телефона человека
) VALUES (
	1, 
	'123'
);

INSERT INTO mike.phone_numbers (
	id, -- идентификатор человека
	phone -- номер телефона человека
) VALUES (
	2, 
	'133'
);

INSERT INTO mike.phone_numbers (
	id, -- идентификатор человека
	phone -- номер телефона человека
) VALUES (
	2, 
	'111'
);

INSERT INTO mike.phone_numbers (
	id, -- идентификатор человека
	phone -- номер телефона человека
) VALUES (
	3, 
	'123'
);

SELECT * FROM mike.phone_numbers;

UPDATE mike.phone_numbers
SET
	phone = '72345678911'
WHERE id = 1;

UPDATE mike.phone_numbers
SET
	phone = '72345671234'
WHERE id = 2;

UPDATE mike.phone_numbers
SET
	phone = '72341328911'
WHERE id = 3;