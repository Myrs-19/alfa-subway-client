INSERT INTO mike.documents(
	id, -- идентификатор человека
	inn, -- инн человека (набор цифр)
	pasport -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
) VALUES 
(
	1,
	123,
	123
);


INSERT INTO mike.documents(
	id, -- идентификатор человека
	inn, -- инн человека (набор цифр)
	pasport -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
) VALUES 
(
	2,
	132,
	111
);

INSERT INTO mike.documents(
	id, -- идентификатор человека
	inn, -- инн человека (набор цифр)
	pasport -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
) VALUES 
(
	3,
	1323,
	1113
);

SELECT * FROM mike.documents;

UPDATE mike.documents
SET
	inn = 12345678911
WHERE id = 1;

UPDATE mike.documents
SET
	inn = 12345612341
WHERE id = 2;

UPDATE mike.documents
SET
	inn = 12332178911
WHERE id = 3;