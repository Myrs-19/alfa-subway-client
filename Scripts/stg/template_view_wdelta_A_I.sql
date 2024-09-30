-- ШАБЛОН НА СОЗДАНИЕ ВЬЮХИ ДЛЯ УНИФИКАЦИИ АТРИБУТА C ТИПОМ АГРЕГАЦИИ

-- вьюха для insert`а в wdelta, случай, когда dwsact = I
-- то есть для новых uk
CREATE OR REPLACE VIEW mike.v_wdelta_A_I AS 
SELECT
	-- ключи dwh
	context.uk uk,
	
	-- поля атрибутов измерения
	max(context.id) id, -- идентификатор человека
	max(context.name) name, -- имя человека
	max(context.birthday) birthday, -- день рождения человека
	max(context.age) age, -- возраст человека
	max(context.phone) phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
	max(context.inn) inn, -- инн человека (формат - ххх-ххх-ххх хх)
	max(context.pasport) pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
	max(context.weight) weight, -- вес человека 
	max(context.height) height -- рост человека
FROM (
		SELECT *
		FROM STG.CLIENT_CONTEXT context
		WHERE dwsarchive IS NULL
	) context
LEFT JOIN STG.CLIENT_WDELTA wdelta
	ON context.uk = wdelta.uk
WHERE wdelta.uk IS NULL
GROUP BY context.uk;

SELECT * FROM mike.v_wdelta_A_I;

SELECT * FROM STG.CLIENT_CONTEXT;