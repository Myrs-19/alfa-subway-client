-- TODO

-- вьюха для insert`а в wdelta, случай, когда dwsact = I
-- то есть для новых uk
CREATE OR REPLACE VIEW mike.v_wdelta_I AS 
SELECT
	-- ключи dwh
	COALESCE(view1.uk, view2.uk, view3.uk ...) uk,
	
	-- поля атрибутов измерения
	id, -- идентификатор человека
	name, -- имя человека
	birthday, -- день рождения человека
	age, -- возраст человека
	phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
	inn, -- инн человека (формат - ххх-ххх-ххх хх)
	pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
	weight, -- вес человека 
	height -- рост человека
FROM view1, view2, view3, ..., viewN
WHERE view1.uk = view2.uk and view2.uk = view3.uk AND view3.uk = view4.uk AND ...;


SELECT * FROM mike.v_wdelta_I;

SELECT * FROM STG.CLIENT_CONTEXT;
