-- вьюха для insert`а в wdelta, случай, когда dwsact = D
-- то есть когда все записи в контексте имеют значения dwsarchive = 'D'

-- todo

CREATE OR REPLACE VIEW mike.v_wdelta_D AS 
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
FROM STG.CLIENT_CONTEXT context
LEFT JOIN STG.CLIENT_WDELTA wdelta
	ON context.uk = wdelta.uk
WHERE 
	wdelta.uk IS NOT NULL
	-- проверяем что все записи имеют значени 'D' в поле dwsarchive
	AND 'D' = (
		SELECT dwsarchive
		FROM STG.CLIENT_CONTEXT context_inner
		WHERE context_inner.uk = context.uk
		GROUP BY dwsarchive
		HAVING COUNT(DISTINCT dwsarchive) = 1
	)

GROUP BY context.uk;

SELECT * FROM mike.v_wdelta_D;

SELECT * FROM STG.CLIENT_CONTEXT;


WITH t(c) AS (
	SELECT 1 FROM dual
	UNION ALL 
	SELECT 1 FROM dual
	UNION ALL 
	SELECT null FROM dual
)
SELECT count(DISTINCT c)
FROM t
GROUP BY c