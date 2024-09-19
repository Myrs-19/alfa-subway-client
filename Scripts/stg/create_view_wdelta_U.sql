-- вьюха для insert`а в wdelta, случай, когда dwsact = U
-- то есть для тех у кого произошли изменения - из всех записей для какого-то uk: 
--		удалилась запись для uk, 
--		запись добавилась для uk, 
--		запись изменилась для uk
CREATE OR REPLACE VIEW mike.v_wdelta_U AS 
SELECT 
	wdelta_possible_changed.uk uk,
	
	wdelta_possible_changed.id id,
	wdelta_possible_changed.name name,
	wdelta_possible_changed.birthday birthday,
	wdelta_possible_changed.age age,
	wdelta_possible_changed.phone phone,
	wdelta_possible_changed.inn inn,
	wdelta_possible_changed.pasport pasport,
	wdelta_possible_changed.weight weight,
	wdelta_possible_changed.height height
FROM (
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
	JOIN STG.CLIENT_WDELTA wdelta
		ON context.uk = wdelta.uk
	WHERE 
		-- проверка на то, что все записи в контексте не удалены
		-- то есть записи в контексте могли меняться, 
		-- то есть либо вставится новая, либо измениться старая
		EXISTS(
			SELECT 1
			FROM STG.CLIENT_CONTEXT context_inner
			WHERE context_inner.uk = context.uk AND dwsarchive IS NULL
		) 
		OR 
		-- проверка на то, что есть хотя бы одна удаленая запись для uk
		EXISTS(
			SELECT 1
			FROM (
				SELECT uk, dwsarchive
				FROM STG.CLIENT_CONTEXT context_inner
				GROUP BY uk, dwsarchive
			) context_inner
			WHERE context_inner.uk = context.uk
			GROUP BY dwsarchive
			HAVING count(dwsarchive) = 2
		)
	GROUP BY context.uk
) wdelta_possible_changed
JOIN STG.CLIENT_WDELTA wdelta
	ON wdelta.uk = wdelta_possible_changed.uk
WHERE
	-- сравнивается каждый атрибут измерения, если оба значения null, то сравнивать их
	-- нет смысла, поэтому хоть какое-то из значение не должно принимать null
	(wdelta.id <> wdelta_possible_changed.id AND (wdelta.id IS NOT NULL OR wdelta_possible_changed.id IS NOT NULL)) -- идентификатор человека
	OR (wdelta.name <> wdelta_possible_changed.name AND (wdelta.name IS NOT NULL OR wdelta_possible_changed.name IS NOT NULL)) -- имя человека
	OR (wdelta.birthday <> wdelta_possible_changed.birthday AND (wdelta.birthday IS NOT NULL OR wdelta_possible_changed.birthday IS NOT NULL)) -- день рождения человека
	OR (wdelta.age <> wdelta_possible_changed.age AND (wdelta.age IS NOT NULL OR wdelta_possible_changed.age IS NOT NULL))-- возраст человека
	OR (wdelta.phone <> wdelta_possible_changed.phone AND (wdelta.phone IS NOT NULL OR wdelta_possible_changed.phone IS NOT NULL))-- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
	OR (wdelta.inn <> wdelta_possible_changed.inn AND (wdelta.inn IS NOT NULL OR wdelta_possible_changed.inn IS NOT NULL))-- инн человека (формат - ххх-ххх-ххх хх)
	OR (wdelta.pasport <> wdelta_possible_changed.pasport AND (wdelta.pasport IS NOT NULL OR wdelta_possible_changed.pasport IS NOT NULL))-- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
	OR (wdelta.weight <> wdelta_possible_changed.weight AND (wdelta.weight IS NOT NULL OR wdelta_possible_changed.weight IS NOT NULL))-- вес человека 
	OR (wdelta.height <> wdelta_possible_changed.height AND (wdelta.height IS NOT NULL OR wdelta_possible_changed.height IS NOT NULL))-- рост человека
;

SELECT * FROM mike.v_wdelta_U;

SELECT * FROM STG.CLIENT_CONTEXT;






