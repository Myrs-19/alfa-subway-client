-- создание вьюхи для маппинга источника 001 в cdelta 
-- выполнять под mike
CREATE OR REPLACE VIEW mike.V_MAP_001 AS
SELECT 
	-- ключи dwh
	delta.nk,	
	
	-- метаполя
	001 dwseid, -- идентификатор исходной таблицы-источника записи
	delta.dwsjob, -- идентификатор загрузки, в рамках которой произошло изменение записи
	delta.dwsact, -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
	delta.dwsuniact, -- ('I', 'U', 'N' - логический ключ не изменился)
		
	-- поля таблицы-источника
	delta.id, -- идентификатор человека
	delta.name, -- имя человека
	delta.birthday, -- день рождения человека
	delta.age, -- возраст человека
	CASE 
		WHEN REGEXP_INSTR(delta.phone, '^\+7\(\d{3}\)\d{3}-\d{2}-\d{2}$') <> 0 
			THEN REGEXP_REPLACE(delta.phone, '[+()\-]', '')
		ELSE NULL
	END phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
	CASE 
		WHEN REGEXP_INSTR(delta.inn, '^\d{3}-\d{3}-\d{3} \d{2}$') <> 0 
			THEN REGEXP_REPLACE(delta.inn, '[ -]', '')
		ELSE NULL
	END inn, -- инн человека (формат - ххх-ххх-ххх хх)
	delta.pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
	delta.weight, -- вес человека 
	delta.height-- рост человека
FROM DWS001_CLNT.CLIENT001_DELTA delta;

SELECT * FROM mike.V_MAP_001;

