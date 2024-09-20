-- создание вьюхи для маппинга источника 002 в cdelta 
-- выполнять под mike
CREATE OR REPLACE VIEW mike.V_MAP_002 AS
SELECT 
	-- ключи dwh
	delta_pi.nk, -- целочисленный ключ dwh
	
	-- метаполя
	002 dwseid, -- идентификатор исходной таблицы-источника записи
	delta_pi.dwsjob, -- идентификатор загрузки, в рамках которой произошло изменение записи
	delta_pi.dwsact, -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
	delta_pi.dwsuniact, -- ('I', 'U', 'N' - логический ключ не изменился)

	-- поля атрибутов измерения
	delta_pi.id, -- идентификатор человека
	delta_pi.name, -- имя человека
	delta_pi.birthday, -- день рождения человека
	delta_pi.age, -- возраст человека
	NULL phone,
	delta_pi.inn,
	NULL pasport,
	NULL weight, -- вес человека 
	NULL height -- рост человека
FROM DWS002_3NF.PI001_DELTA delta_pi
UNION ALL
SELECT 
	-- ключи dwh
	delta_pn.nk, -- целочисленный ключ dwh
	
	-- метаполя
	002 dwseid, -- идентификатор исходной таблицы-источника записи
	delta_pn.dwsjob, -- идентификатор загрузки, в рамках которой произошло изменение записи
	delta_pn.dwsact, -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
	delta_pn.dwsuniact, -- ('I', 'U', 'N' - логический ключ не изменился)
	
	-- поля атрибутов измерения
	delta_pn.id, -- идентификатор человека
	NULL name, -- имя человека
	NULL birthday, -- день рождения человека
	NULL age, -- возраст человека
	CASE 
		WHEN REGEXP_INSTR(delta_pn.phone, '^7\d{10}$') <> 0 
			THEN delta_pn.phone
		ELSE NULL
	END phone,
	delta_pn.inn,
	NULL pasport,
	NULL weight, -- вес человека 
	NULL height -- рост человека
FROM DWS002_3NF.PN001_DELTA delta_pn
UNION ALL
SELECT 
	-- ключи dwh
	delta_docs.nk, -- целочисленный ключ dwh
	
	-- метаполя
	002 dwseid, -- идентификатор исходной таблицы-источника записи
	delta_docs.dwsjob, -- идентификатор загрузки, в рамках которой произошло изменение записи
	delta_docs.dwsact, -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
	delta_docs.dwsuniact, -- ('I', 'U', 'N' - логический ключ не изменился)
	
	-- поля атрибутов измерения
	delta_docs.id, -- идентификатор человека
	NULL name, -- имя человека
	NULL birthday, -- день рождения человека
	NULL age, -- возраст человека
	NULL phone,
	CASE 
		WHEN LENGTH(delta_docs.inn || '') = 11
			THEN delta_docs.inn
		ELSE NULL
	END inn,
	delta_docs.pasport,
	NULL weight, -- вес человека 
	NULL height -- рост человека
FROM DWS002_3NF.DOCS001_DELTA delta_docs
UNION ALL
SELECT 
	-- ключи dwh
	delta_hp.nk, -- целочисленный ключ dwh
	
	-- метаполя
	002 dwseid, -- идентификатор исходной таблицы-источника записи
	delta_hp.dwsjob, -- идентификатор загрузки, в рамках которой произошло изменение записи
	delta_hp.dwsact, -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
	delta_hp.dwsuniact, -- ('I', 'U', 'N' - логический ключ не изменился)
	
	-- поля атрибутов измерения
	delta_hp.id, -- идентификатор человека
	NULL name, -- имя человека
	NULL birthday, -- день рождения человека
	NULL age, -- возраст человека
	NULL phone,
	NULL inn,
	NULL pasport,
	delta_hp.weight, -- вес человека 
	delta_hp.height -- рост человека
FROM DWS002_3NF.HP001_DELTA delta_hp;

SELECT * FROM mike.V_MAP_002;

