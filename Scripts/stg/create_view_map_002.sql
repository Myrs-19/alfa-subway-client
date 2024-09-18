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
	CASE 
		WHEN REGEXP_INSTR(delta_pn.phone, '^7\d{10}$') <> 0 
			THEN delta_pn.phone
		ELSE NULL
	END phone,
	CASE 
		WHEN LENGTH(delta_docs.inn || '') = 11
			THEN delta_pn.inn
		ELSE NULL
	END inn,
	delta_docs.pasport,
	delta_hp.weight, -- вес человека 
	delta_hp.height -- рост человека
FROM DWS002_3NF.PI001_DELTA delta_pi
JOIN DWS002_3NF.PN001_DELTA delta_pn 
	ON delta_pn.id = delta_pi.id AND delta_pn.dwsjob = delta_pi.dwsjob
JOIN DWS002_3NF.DOCS001_DELTA delta_docs
	ON delta_docs.id = delta_pn.id AND delta_docs.dwsjob = delta_pn.dwsjob
JOIN DWS002_3NF.HP001_DELTA delta_hp
	ON delta_hp.id = delta_docs.id AND delta_hp.dwsjob = delta_docs.dwsjob;

SELECT * FROM mike.V_MAP_002;