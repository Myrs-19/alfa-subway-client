SELECT 
	-- ключи dwh
	delta.nk,	
	
	-- метаполя
	delta.dwsjob,
	delta.dwsact, -- (‘I’, ‘U’, ‘D’ – добавление, изменение, удаление соответственно)
	delta.dwsuniact, -- ('I', 'U', 'N' - логический ключ не изменился)
		
	-- поля таблицы-источника
	delta.id, -- идентификатор человека
	delta.name, -- имя человека
	delta.birthday, -- день рождения человека
	delta.age, -- возраст человека
	(
		'7' ||
		REGEXP_SUBSTR(delta.phone, '.+\((d{3})\)', 1, 1, NULL, 1) || -- выбираю цифры в части (ххх)
		REGEXP_SUBSTR(delta.phone, '') || -- выбираю цифры в части ххх
		REGEXP_SUBSTR(delta.phone, '') ||  -- выбираю цифры в 1ой части -хх
		REGEXP_SUBSTR(delta.phone, '') -- выбираю цифры во 2ой части -хх
	) phone, -- номер телефона человека (формат - +7(ххх)ххх-хх-хх - строка)
	delta.inn, -- инн человека (формат - ххх-ххх-ххх хх)
	delta.pasport, -- паспорт человека (сочетание номера и кода паспорта без пробелов и др знаков - только цифры)
	delta.weight, -- вес человека 
	delta.height-- рост человека
FROM DWS001_CLNT.CLIENT001_DELTA delta;








