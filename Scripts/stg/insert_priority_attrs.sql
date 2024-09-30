-- вставляем записи с приоритетами для атрибутов

-- delete from mike.priority_attrs;

INSERT INTO mike.priority_attrs (
	attr, -- название атрибута
	dwseid, -- код системы источника
	priority  -- приоритет системы источникаы
)
SELECT 
	'PHONE', -- название атрибута
	1, -- код системы источника
	1 -- приоритет системы источникаы
FROM dual
UNION ALL
SELECT 
	'PHONE', -- название атрибута
	2, -- код системы источника
	2 -- приоритет системы источникаы
FROM dual;

SELECT * FROM mike.priority_attrs;