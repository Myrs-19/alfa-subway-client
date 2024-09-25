-- ШАБЛОН НА СОЗДАНИЕ ВЬЮХИ ДЛЯ УНИФИКАЦИИ АТРИБУТА C ТИПОМ ПРИОРИТЕТ

-- вьюха для insert`а в wdelta, случай, когда dwsact = I
-- то есть для новых uk

-- места где указано attr подставляется переменная, 
-- вместо текущего примера
CREATE OR REPLACE VIEW mike.v_wdelta_P_I_attr AS 

SELECT
	-- ключи dwh
	wcontext.uk uk,
		
	-- унифицируемый атрибут
	-- в примере это атрибут - id
	-- attr
	wcontext.id id
FROM (
	SELECT 
		-- ключи dwh
		wcontext.uk uk,
		
		-- унифицируемый атрибут
		-- в примере это атрибут - id
		-- attr
		wcontext.id id,
		ROW_NUMBER() OVER(PARTITION BY wcontext.uk ORDER BY p_a.priority) rn
	FROM (
		SELECT
			-- ключи dwh
			context.uk uk,
			
			-- метаполя
			context.DWSEID DWSEID,
			
			-- унифицируемый атрибут
			-- в примере это атрибут - id
			-- attr
			context.id id
		FROM (
				SELECT *
				FROM STG.CLIENT_CONTEXT context
				WHERE dwsarchive IS NULL
			) context
		LEFT JOIN STG.CLIENT_WDELTA wdelta
			ON context.uk = wdelta.uk
		WHERE wdelta.uk IS NULL 
			-- attr
			AND context.id IS NOT NULL
	) wcontext
	JOIN mike.priority_attrs p_a 
		ON p_a.DWSEID = wcontext.DWSEID
			-- attr
			AND p_a.attr = 'PHONE'
) wcontext
WHERE rn = 1;

-- SELECT * FROM mike.v_wdelta_P_I_attr;

SELECT UK, PHONE
FROM MIKE.V_WDELTA_P_I_PHONE;

SELECT * FROM STG.CLIENT_CONTEXT;

SELECT * FROM mike.priority_attrs;

