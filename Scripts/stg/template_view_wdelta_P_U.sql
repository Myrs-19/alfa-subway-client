-- ШАБЛОН НА СОЗДАНИЕ ВЬЮХИ ДЛЯ УНИФИКАЦИИ АТРИБУТА C ТИПОМ ПРИОРИТЕТ

-- вьюха для insert`а в wdelta, случай, когда dwsact = U
-- то есть для новых uk

-- места где указано attr подставляется переменная, 
-- вместо текущего примера
CREATE OR REPLACE VIEW mike.v_wdelta_P_U_attr AS 

SELECT
	wdelta_possible_changed.uk uk,
	
	wdelta_possible_changed.PHONE PHONE
FROM (
	SELECT
		-- ключи dwh
		wcontext.uk uk,
			
		-- унифицируемый атрибут
		-- в примере это атрибут - PHONE
		-- attr
		wcontext.PHONE PHONE
	FROM (
		SELECT 
			-- ключи dwh
			wcontext.uk uk,
			
			-- унифицируемый атрибут
			-- в примере это атрибут - PHONE
			-- attr
			wcontext.PHONE PHONE,
			ROW_NUMBER() OVER(PARTITION BY wcontext.uk ORDER BY p_a.priority) rn
		FROM (
			SELECT
				-- ключи dwh
				context.uk uk,
				
				-- метаполя
				context.DWSEID DWSEID,
				
				-- унифицируемый атрибут
				-- в примере это атрибут - PHONE
				-- attr
				context.PHONE PHONE
			FROM (
					SELECT *
					FROM STG.CLIENT_CONTEXT context
					WHERE dwsarchive IS NULL
				) context
			JOIN STG.CLIENT_WDELTA wdelta
				ON context.uk = wdelta.uk
			WHERE -- проверка на то, что все записи в контексте не удалены
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
				FROM STG.CLIENT_CONTEXT context_inner1
				GROUP BY uk, dwsarchive
			) context_inner
			WHERE context_inner.uk = context.uk
			GROUP BY uk
			HAVING count(*) = 2
		)
		-- attr
		AND context.PHONE IS NOT NULL
		) wcontext
		JOIN mike.priority_attrs p_a 
			ON p_a.DWSEID = wcontext.DWSEID
				-- attr
				AND p_a.attr = 'PHONE'
	) wcontext
	WHERE rn = 1
) wdelta_possible_changed
JOIN STG.CLIENT_WDELTA wdelta
	ON wdelta.uk = wdelta_possible_changed.uk
WHERE (wdelta.PHONE <> wdelta_possible_changed.PHONE AND (wdelta.PHONE IS NOT NULL OR wdelta_possible_changed.PHONE IS NOT NULL));


-- SELECT * FROM mike.v_wdelta_P_I_attr;

SELECT UK, PHONE
FROM MIKE.V_WDELTA_P_I_PHONE;

SELECT * FROM STG.CLIENT_CONTEXT;

SELECT * FROM mike.priority_attrs;


SELECT * FROM STG.CLIENT_WDELTA;
