-- вьюха для insert`а в wdelta, случай, когда dwsact = D
-- то есть когда все записи в контексте имеют значения dwsarchive = 'D'
CREATE OR REPLACE VIEW mike.v_wdelta_D AS 
SELECT
	-- ключи dwh
	context.uk uk
FROM STG.CLIENT_CONTEXT context
LEFT JOIN STG.CLIENT_WDELTA wdelta
	ON context.uk = wdelta.uk
WHERE 
	wdelta.uk IS NOT NULL
	-- проверяем что все записи имеют значени 'D' в поле dwsarchive
	AND EXISTS(
		SELECT max(dwsarchive)
		FROM (
			SELECT uk, dwsarchive
			FROM STG.CLIENT_CONTEXT context_inner1
			GROUP BY uk, dwsarchive
		) context_inner
		WHERE context_inner.uk = context.uk
		GROUP BY uk
		HAVING count(*) = 1 AND max(dwsarchive) = 'D'
	)
GROUP BY context.uk;

SELECT * FROM mike.v_wdelta_D;

SELECT * FROM STG.CLIENT_CONTEXT;