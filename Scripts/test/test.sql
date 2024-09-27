SELECT DISTINCT cdelta_inner2.uk, max(cdelta_inner2.nk)
		--	INTO l_target_uk, l_target_nk
			FROM (
				SELECT cdelta_inner2.*, uklink.uk
				FROM STG.CLIENT_CDELTA cdelta_inner2
				JOIN STG.CLIENT_UKLINK uklink ON uklink.nk = cdelta_inner2.nk
			) cdelta_inner2
			WHERE cdelta_inner2.dwsjob <> 4
				AND cdelta_inner2.dwseid <> 2
				AND cdelta_inner2.nk <> 849
				AND cdelta_inner2.inn = 12345612331
				AND cdelta_inner2.dwsact <> 'D'
			GROUP BY uk;
			
		
		
		SELECT dwsjob, dwseid, nk, inn, uk
		FROM (
		SELECT cdelta.*, uklink.uk
		FROM STG.CLIENT_CDELTA cdelta
		JOIN STG.CLIENT_UKLINK uklink
			ON uklink.nk = cdelta.nk
		) cdelta
		WHERE EXISTS(
			SELECT 1
			FROM STG.CLIENT_CDELTA cdelta_inner
			WHERE cdelta_inner.dwsjob < 6
				AND cdelta_inner.dwseid <> cdelta.dwseid
				AND cdelta_inner.nk <> cdelta.nk
				AND cdelta_inner.inn = cdelta.inn
				AND cdelta_inner.dwsact <> 'D'
		);