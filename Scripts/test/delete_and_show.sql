/*

-- удаляем все
delete from mike.orchestrator_alfa
where STAGING_LVL = 'dws' or STAGING_LVL = 'stg';
delete from mike.LOGS_ALFA
where STAGING_DWH_LVL = 'dws' or STAGING_DWH_LVL = 'stg';

-- dwi


-- dws
-- star clnt
delete from DWS001_CLNT.CLIENT001_DOUBLE;
delete from DWS001_CLNT.CLIENT001_NKLINK;
delete from DWS001_CLNT.CLIENT001_DELTA;
delete FROM DWS001_CLNT.CLIENT001_MIRROR;

-- 3nf pi
delete from DWS002_3NF.PI001_DOUBLE;
delete from DWS002_3NF.PI001_NKLINK;
delete from DWS002_3NF.PI001_DELTA;
delete FROM DWS002_3NF.PI001_MIRROR;

-- 3nf pn
delete from DWS002_3NF.Pn001_DOUBLE;
delete from DWS002_3NF.Pn001_NKLINK;
delete from DWS002_3NF.Pn001_DELTA;
delete FROM DWS002_3NF.Pn001_MIRROR;

-- 3nf docs
delete from DWS002_3NF.docs001_DOUBLE;
delete from DWS002_3NF.docs001_NKLINK;
delete from DWS002_3NF.docs001_DELTA;
delete FROM DWS002_3NF.docs001_MIRROR;

-- 3nf hp
delete from DWS002_3NF.hp001_DOUBLE;
delete from DWS002_3NF.hp001_NKLINK;
delete from DWS002_3NF.hp001_DELTA;
delete FROM DWS002_3NF.hp001_MIRROR;

-- stg
delete FROM STG.CLIENT_CDELTA;
delete FROM STG.CLIENT_UKLINK;
delete FROM STG.CLIENT_CONTEXT;
delete FROM STG.CLIENT_WDELTA;
*/

-- выполнение всего staging dwh
BEGIN
	--mike.dwi.wrap_dwi();
	mike.dws.wrap_dws();
	mike.stg.wrap_stg();
END;

SELECT * from mike.orchestrator_alfa
ORDER BY JOB_NUMBER desc, STAGING_LVL;

SELECT * from mike.LOGS_ALFA
ORDER BY id_job DESC, STAGING_DWH_LVL DESC, PROGRAM_TITLE DESC, num ASC;

-- source tables
SELECT * FROM mike.personal_information;
SELECT * FROM mike.phone_numbers;
SELECT * FROM mike.documents;
SELECT * FROM mike.human_params;
SELECT * FROM mike.star_client;

-- dwi
SELECT * FROM DWI001_STAR.client001_DSRC;
SELECT * FROM DWI002_3NF.personal_information002_DSRC;
SELECT * FROM DWI002_3NF.phone_numbers002_DSRC;
SELECT * FROM DWI002_3NF.documents002_DSRC;
SELECT * FROM DWI002_3NF.human_params002_DSRC;

--dws

-- star client
-- double
SELECT * FROM DWS001_CLNT.CLIENT001_DOUBLE;
-- nklink
SELECT * FROM DWS001_CLNT.CLIENT001_NKLINK;
-- delta
SELECT * FROM DWS001_CLNT.CLIENT001_DELTA;
-- mirror
SELECT * FROM DWS001_CLNT.CLIENT001_MIRROR;

-- 3nf pi
-- double
SELECT * FROM DWS002_3NF.PI001_DOUBLE;
-- nklink
select * FROM DWS002_3NF.PI001_NKLINK;
-- delta
SELECT * FROM DWS002_3NF.PI001_DELTA;
-- mirror
SELECT * FROM DWS002_3NF.PI001_MIRROR;

-- 3nf pn
-- double
SELECT * FROM DWS002_3NF.PN001_DOUBLE;
-- nklink
select * FROM DWS002_3NF.PN001_NKLINK;
-- delta
SELECT * FROM DWS002_3NF.PN001_DELTA;
-- mirror
SELECT * FROM DWS002_3NF.PN001_MIRROR;

-- 3nf docs
-- double
SELECT * FROM DWS002_3NF.DOCS001_DOUBLE;
-- nklink
select * FROM DWS002_3NF.DOCS001_NKLINK;
-- delta
SELECT * FROM DWS002_3NF.DOCS001_DELTA;
-- mirror
SELECT * FROM DWS002_3NF.DOCS001_MIRROR;

-- 3nf hp
-- double
SELECT * FROM DWS002_3NF.HP001_DOUBLE;
-- nklink
select * FROM DWS002_3NF.HP001_NKLINK;
-- delta
SELECT * FROM DWS002_3NF.HP001_DELTA;
-- mirror
SELECT * FROM DWS002_3NF.HP001_MIRROR;


-- stg

-- cdelta
SELECT * FROM STG.CLIENT_CDELTA;

-- uklink
SELECT * FROM STG.CLIENT_UKLINK;

-- context
SELECT * FROM STG.CLIENT_CONTEXT;

-- wdelta
SELECT * FROM STG.CLIENT_WDELTA;

-- вьюхи для маппинга
SELECT * FROM mike.V_MAP_001;
SELECT * FROM mike.V_MAP_002;

-- промежуточная таблица для унификации записи
SELECT * FROM mike.staging_uni_record;