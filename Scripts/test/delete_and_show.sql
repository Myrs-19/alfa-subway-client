/*

-- удаляем все
delete from mike.orchestrator_alfa;
delete from mike.LOGS_ALFA;

-- dwi
delete FROM DWI001_STAR.client001_DSRC;
delete FROM DWI002_3NF.personal_information002_DSRC;
delete FROM DWI002_3NF.phone_numbers002_DSRC;
delete FROM DWI002_3NF.documents002_DSRC;
delete FROM DWI002_3NF.human_params002_DSRC;

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

*/


BEGIN
	mike.dwi.wrap_dwi();
	mike.dws.wrap_dws();
END;

SELECT * from mike.orchestrator_alfa;
SELECT * from mike.LOGS_ALFA;

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
SELECT * FROM DWS002_3NF.DOCS001_DOUBLE;
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