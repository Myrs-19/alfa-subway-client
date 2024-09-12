/*
-- удаляем все
delete from mike.orchestrator_alfa;
delete from mike.LOGS_ALFA;

delete FROM DWI001_STAR.client001_DSRC;
delete FROM DWI002_3NF.personal_information002_DSRC;
delete FROM DWI002_3NF.phone_numbers002_DSRC;
delete FROM DWI002_3NF.documents002_DSRC;
delete FROM DWI002_3NF.human_params002_DSRC;

*/


BEGIN
	mike.dwi.wrap_dwi();
END;

SELECT * from mike.orchestrator_alfa;
SELECT * from mike.LOGS_ALFA;

SELECT * FROM DWI001_STAR.client001_DSRC;
SELECT * FROM DWI002_3NF.personal_information002_DSRC;
SELECT * FROM DWI002_3NF.phone_numbers002_DSRC;
SELECT * FROM DWI002_3NF.documents002_DSRC;
SELECT * FROM DWI002_3NF.human_params002_DSRC;