-- создание джоба для уровня stg
BEGIN
	--DBMS_SCHEDULER.drop_job(job_name => 'stg_job');
	
	DBMS_SCHEDULER.CREATE_JOB (
		job_name           =>  'stg_job',
		job_type           =>  'STORED_PROCEDURE',
		job_action         =>  'mike.stg.wrap_stg',
		repeat_interval    =>  'freq=minutely; interval=5; bysecond=0;', /* каждые 5 минут*/
		comments           =>  'My new job',
		enabled			   => TRUE
   );
  
  --dbms_output.put_line('h');
END;