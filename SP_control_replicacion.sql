 /***********************************************************************************************************************/
/* nombre				: SP_control_replicacion																	*/
/* descripcion				: Controla agentes de distribucion y logreader e informa errores encontrados				*/
/* nombre de los parametros		: no hay				     															*/
/* tipo de los parametros		: no hay																				*/
/* descripcion del parametro		: no hay																			*/
/* autor				: Javier Kondratiuk						  														*/
/* fecha creacion			: 28/04/2016																				*/
/* observaciones			:																							*/
/***********************************************************************************************************************/

alter procedure SP_control_replicacion
   with encryption 
as

 set nocount on

 DECLARE @job_id UNIQUEIDENTIFIER, @PublisherDB sysname, @Publisher sysname, @Distributor sysname
 DECLARE @Subscriber sysname, @SubscriberDB sysname, @subsystem sysname, @Continuo bit
 DECLARE @pubname sysname, @strBody varchar(8000)

DECLARE @msg nvarchar(1024), @status int, @fecha varchar(10), @hora varchar(10)
DECLARE @publication sysname, @cmd nvarchar(4000)

declare @jobname varchar(100), @JobDescription varchar(100), @CategoryName varchar(20)
declare @id_tipo_alerta tinyint, @servidor varchar(200)
declare @prioridad_envio tinyint, @id_instancia int, @fecha_entrada_mensaje datetime

create table #status (Publisher sysname, PublisherDB sysname, publication sysname, status int, msg nvarchar(1024) , fecha varchar(10), hora varchar(10))

SELECT @id_instancia = id_instancia,
    @servidor = instancia,
	@fecha_entrada_mensaje = getdate()
FROM SP_Instancias
WHERE habilitado = 1


 DECLARE c_AgentsControl INSENSITIVE CURSOR FOR
 
 
 SELECT sj.job_id,
SUBSTRING(sjs.command,CHARINDEX('PublisherDB',sjs.command)+13,CHARINDEX(']',
sjs.command,CHARINDEX('PublisherDB',sjs.command)+13)-(CHARINDEX('PublisherDB',
sjs.command)+13))  as PublisherDB
,SUBSTRING(sjs.command,CHARINDEX('Publisher',sjs.command)+11,CHARINDEX(']',
sjs.command,CHARINDEX('Publisher',sjs.command)+11)-(CHARINDEX('Publisher',
sjs.command)+11))  as Publisher
,SUBSTRING(sjs.command,CHARINDEX('Distributor',sjs.command)+13,CHARINDEX(']',
sjs.command,CHARINDEX('Distributor',sjs.command)+13)-(CHARINDEX('Distributor',
sjs.command)+13))  as Distributor,
CASE  WHEN ct.name = 'REPL-LogReader' THEN 'N/A'
ELSE
SUBSTRING(sjs.command,CHARINDEX('Subscriber',sjs.command)+12,CHARINDEX(']',
sjs.command,CHARINDEX('Subscriber',sjs.command)+12)-(CHARINDEX('Subscriber',
sjs.command)+12))
END Subscriber,
CASE  WHEN ct.name = 'REPL-LogReader' THEN 'N/A'
ELSE
SUBSTRING(sjs.command,CHARINDEX('SubscriberDB',sjs.command)+14,CHARINDEX(']',
sjs.command,CHARINDEX('SubscriberDB',sjs.command)+14)-(CHARINDEX('SubscriberDB',
sjs.command)+14))
END SubscriberDB
,sjs.subsystem
, CASE WHEN CHARINDEX('-Continuous', sjs.command) > 0 THEN 1
ELSE 0 END AS Continuo	
    FROM msdb..syscategories ct
        INNER JOIN msdb..sysjobs sj
        	ON ct.category_id =sj.category_id
	INNER JOIN msdb..sysjobsteps sjs
		ON sj.job_id = sjs.job_id
    WHERE sj.enabled = 1
    AND ct.name IN ('REPL-Distribution', 'REPL-Merge', 'REPL-LogReader')
    AND subsystem IN ('Distribution', 'Merge', 'LogReader')



OPEN c_AgentsControl
            FETCH c_AgentsControl
              INTO @job_id, @PublisherDB, @Publisher, @Distributor, @Subscriber, @SubscriberDB, @subsystem, @Continuo
    
        
           WHILE @@fetch_status=0 
            BEGIN

			set @status = NULL

			truncate table #status

			--Agentes push o logreader
			if (@Distributor = @@SERVERNAME AND (@subsystem IN ('Distribution', 'Merge')))
			OR (@subsystem = 'LogReader')
			BEGIN

				--Agente trans
				IF @subsystem = 'Distribution'
				BEGIN
				
				select TOP 1 @publication= pb.publication,  @status = dh.runstatus, @msg = comments,
				@fecha = convert(varchar, DH.time, 103), 
				 @hora = convert(varchar, DH.time, 108)
				from distribution.dbo.MSsubscriptions sb WITH (READPAST)
				inner join distribution.dbo.MSpublications pb WITH (READPAST)
				on sb.publisher_id = pb.publisher_id 
				and sb.publisher_db = pb.publisher_db
				and sb.publication_id = pb.publication_id
				inner join distribution.dbo.MSdistribution_agents ag WITH (READPAST)
				on sb.agent_id = ag.id
				inner join  distribution.dbo.MSdistribution_history dh  WITH (READPAST)
				ON Ag.id = dh.agent_id
				and comments not like N'<stats state%'
				where job_id = CAST(@job_id AS BINARY(16))
				and (dh.runstatus != 6 or (dh.runstatus =6 and dh.error_id != 0))
				ORDER BY time DESC, timestamp DESC 


				END
				ELSE
				BEGIN
					--LogReader
					IF @subsystem = 'LogReader'
					BEGIN
						select TOP 1 
						@Publisher = @@SERVERNAME, @PublisherDB = pb.publisher_db, 
						@publication = pb.publication, 
						@status = lh.runstatus, @msg = lh.comments,
						@fecha = convert(varchar, lh.time, 103), 
						@hora = convert(varchar, lh.time, 108)
						from distribution.dbo.MSsubscriptions sb WITH (READPAST)
						inner join distribution.dbo.MSpublications pb WITH (READPAST)
						on sb.publication_id = pb.publication_id 
						inner join distribution.dbo.MSlogreader_agents ag WITH (READPAST)
						on sb.publisher_id = ag.publisher_id
						AND pb.publisher_db = ag.publisher_db
						inner join distribution.dbo.MSlogreader_history lh WITH (READPAST)
						on lh.agent_id = ag.id 
						and ag.job_id = CAST(@job_id AS BINARY(16))
						order by lh.time desc
					END
					ELSE -- Push Merge
					BEGIN						
						
						SELECT TOP (1) 
						@publication= msma.publication,
						@status = msms.runstatus,
						@msg = msmh.comments,
						@fecha = convert(varchar, msmh.time, 103), 
						@hora = convert(varchar, msmh.time, 108)
						FROM distribution.dbo.MSmerge_history msmh WITH (READPAST)
						LEFT JOIN distribution.dbo.MSmerge_sessions msms WITH (READPAST)
						ON msmh.session_id = msms.session_id
						INNER JOIN distribution.dbo.MSmerge_agents msma WITH (READPAST)
						ON msmh.agent_id = msma.id
						WHERE msma.job_id =  CAST(@job_id AS BINARY(16))
						and (msms.runstatus != 6 or (msms.runstatus =6 and msmh.error_id != 0))
						ORDER BY msmh.time DESC, msmh.timestamp DESC

						
					END
				END
				
			END
			ELSE -- Agentes pull
			BEGIN

				--Obtiene detalle de acuerdo al tipo de agente
				
				--Pull Merge
				if @subsystem = 'Merge'
				begin
				
				set @cmd = '
				    SELECT p.publisher, p.publisher_db, p.name ,
					s.last_sync_status, s.last_sync_summary,
					convert(varchar, s.last_sync_date, 103),
					convert(varchar, s.last_sync_date, 108) 
					from ['+@SubscriberDB+'].[dbo].[sysmergepublications] as p
					join ['+@SubscriberDB+'].[dbo].[sysmergesubscriptions] as s 
					on p.pubid = s.pubid
					and s.pubid <> s.subid
					and lower(s.subscriber_server) collate database_default = lower(@@servername) collate database_default 
					inner join ['+@SubscriberDB+'].[dbo].[MSmerge_replinfo] r
					on (s.subid = r.repid )
					where merge_jobid = '+CONVERT(NVARCHAR(100),CAST(@job_id AS BINARY(16)), 1) +''

					INSERT INTO #status
					EXEC (@cmd) 

					select @Publisher = publisher, @PublisherDB = PublisherDB, @publication =  publication,
					@status = status, @msg = msg,
					@fecha = fecha, 
					@hora = hora
					from #status

				end
				-- Pull Trans
				if @subsystem = 'Distribution'
				begin
					set @cmd = ' 
					select  s.publisher, s.publisher_db, s.publication,
					a.last_sync_status, a.last_sync_summary,
					convert(varchar, a.last_sync_time, 103), 
					convert(varchar, a.last_sync_time, 108) 
					from ['+@SubscriberDB+'].[dbo].[MSreplication_subscriptions] s with (NOLOCK)
					INNER JOIN ['+@SubscriberDB+'].[dbo].[MSsubscription_agents] a with (NOLOCK)
					on (UPPER(s.publisher) = UPPER(a.publisher) and   
                    s.publisher_db = a.publisher_db and   
                    ((s.publication = a.publication and   
                    s.independent_agent = 1 and  
                    a.publication <> N''ALL'') or  
                    (a.publication = N''ALL'' and s.independent_agent = 0)) and  
                    s.subscription_type = a.subscription_type)
					where agent_id = '+CONVERT(NVARCHAR(100),CAST(@job_id AS BINARY(16)), 1) +'
					'
					INSERT INTO #status
					EXEC (@cmd) 

					select @Publisher = publisher, @PublisherDB = PublisherDB, @publication =  publication,
					@status = status, @msg = msg,
					@fecha = fecha, 
					@hora = hora
					from #status


				end

				END

				--Si no se pudo obtener el status del agente obtiene estado y mensaje del job
				if @status is null
				select  
				@status = CASE(
			
								case 
								when sysja.run_requested_date is NULL then 5								-- Case when job has never been run but sqlagent is started
								when sysja.job_history_id is not NULL and sysjh.run_status is NULL then 5	-- Case when job has been run but history has been truncated
								else isnull(sysjh.run_status, 4)											-- Normal case...
							end
							)							 
			                        when 0 then 6   -- Fail mapping
			                        when 1 then 2   -- Success mapping
			                        when 2 then 5   -- Retry mapping
			                        when 3 then 2   -- Shutdown mapping
			                        when 4 then 3   -- Inprogress mapping
			                        when 5 then 0   -- Unknown is mapped to never run
			                    end 
							,
				@msg =	isnull(nullif(ltrim(sysjh.message), N'') , formatmessage(14243, sysj.name)), 
				@fecha = convert(varchar, sysjh.run_date, 103), 
				@hora = convert(varchar, sysjh.run_time, 108)
				from msdb.dbo.sysjobactivity sysja 
				join msdb.dbo.sysjobs sysj
					on sysja.job_id = sysj.job_id
				left join msdb.dbo.sysjobhistory sysjh
					on sysja.job_id = sysjh.job_id
						and sysja.job_history_id = sysjh.instance_id
				where sysja.job_id = @job_id
				and sysja.session_id = ( select MAX(session_id)
				from msdb..syssessions)
			
			
			
			-- Se realiza chequeo final y define si se informa error
			if @status NOT IN (1, 2, 3, 4) -- start, success Inprogress, idle
			    OR (@status = 2 AND @Continuo = 1) -- Job continuo detenido
			begin

				-- Si el error esta definido para ser informado se procesa
				IF EXISTS(SELECT valor
						FROM SP_Valores
						WHERE id_tipo_alerta = 4
						AND habilitado = 1
						AND @msg like '%' + valor + '%')
				BEGIN

				SELECT @id_tipo_alerta = id_tipo_alerta,
					@prioridad_envio = prioridad_envio
					FROM SP_Alertas
					WHERE habilitado = 1
					AND tipo_alerta = 'M'
					AND valor_tipo_alerta = 'Agentes de Replicacion'


					SET @strBody = 'DETALLES DE ERROR EN REPLICACION' + char(13) 
					SET @strBody = @strBody + ' ' + char(13) 
					SET @strBody = @strBody + 'DATE/TIME: '  + convert(varchar, getdate()) + char(13) 
					SET @strBody = @strBody + char(13) + char(13) + char(13) + ' ' + char(13) + char(13) + 'Publicador: ' + convert(varchar, @publisher) + char(13) + 'Base datos publicador: ' + convert(varchar, @PublisherDB) + char(13) + 'Publicacion: ' + convert(varchar, @publication) + char(13) + 'Suscriptor: ' + convert(varchar, @subscriber) + char(13) + 'Base de datos suscriptor: ' + convert(varchar, @SubscriberDB) + char(13) + 'Fecha-Hora: ' + convert(varchar(1000), @fecha) + ' - ' + + convert(varchar(1000), @hora) + char(13) + 'Error: ' + convert(varchar(1000), @msg)


				END
				ELSE -- Si el error no está definido para ser informado se envia como warning
				BEGIN

					SELECT @id_tipo_alerta = id_tipo_alerta,
					@prioridad_envio = prioridad_envio
					FROM SP_Alertas with (nolock)
					WHERE habilitado = 1
					AND tipo_alerta = 'R'
					AND valor_tipo_alerta = 'Errores Replicacion'

					SELECT  @jobname = msdb..sysjobs.name , 
					@JobDescription = msdb..sysjobs.description, 
					@CategoryName = msdb..syscategories.name
					FROM msdb..syscategories 
					INNER JOIN msdb..sysjobs 
					ON msdb..syscategories.category_id = msdb..sysjobs.category_id
					WHERE msdb..sysjobs.job_id = @job_id

					SET @strBody = 'MENSAJE ERROR EN AGENTES DE REPLICA DESDE ' + @@servername + char(13) + ' ' + char(13)
                    SET @strBody = @strBody + 'JobName: "' + @jobname + '"' + char(13)
                    SET @strBody = @strBody + '@JobDescription: ' + @JobDescription + char(13)
                    SET @strBody = @strBody + '@CategoryName: ' + @CategoryName + char(13)
                    SET @strBody = @strBody + '@LastMsg: ' + @msg + char(13)
					SET @strBody = @strBody + char(13) +' ' + char(13)

				END

				
				 INSERT INTO SP_BandejaMensajes (servidor, 
		        id_instancia, 
		        id_existencia_bases_datos, 
		        mensaje, 
		        fecha_entrada_mensaje, 
		        fecha_salida_mensaje, 
		        prioridad_mensaje, 
		        enviado, 
		        id_tipo_alerta, 
		        formato)
		    VALUES (@servidor,
		        @id_instancia,
		        NULL,
		        @strBody,
		        @fecha_entrada_mensaje,
		        NULL,
		        @prioridad_envio,
		        0,
		        @id_tipo_alerta,
		        'T') 
				/*
				select @servidor,
		        @id_instancia,
		        NULL,
		        @strBody,
		        @fecha_entrada_mensaje,
		        NULL,
		        @prioridad_envio,
		        0,
		        @id_tipo_alerta,
		        'T'
			*/
			end

			 --Si el job es continuo y no esta en ejecución lo ejecuta
		   if @Continuo = 1  and (@status = 2 OR @status = 6 or @status is null) 
		   begin
		   try
		  		--EXEC msdb.dbo.sp_start_job NULL, @job_id
				select @job_id, @Continuo, @status
			end try
			begin catch

			end catch



            FETCH c_AgentsControl
                INTO @job_id, @PublisherDB, @Publisher, @Distributor, @Subscriber, @SubscriberDB, @subsystem, @Continuo


        END

    CLOSE c_AgentsControl
    DEALLOCATE c_AgentsControl

drop table #status
