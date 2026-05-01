USE [ETL_Local_PROD]
GO





alter PROCEDURE [dbo].[AT_MASTERBUILD_Service_Search_Update] 

AS
BEGIN





 	 if OBJECT_ID ('Tempdb..#MaxID') is not null
		drop table #MaxID
		select 
		RowItemID 
		into #MaxID 
		from [ETL_Local_PROD].[dbo].[ServiceSearch] where OverallOrder = 
																			(
																			select max(OverAllOrder) from [ETL_Local_PROD].[dbo].[ServiceSearch]
																			)







WAITFOR DELAY '00:00:00' -- WAIT EIGHT after incremental builds



	
		insert into [ETL_Local_PROD].[dbo].[ServiceSearch]

		select 
		CAST(SUBSTRING(a.ItemId,9,Len(a.ItemId)) as numeric) as RowItemID,
		b.OverAllOrder,
		a.ItemId,
		ActionTime,
		MRNs,
		Actor,
		REGISTRYRoles,
		---- add stripped actor for performance MS 21.5.16
		 rtrim(REPLACE(Actor,'EMISAvail','')) as ODS
 
		from etl_PROD.dbo.cmc_auditdata a with (nolock)
		 inner join [ETL_Local_PROD].[dbo].[AT_CMC_AuditData_RowID]b with (nolock) on b.Itemid = a.ItemId
		where ActionType = 'SearchPatient'
		and REGISTRYRoles like '%HSCC_Service_Search%'
		and CAST(SUBSTRING(a.ItemId,9,Len(a.ItemId)) as numeric) > (
																		select RowItemID from #MaxID
																		)
		and a.actionTime >= dateadd(week,-1,getdate())	
		


			IF OBJECT_ID('[ETL_Local_PROD].[dbo].[ServiceSearchGeneric]') IS NOT NULL 
			 dROP TABLE [ETL_Local_PROD].[dbo].[ServiceSearchGeneric]
			  select*,
			rtrim(REPLACE(Actor,'AvailGen','')) as ODS2

			 into  [ETL_Local_PROD].[dbo].[ServiceSearchGeneric]
			 from ServiceSearch 
			where Actor like 'AvailGen%'


			 if OBJECT_ID ('Tempdb..#MaxED') is not null
				drop table #MaxED
				select 
				max(EventDate) as MaxDate 
				into #MaxED
				from [ETL_Local_PROD].[dbo].[AT_AvailabilityLog]

				 if OBJECT_ID ('Tempdb..#Teams') is not null
				drop table #Teams
				select *,
				row_number() over (partition by activitydepartmentid order by team) rn 
				into #teams
				from Reporting.DisambiguatedActivityTeams

				if OBJECT_ID ('Tempdb..#ODSCODE') is not null
				drop table #ODSCODE
				select deptodscode,
				deptenterpriseid,
				ROW_NUMBER() over (PARTITION by deptodscode order by deptodscode) as rn 

				into #ODSCODE

				from AT_Dept


				
				

 	 if OBJECT_ID ('Tempdb..#MaxID2') is not null
		drop table #MaxID2
		select 
		max(eventdate)as LastDate
		into #MaxID2
		from [ETL_Local_PROD].[dbo].[AT_AvailabilityLog] 


				WAITFOR DELAY '00:00:20' 


			--IF OBJECT_ID('[ETL_Local_PROD].[dbo].[AT_AvailabilityLog]') IS NOT NULL 
			-- dROP TABLE [ETL_Local_PROD].[dbo].[AT_AvailabilityLog]

			insert into [ETL_Local_PROD].[dbo].[AT_AvailabilityLog]

			select 
			RowItemID,
			ItemId as id,
			cast(actiontime as datetime) as EventDateTime,
			cast(actiontime as date) as EventDate,
			case when MRNs='0' then 'N' else 'Y' end as match,
			'EMIS' as Service, 
			DeptEnterpriseID, 
			Team,
			ODS


			--into [ETL_Local_PROD].[dbo].[AT_AvailabilityLog]

			from ServiceSearch a
			-- logic here heeds to take close dates into account and also to follow Gareth's instructions re filtering
			left join #ODSCODE d on a.ODS = DeptODSCode and d.rn=1
			-- enforce uniqueness MS 29.5.16
 
			left join #teams at on at.ActivityDepartmentID = d.DeptEnterpriseID and at.rn=1
			left join #MaxID2 n on n.LastDate = convert(date,a.ActionTime)
			where not exists (select RowItemID from [ETL_Local_PROD].[dbo].[AT_AvailabilityLog] x where x.RowItemID = a.RowItemID)
			and convert(date,a.actiontime) >= n.LastDate

 /**
	  CREATE INDEX RID_EVT
ON [ETL_Local_PROD].[dbo].[AT_AvailabilityLog]  (RowItemID, EventDateTime);
	

	  CREATE INDEX EVT
ON [ETL_Local_PROD].[dbo].[AT_AvailabilityLog]  (EventDate);

	  CREATE INDEX DeptDate
ON [ETL_Local_PROD].[dbo].[AT_AvailabilityLog]  (DeptEnterpriseID,EventDate);


**/


END


