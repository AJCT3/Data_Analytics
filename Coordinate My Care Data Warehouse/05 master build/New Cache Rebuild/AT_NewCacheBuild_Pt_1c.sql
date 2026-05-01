
 USE [ETL_Local_PROD]
GO
/****** Object:  StoredProcedure [dbo].[NewCacheUpdated]    Script Date: 02/03/2020 08:32:57 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/** =============================================
 What a f**king mess! And you can quote me on that.. A. Turner March 2020
---- =============================================**/

alter PROCEDURE [dbo].[AT_NewCacheUpdated_Pt_1c]

AS

BEGIN




	DROP TABLE [dbo].[OrgArea]
	select * into [dbo].[OrgArea] from [dbo].[Cache-OrgArea] with (nolock)
	-- cache GPInformation MS 18.10.16
	DROP TABLE [Reporting].[GPInformation]
	select * into [Reporting].[GPInformation] from [Reporting].[Cache-GPInformation] with (nolock)
	-- GW April 2019
	DROP TABLE [reporting].[GPInformation-No-GP-Check]
	select * into [reporting].[GPInformation-No-GP-Check] from [reporting].[Cache-GPInformation-No-GP-Check] with (nolock)

	--alter TABLE [ETL_Local_PROD].[dbo].[ServiceSearch]
 --  ADD CONSTRAINT PK_ItemID PRIMARY KEY CLUSTERED (ItemID);

 	 if OBJECT_ID ('Tempdb..#MaxID') is not null
		drop table #MaxID
		select 
		RowItemID 
		into #MaxID 
		from [ETL_Local_PROD].[dbo].[ServiceSearch] where OverallOrder = 
																			(
																			select max(OverAllOrder) from [ETL_Local_PROD].[dbo].[ServiceSearch]
																			)




--CREATE NONCLUSTERED INDEX [TimeSearchIndex] ON [dbo].[ServiceSearch]
--(
--	[ActionTime] ASC,
--	[OverallOrder] ASC
--)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
--GO 


WAITFOR DELAY '00:05:00' -- WAIT EIGHT after incremental builds



	
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
		and a.actionTime >= dateadd(week,-3,getdate())																	 

			IF OBJECT_ID('[ETL_Local_PROD].[dbo].[ServiceSearchGeneric]') IS NOT NULL 
			 dROP TABLE [ETL_Local_PROD].[dbo].[ServiceSearchGeneric]
			  select*,
			rtrim(REPLACE(Actor,'AvailGen','')) as ODS2

			 into  [ETL_Local_PROD].[dbo].[ServiceSearchGeneric]
			 from ServiceSearch 
			where Actor like 'AvailGen%'




		 




			if OBJECT_ID ('[ETL_Local_PROD].[reporting].[TeamAudit]') is not null
		drop table [ETL_Local_PROD].[reporting].[TeamAudit]
		select
		*
		into [ETL_Local_PROD].[reporting].[TeamAudit]
		 from [Reporting].[Cache-TeamAudit]


 

		
	 if OBJECT_ID ('[ETL_Local_PROD].[reporting].[DisambiguatedActivityTeams]') is not null
		drop table [ETL_Local_PROD].[reporting].[DisambiguatedActivityTeams]

		select distinct 
		* 
		into [ETL_Local_PROD].[reporting].[DisambiguatedActivityTeams]
		from [ETL_Local_PROD].[reporting].[Cache-DisambiguatedActivityTeams]



	PRINT 'End Section EIGHT'
	PRINT GETDATE()

	WAITFOR DELAY '00:05:00' -- WAIT EIGHT after incremental builds

	PRINT 'Start Section NINE'
	PRINT GETDATE()

	--//////////up to here

	 


	insert into AvailabilityLog

	select 
	ItemId as id,
cast(actiontime as datetime) as EventDateTime,
cast(actiontime as date) as EventDate,
case when MRNs='0' then 'N' else 'Y' end as match,
'EMIS' as Service, 
DeptEnterpriseID, 
Team,
a.RowItemID
from ServiceSearch a
-- logic here heeds to take close dates into account and also to follow Gareth's instructions re filtering
join (select deptodscode,deptenterpriseid,ROW_NUMBER() over (PARTITION by deptodscode order by deptodscode) as rn from PDDepartment  where deptodscode is not null) d on a.ODS = DeptODSCode and d.rn=1
-- enforce uniqueness MS 29.5.16
join (select *,
row_number() over (partition by activitydepartmentid order by team) rn 
from Reporting.DisambiguatedActivityTeams) at on at.ActivityDepartmentID = d.DeptEnterpriseID and at.rn=1

where a.RowItemID > (
																		select RowItemID from #MaxID
																			 
																	)

--select top 5* from ServiceSearch
--select top 5* from AvailabilityLog

	WAITFOR DELAY '00:05:00' 


	end