USE [ETL_Local_PROD]
GO
/****** Object:  StoredProcedure [dbo].[AT_MASTERBUILD_AuditPatient_AuditCareplan]    Script Date: 24/05/2020 13:16:43 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






alter PROCEDURE [dbo].[AT_Cache_AccessLog] 
-- Amended for PD Upgrade
AS
BEGIN


	if object_id('tempdb..#Temp1') is not null
	drop table #Temp1
	select dateadd(day,-1,max(convert(date,ActivityDate))) as LastTIme into #Temp1 from [ETL_Local_PROD].[dbo].[AT_AccessDataDetail_CarePlan]



--		CREATE NONCLUSTERED INDEX [DateCMCID] ON [ETL_Local_PROD].[dbo].[AT_AccessDataDetail_CarePlan]

--(
--	activitydate,CMC_Id ASC
--)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

--alter table  [ETL_Local_PROD].[dbo].[AT_AccessDataDetail_CarePlan] alter column CMC_ID nvarchar(75)



		insert into [ETL_Local_PROD].[dbo].[AT_AccessDataDetail_CarePlan]

		select
		ap.CPAuditID,
	ap.cmc_id,
	ActionTime as ActivityDate,
	ActionType as [Access Type],
	s.StaffEnterpriseId,
	ISNULL(s.StaffForename + ' ','') + ISNULL(s.StaffSurname,'') as [User Name],
	d1.DeptEnterpriseId as ActivityEnterpriseId,
	d1.DeptName as name,
	d1.LocalCMCOrgTypeDescription as TeamType,
	s.StaffActiveDescription as Active_Status,
	s.StaffProviderTypeDescription as UserType,
	io.CMCRoleDescription,
	ap.Role,
	ap.FromCarePlan, ap.ToCarePlan



	from [AuditPatient_CarePlan_New] ap
	join AT_Staff s on s.StaffRegistryId = ap.StaffRegistryId
	join AT_PD_Dept d1 on d1.DeptPDRegistryId = ap.DeptPDRegistryId
	join AT_IndToOrg io on io.StaffEnterpriseId = s.StaffEnterpriseId and io.DeptEnterpriseId = d1.DeptEnterpriseId

	where convert(date,ap.ActionTime)>= (select LastTIme from #Temp1)
	and not exists (select CPAuditID from [ETL_Local_PROD].[dbo].[AT_AccessDataDetail_CarePlan]x where x.CPAuditID = ap.CPAuditID)
	

	--select top 500 * from  [ETL_Local_PROD].[Reporting].[AccessLog]

	 --if OBJECT_ID ('[ETL_Local_PROD].[Reporting].[AccessLog]') is not null
	 --drop table [ETL_Local_PROD].[Reporting].[AccessLog]


	if object_id('tempdb..#Temp2') is not null
	drop table #Temp2
	select dateadd(day,-1,max(convert(date,Activitytime))) as LastTIme into #Temp2 from [ETL_Local_PROD].[Reporting].[AccessLog]


--		CREATE NONCLUSTERED INDEX [DateCMCID] ON  [ETL_Local_PROD].[Reporting].[AccessLog]

--(
--		cmc_id, 
--	ActivityTime
--	 ASC
--)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

--alter table  [ETL_Local_PROD].[dbo].[AT_AccessDataDetail_CarePlan] alter column CMC_ID nvarchar(75)





	insert into  [ETL_Local_PROD].[Reporting].[AccessLog]

	select
	*
	from
	(

	select
	'Yes' as OnNewSystem,
	[User Name],
	ActivityDate as ActivityTime,
	pa.cmc_id, 
	case [Access Type] when 'custom-consent_removed' then 'delete' else REPLACE([Access Type],'custom-','') end as [Access Type],
	case
	  when TeamType = 'Acute Trust' and CMCRoleDescription = 'isUrgentCare' then 'A&E'
	  when rtrim(TeamType) = 'CCG' then 'Community Trust'
	  else TeamType end as TeamType,
	name as ActivityTeam,
	(select splitdata from (select splitdata,ROW_NUMBER() over (order by offset) as rn from dbo.SplitString(isnull(tocareplan,fromcareplan),'|')) sel1 where rn=5) as VersionNumber,
	-- Add team enterprise id MS 25.3.16
	ActivityEnterpriseId
	from [ETL_Local_PROD].[dbo].[AT_AccessDataDetail_CarePlan] pa with (nolock)
	where convert(date,pa.ActivityDate)>= (select LastTIme from #Temp1)



	union all
	select
	'No' as OnNewSystem,
	[User Name],
	ActivityDate as ActivityTime,
	pa.cmc_id, 
	case [Access Type]

	  when 'Access Denied Record' then 'access denied (restricted)'
	  when 'approved' then 'approved by clinician'
	  when 'Associate PDS Record' then 'connect to PDS record'
	  when 'Auto Associate PDS Record' then 'auto connect to PDS record'
	  when 'Auto Dissociate PDS Record' then 'auto disconnect from PDS record'
	  when 'cancelled' then 'discard'
	  when 'completed' then 'completed post approval'
	  when 'created' then 'create care plan version'
	  when 'Delete Record' then 'delete'
	  when 'Dissociate PDS Record' then 'disconnect from PDS record'
	  when 'Hard Delete Record' then 'hard delete'
	  when 'Insert Record' then 'create demographics'
	  when 'Insert Episode' then 'create care plan version'
	  when 'mark-dup' then 'marked as duplicate'
	  when 'Update Episode' then 'save care plan version'
	  when 'Update Record' then 'update demographics'
	  when 'Print Episode' then 'print care plan version'
	  when 'Print Record' then 'print'
	  when 'published' then 'publish'
	  when 'rejected' then 'rejected by clinician'
	  when 'Restrict Record' then 'restrict'
	  when 'View Record' then 'view'
	  when 'View Episode' then 'view care plan version'
	  else [Access Type] end as [Access Type],
	case rtrim(TeamType)
	  when '111' then '111 Provider'
	  when 'Acute' then 'Acute Trust'
	  when 'Ambulance' then 'Ambulance Trust'
	  when 'CMC' then 'CMC Team'
	  when 'CCG' then 'Community Trust'
	  when 'Community' then 'Community Trust'
	  when 'GP' then 'General Practice'
	  when 'OOH' then 'Out Of Hour GP Provider'
	  else TeamType end as TeamType,
	d.DeptName as ActivityTeam,
	case when Episode=0 then '' else cast(Episode as varchar(10)) + ' ' end + '(old system)' as VersionNumber,
	d.DeptEnterpriseID
	from (select * from Protocol.AccessDataDetail2
	union all
	select * from protocol.extraaccessdetail
	where [access type] <> 'created' or CAST(activitydate as DATE) < CAST('2013-04-01' as date)) pa
	left join PDDepartment d on pa.ActivityDepartmentId = d.DeptLocalCMCId
	)d

	where convert(date,d.ActivityTime)>= (select LastTIme from #Temp1)

	and not exists
	(
	select 
	cmc_id, 
	ActivityTime, 
	[Access Type],
	[User Name] 
	from [ETL_Local_PROD].[Reporting].[AccessLog]x 
	where x.[Access Type] = d.[Access Type] 
	and x.cmc_id = d.cmc_id
	and x.[User Name] = d.[User Name]
	and x.ActivityTime = d.ActivityTime

	)


END

