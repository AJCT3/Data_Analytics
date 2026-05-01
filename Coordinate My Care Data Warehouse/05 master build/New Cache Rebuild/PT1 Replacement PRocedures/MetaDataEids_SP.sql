USE [ETL_Local_PROD]
GO
/****** Object:  StoredProcedure [dbo].[AT__Activity_Data]    Script Date: 14/05/2020 14:13:39 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




ALTER PROCEDURE [dbo].[AT__MetadataDeptEids_SP] 
-- Amended for PD Upgrade
AS
BEGIN


--ALTER view [Protocol].[Cache-MetadataDeptEids] as
 --proper denormalisation of old system data MS 12.3.17
 


IF OBJECT_ID('tempdb..#MetadataDeptEids') IS NOT NULL 
dROP TABLE #MetadataDeptEids
------------------------------------------------------------------------------------------------------------------------------------------------------------
select
distinct
cmc_id, --OldSystemCarePlans
OriginalApproverWorkbaseID,
LatestApproverWorkbaseID,
OriginalWorkbaseId,
LatestWorkbaseId,
--------------------------------------------------------
null as OriginalAssessorWorkbaseEid,
null as LatestAssessorWorkbaseEid,
null as OriginalApproverWorkbaseEid,
null as LatestApproverWorkbaseEid,
-------------------------------------------------------
cast(null as nvarchar(max)) as OriginalAssessorWorkbase,
cast(null as nvarchar(max)) as LatestAssessorWorkbase,
cast(null as nvarchar(max)) as OriginalApproverWorkbase,
cast(null as nvarchar(max)) as LatestApproverWorkbase,
-------------------------------------------------------
ORIGINAL_WORKBASE,
LATEST_WORKBASE,
ORIGINAL_APPROVER_WORKBASE,
LATEST_APPROVER_WORKBASE,
Original_Prof_Group,
Latest_Prof_Group,
Original_Approver_Prof_Group,
Latest_Approver_Prof_Group,
Original_Workbase_ODS,
Latest_Workbase_ODS,
Original_Approver_Workbase_ODS,
Latest_Approver_Workbase_ODS,
null as pdo_deptenterpriseid,
null as pdl_deptenterpriseid,
null as pdo2_deptenterpriseid,
null as pdl2_deptenterpriseid,
cast(null as nvarchar(max)) as OriginalAssessorProfGroup,

cast(null as nvarchar(max)) as LatestAssessorProfGroup,

cast(null as nvarchar(max)) as OriginalApproverProfGroup,

cast(null as nvarchar(max)) as LatestApproverProfGroup,

cast(null as nvarchar(255)) as OriginalAssessorWorkbaseODS,

cast(null as nvarchar(255)) as LatestAssessorWorkbaseODS,

cast(null as nvarchar(255)) as OriginalApproverWorkbaseODS,
cast(null as nvarchar(255)) as LatestApproverWorkbaseODS,

cast(null as nvarchar(max)) as OriginalAssessorWorkbaseEmail,
cast(null as nvarchar(max)) as LatestAssessorWorkbaseEmail,
cast(null as nvarchar(max)) as OriginalApproverWorkbaseEmail,
cast(null as nvarchar(max)) as LatestApproverWorkbaseEmail,
--ROW_NUMBER() over (partition by pd.cmc_id order by pdo.deptenterpriseid,pdl.deptenterpriseid,pdo2.deptenterpriseid,pdl2.deptenterpriseid) as rn
null as rn

into #MetadataDeptEids

from protocol.OldSystemCarePlans  
------------------------------------------------------------------------------------------------------------------------------------------------------------

IF OBJECT_ID('tempdb..#WorkbaseDQInfopdo') IS NOT NULL 
dROP TABLE #WorkbaseDQInfopdo
select
distinct
DeptLocalCMCId,
DeptEnterpriseID,
DeptName,
LocalCMCOrgTypeDescription,
DeptODSCode,
WorkbaseEmail 
into #WorkbaseDQInfopdo
from WorkbaseDQInfo a 
inner join #MetadataDeptEids b on b.OriginalApproverWorkbaseID = a.DeptLocalCMCId
 where b.OriginalApproverWorkbaseID is not null

IF OBJECT_ID('tempdb..#WorkbaseDQInfopdl') IS NOT NULL 
dROP TABLE #WorkbaseDQInfopdl
select
distinct
DeptLocalCMCId,
DeptEnterpriseID,
DeptName,
LocalCMCOrgTypeDescription,
DeptODSCode,
WorkbaseEmail 
into #WorkbaseDQInfopdl
from WorkbaseDQInfo a 
inner join #MetadataDeptEids b on LatestApproverWorkbaseID = a.DeptLocalCMCId
where b.LatestApproverWorkbaseID is not null
 

IF OBJECT_ID('tempdb..#WorkbaseDQInfopdo2') IS NOT NULL 
dROP TABLE #WorkbaseDQInfopdo2
select
distinct
DeptLocalCMCId,
DeptEnterpriseID,
DeptName,
LocalCMCOrgTypeDescription,
DeptODSCode,
WorkbaseEmail 
into #WorkbaseDQInfopdo2
from WorkbaseDQInfo a 
inner join #MetadataDeptEids b on OriginalWorkbaseId = a.DeptLocalCMCId
where b.OriginalWorkbaseId is not null
 

IF OBJECT_ID('tempdb..#WorkbaseDQInfopdl2') IS NOT NULL 
dROP TABLE #WorkbaseDQInfopdl2
select
distinct
DeptLocalCMCId,
DeptEnterpriseID,
DeptName,
LocalCMCOrgTypeDescription,
DeptODSCode,
WorkbaseEmail 
into #WorkbaseDQInfopdl2
from WorkbaseDQInfo a 
inner join #MetadataDeptEids b on LatestWorkbaseId = a.DeptLocalCMCId
where b.LatestWorkbaseId is not null
 --select OriginalApproverWorkbaseID  from #MetadataDeptEids


 Update pd
		set OriginalAssessorWorkbaseEid = pdo2.DeptEnterpriseID
		,	LatestAssessorWorkbaseEid = pdl2.DeptEnterpriseID
		,	OriginalApproverWorkbaseEid = coalesce(pdo.DeptEnterpriseID,null)
		,	LatestApproverWorkbaseEid = coalesce(pdl.DeptEnterpriseID,null)
		,
			OriginalAssessorWorkbase = isnull(cast(pdo2.DeptName as nvarchar(max)),pd.ORIGINAL_WORKBASE),
			LatestAssessorWorkbase = isnull(cast(pdl2.DeptName as nvarchar(max)),pd.LATEST_WORKBASE),
			OriginalApproverWorkbase = 	isnull(isnull(cast(pdo.DeptName as nvarchar(max)),pd.ORIGINAL_APPROVER_WORKBASE),'Not recorded'),
			LatestApproverWorkbase = isnull(isnull(cast(pdl.DeptName as nvarchar(max)),pd.LATEST_APPROVER_WORKBASE),'Not recorded')
			,
			OriginalAssessorProfGroup = isnull(
												pdo2.LocalCMCOrgTypeDescription, 
												case 
													when pd.Original_Prof_Group='Acute' then 'Acute Trust'
													when pd.Original_Prof_Group='CMC' then 'CMC Team'
													when pd.Original_Prof_Group='Community' then 'Community Trust'
													when pd.Original_Prof_Group='GP' then 'General Practice'
													else pd.ORIGINAL_PROF_GROUP 
												end
												),
			LatestAssessorProfGroup = isnull(
												pdl2.LocalCMCOrgTypeDescription,
												case 
												  when pd.Latest_Prof_Group='Acute' then 'Acute Trust'
												  when pd.Latest_Prof_Group='CMC' then 'CMC Team'
												  when pd.Latest_Prof_Group='Community' then 'Community Trust'
												  when pd.Latest_Prof_Group='GP' then 'General Practice'
												  else pd.Latest_PROF_GROUP 
												 end
												 ), 
			OriginalApproverProfGroup = isnull(
												pdo.LocalCMCOrgTypeDescription,
												case 
												  when pd.Original_Approver_Prof_Group='Acute' then 'Acute Trust'
												  when pd.Original_Approver_Prof_Group='CMC' then 'CMC Team'
												  when pd.Original_Approver_Prof_Group='Community' then 'Community Trust'
												  when pd.Original_Approver_Prof_Group='GP' then 'General Practice'
												  else pd.ORIGINAL_Approver_PROF_GROUP end
												),
			LatestApproverProfGroup = isnull(
											pdl.LocalCMCOrgTypeDescription,
											case 
											  when pd.Latest_Approver_Prof_Group='Acute' then 'Acute Trust'
											  when pd.Latest_Approver_Prof_Group='CMC' then 'CMC Team'
											  when pd.Latest_Approver_Prof_Group='Community' then 'Community Trust'
											  when pd.Latest_Approver_Prof_Group='GP' then 'General Practice'
											  else pd.Latest_Approver_PROF_GROUP end
											),
			OriginalAssessorWorkbaseODS = isnull(pdo2.DeptODSCode,pd.Original_Workbase_ODS),
			LatestAssessorWorkbaseODS = isnull(pdl2.DeptODSCode,pd.Latest_Workbase_ODS),
			OriginalApproverWorkbaseODS = isnull(isnull(pdo.DeptODSCode,pd.Original_Approver_Workbase_ODS),'Not recorded'),
			LatestApproverWorkbaseODS = isnull(isnull(pdl.DeptODSCode,pd.Latest_Approver_Workbase_ODS),'Not recorded'),
			OriginalAssessorWorkbaseEmail = pdo2.WorkbaseEmail,
			LatestAssessorWorkbaseEmail = pdl2.WorkbaseEmail,
			OriginalApproverWorkbaseEmail = pdo.WorkbaseEmail,
			LatestApproverWorkbaseEmail = pdl.WorkbaseEmail,
			pdo_deptenterpriseid = pdo.deptenterpriseid,
			pdl_deptenterpriseid = pdl.deptenterpriseid,
			pdo2_deptenterpriseid = pdo2.deptenterpriseid,
			pdl2_deptenterpriseid = pdl2.deptenterpriseid
		
 
  

  
from #MetadataDeptEids pd
left join #WorkbaseDQInfopdo pdo on pd.OriginalApproverWorkbaseID = pdo.DeptLocalCMCId --and pd.OriginalApproverWorkbaseID is not null
left join #WorkbaseDQInfopdl pdl on pd.LatestApproverWorkbaseID = pdl.DeptLocalCMCId --and pd.LatestApproverWorkbaseID is not null
left join #WorkbaseDQInfopdo2 pdo2 on pd.OriginalWorkbaseId = pdo2.DeptLocalCMCId --and pd.OriginalWorkbaseId is not null
left join #WorkbaseDQInfopdl2 pdl2 on pd.LatestWorkbaseId = pdl2.DeptLocalCMCId --and pd.LatestWorkbaseId is not null


update pd
		set pd.rn = b.rn

from #MetadataDeptEids pd
left join
			(
			Select
			cmc_id,
			ROW_NUMBER() over (partition by cmc_id order by pdo_deptenterpriseid,pdl_deptenterpriseid,pdo2_deptenterpriseid,pdl2_deptenterpriseid)as rn
			from #MetadataDeptEids
			)b on b.cmc_id = pd.CMC_ID




			--select * from #MetadataDeptEids order by cmc_id,rn



IF OBJECT_ID('[ETL_Local_PROD].[Protocol].[MetadatadeptEids]') IS NOT NULL 
dROP TABLE [ETL_Local_PROD].[Protocol].[MetadatadeptEids]
select 
[cmc_id]
      ,[OriginalAssessorWorkbaseEid]
      ,[LatestAssessorWorkbaseEid]
      ,[OriginalApproverWorkbaseEid]
      ,[LatestApproverWorkbaseEid]
      ,[OriginalAssessorWorkbase]
      ,[LatestAssessorWorkbase]
      ,[OriginalApproverWorkbase]
      ,[LatestApproverWorkbase]
      ,[OriginalAssessorProfGroup]
      ,[LatestAssessorProfGroup]
      ,[OriginalApproverProfGroup]
      ,[LatestApproverProfGroup]
      ,[OriginalAssessorWorkbaseODS]
      ,[LatestAssessorWorkbaseODS]
      ,[OriginalApproverWorkbaseODS]
      ,[LatestApproverWorkbaseODS]
      ,[OriginalAssessorWorkbaseEmail]
      ,[LatestAssessorWorkbaseEmail]
      ,[OriginalApproverWorkbaseEmail]
      ,[LatestApproverWorkbaseEmail]
      ,[rn]
into [ETL_Local_PROD].[Protocol].[MetadatadeptEids] 
from #MetadataDeptEids

END