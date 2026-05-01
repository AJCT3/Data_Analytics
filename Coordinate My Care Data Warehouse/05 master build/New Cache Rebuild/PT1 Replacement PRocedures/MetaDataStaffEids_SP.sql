USE [ETL_Local_PROD]
GO
/****** Object:  StoredProcedure [dbo].[AT__Activity_Data]    Script Date: 14/05/2020 14:13:39 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




alter PROCEDURE [dbo].[AT__MetadataStaffEids_SP] 
-- Amended for PD Upgrade
AS
BEGIN


--ALTER view [Protocol].[Cache-MetadataStaffEids] as
-- Amended for PD Upgrade
 



IF OBJECT_ID('tempdb..#MetadataDeptEids') IS NOT NULL 
dROP TABLE #MetadataDeptEids
------------------------------------------------------------------------------------------------------------------------------------------------------------
select

distinct

pd.cmc_id,
--mso2.StaffEnterpriseID as OriginalAssessorEid,
Original_Approver_Staff_ID,
Latest_Approver_Staff_ID,
Original_Assessor_Staff_ID,
Latest_Assessor_Staff_ID,
ORIGINAL_ASSESSOR,
original_approver,
ORIGINAL_JOB_TITLE,
LATEST_ASSESSOR,
Latest_Approver,
ORIGINAL_APPROVER_JOB_TITLE,
LATEST_JOB_TITLE,
LATEST_APPROVER_JOB_TITLE,
null as OriginalAssessorEid,
cast(null as varchar(max)) as OriginalAssessor,
null as OriginalApproverEid,
cast(null as varchar(max)) as OriginalApprover,
----isnull(ISNULL(mso2.StaffTitleDescription+' ','') + ISNULL(mso2.StaffForename+' ','') + mso2.StaffSurname,pd.ORIGINAL_ASSESSOR) as OriginalAssessor,
--mso.StaffEnterpriseID as OriginalApproverEid,

--isnull(isnull(ISNULL(mso.StaffTitleDescription+' ','') + ISNULL(mso.StaffForename+' ','') + mso.StaffSurname,pd.original_approver),'Not recorded') as OriginalApprover,
--msl2.StaffEnterpriseID as LatestAssessorEid,
null as LatestAssessorEid,
cast(null as varchar(max)) as LatestAssessor,
--isnull(ISNULL(msl2.StaffTitleDescription+' ','') + ISNULL(msl2.StaffForename+' ','') + msl2.StaffSurname,pd.LATEST_ASSESSOR) as LatestAssessor,
--msl.StaffEnterpriseID as LatestApproverEid,
null as LatestApproverEid,
cast(null as varchar(max)) as LatestApprover,
--isnull(isnull(ISNULL(msl.StaffTitleDescription+' ','') + ISNULL(msl.StaffForename+' ','') + msl.StaffSurname,pd.Latest_Approver),'Not recorded') as LatestApprover,
--mso2.AssessorEmail as OriginalAssessorEmail,
--mso.AssessorEmail as OriginalApproverEmail,
--msl2.AssessorEmail as LatestAssessorEmail,
--msl.AssessorEmail as LatestApproverEmail,
cast(null as varchar(max)) as OriginalAssessorEmail,
cast(null as varchar(max)) as OriginalApproverEmail,
cast(null as varchar(max)) as LatestAssessorEmail,
cast(null as varchar(max)) as LatestApproverEmail,

--isnull(isnull(mso2.StaffProviderTypeDescription,pd.ORIGINAL_JOB_TITLE),'Not recorded') as OriginalAssessorJobTitle,
--isnull(isnull(mso.StaffProviderTypeDescription,pd.ORIGINAL_APPROVER_JOB_TITLE),'Not recorded') as OriginalApproverJobTitle,
--isnull(isnull(msl2.StaffProviderTypeDescription,pd.LATEST_JOB_TITLE),'Not recorded') as LatestAssessorJobTitle,
--isnull(isnull(msl.StaffProviderTypeDescription,pd.LATEST_APPROVER_JOB_TITLE),'Not recorded') as LatestApproverJobTitle,
cast(null as varchar(max)) as OriginalAssessorJobTitle,
cast(null as varchar(max)) as OriginalApproverJobTitle,
cast(null as varchar(max)) as LatestAssessorJobTitle,
cast(null as varchar(max)) as LatestApproverJobTitle,


-- Add ODS codes MS 11.3.17

--mso2.StaffODSCode as OriginalAssessorODS,
--mso.StaffODSCode as OriginalApproverODS,
--msl2.StaffODSCode as LatestAssessorODS,
--msl.StaffODSCode as LatestApproverODS,

cast(null as varchar(max)) as OriginalAssessorODS,
cast(null as varchar(max)) as OriginalApproverODS,
cast(null as varchar(max)) as LatestAssessorODS,
cast(null as varchar(max)) as LatestApproverODS,
null as mso_staffenterpriseid,
null as msl_staffenterpriseid,
null as mso2_staffenterpriseid,
null as msl2_staffenterpriseid,
null as rn
----ROW_NUMBER() over (partition by pd.cmc_id order by mso.staffenterpriseid,msl.staffenterpriseid,mso2.staffenterpriseid,msl2.staffenterpriseid) as rn

into #MetadataDeptEids

from protocol.oldsystemcareplans pd

------------------------------------------------------------------------------------------------------------------------------------------------------------




IF OBJECT_ID('tempdb..#AssessorDQInfoMSO') IS NOT NULL 
dROP TABLE #AssessorDQInfoMSO
select
distinct
StaffLocalCMCId,
deptenterpriseid,
StaffTitleDescription,
StaffForename,
StaffSurname,
StaffEnterpriseID,
AssessorEmail,
StaffProviderTypeDescription,
StaffODSCode 

into #AssessorDQInfoMSO
from AssessorDQInfo a 
inner join #MetadataDeptEids b on b.Original_Approver_Staff_ID = a.StaffLocalCMCId
 where b.Original_Approver_Staff_ID is not null


IF OBJECT_ID('tempdb..#AssessorDQInfoMSL') IS NOT NULL 
dROP TABLE #AssessorDQInfoMSL
select
distinct
StaffLocalCMCId,
deptenterpriseid,
StaffTitleDescription,
StaffForename,
StaffSurname,
StaffEnterpriseID,
AssessorEmail,
StaffProviderTypeDescription,
StaffODSCode 

into #AssessorDQInfoMSL
from AssessorDQInfo a 
inner join #MetadataDeptEids b on Latest_Approver_Staff_ID = a.StaffLocalCMCId
where b.Latest_Approver_Staff_ID is not null
 

IF OBJECT_ID('tempdb..#AssessorDQInfoMSO2') IS NOT NULL 
dROP TABLE #AssessorDQInfoMSO2
select
distinct
StaffLocalCMCId,
deptenterpriseid,
StaffTitleDescription,
StaffForename,
StaffSurname,
StaffEnterpriseID,
AssessorEmail,
StaffProviderTypeDescription,
StaffODSCode 

into #AssessorDQInfoMSO2
from AssessorDQInfo a 
inner join #MetadataDeptEids b on b.Original_Assessor_Staff_ID = a.StaffLocalCMCId
 where b.Original_Assessor_Staff_ID is not null


IF OBJECT_ID('tempdb..#AssessorDQInfoMSL2') IS NOT NULL 
dROP TABLE #AssessorDQInfoMSL2
select
distinct
StaffLocalCMCId,
deptenterpriseid,
StaffTitleDescription,
StaffForename,
StaffSurname,
StaffEnterpriseID,
AssessorEmail,
StaffProviderTypeDescription,
StaffODSCode 

into #AssessorDQInfoMSL2
from AssessorDQInfo a 
inner join #MetadataDeptEids b on Latest_Assessor_Staff_ID = a.StaffLocalCMCId
where b.Latest_Approver_Staff_ID is not null
 --select OriginalApproverWorkbaseID  from #MetadataDeptEids




 Update pd
		 
		 set OriginalAssessorEid = mso2.StaffEnterpriseID,
		  OriginalAssessor = isnull(ISNULL(mso2.StaffTitleDescription+' ','') + ISNULL(mso2.StaffForename+' ','') + mso2.StaffSurname,pd.ORIGINAL_ASSESSOR),
	      OriginalApproverEid = mso.StaffEnterpriseID,
		  OriginalApprover = isnull(isnull(ISNULL(mso.StaffTitleDescription+' ','') + ISNULL(mso.StaffForename+' ','') + mso.StaffSurname,pd.original_approver),'Not recorded'),
		  LatestAssessorEid = msl2.StaffEnterpriseID,
		  LatestAssessor = isnull(ISNULL(msl2.StaffTitleDescription+' ','') + ISNULL(msl2.StaffForename+' ','') + msl2.StaffSurname,pd.LATEST_ASSESSOR),
		  LatestApproverEid = msl.StaffEnterpriseID,
		  LatestApprover = isnull(isnull(ISNULL(msl.StaffTitleDescription+' ','') + ISNULL(msl.StaffForename+' ','') + msl.StaffSurname,pd.Latest_Approver),'Not recorded'),
		  OriginalAssessorEmail = mso2.AssessorEmail,
		  OriginalApproverEmail = mso.AssessorEmail,
		  LatestAssessorEmail = msl2.AssessorEmail,
		  LatestApproverEmail = msl.AssessorEmail,
		  OriginalAssessorJobTitle = isnull(isnull(mso2.StaffProviderTypeDescription,pd.ORIGINAL_JOB_TITLE),'Not recorded'),
		  OriginalApproverJobTitle = isnull(isnull(mso.StaffProviderTypeDescription,pd.ORIGINAL_APPROVER_JOB_TITLE),'Not recorded'),
		  LatestAssessorJobTitle = isnull(isnull(msl2.StaffProviderTypeDescription,pd.LATEST_JOB_TITLE),'Not recorded'),
		  LatestApproverJobTitle = isnull(isnull(msl.StaffProviderTypeDescription,pd.LATEST_APPROVER_JOB_TITLE),'Not recorded'),
		  OriginalAssessorODS = mso2.StaffODSCode,
		  OriginalApproverODS = mso.StaffODSCode,
		  LatestAssessorODS = msl2.StaffODSCode,
		  LatestApproverODS = msl.StaffODSCode,
		  mso_staffenterpriseid = mso.StaffEnterpriseID,
		  msl_staffenterpriseid = msl.staffenterpriseid,
		  mso2_staffenterpriseid = mso2.staffenterpriseid,
		  msl2_staffenterpriseid = msl2.staffenterpriseid 
 
  
from #MetadataDeptEids pd
left join Protocol.MetadataDeptEIDs e on e.CMC_ID = pd.CMC_ID
left join #AssessorDQInfoMSO mso on pd.Original_Approver_Staff_ID = mso.StaffLocalCMCId and e.originalapproverworkbaseeid = mso.deptenterpriseid
left join #AssessorDQInfoMSL msl on pd.Latest_Approver_Staff_ID = msl.StaffLocalCMCId and e.latestapproverworkbaseeid = msl.deptenterpriseid
left join #AssessorDQInfoMSO2 mso2 on pd.Original_Assessor_Staff_ID = mso2.StaffLocalCMCId and e.originalassessorworkbaseeid = mso2.deptenterpriseid
left join #AssessorDQInfoMSL2 msl2 on pd.Latest_Assessor_Staff_ID = msl2.StaffLocalCMCId and e.latestassessorworkbaseeid = msl2.deptenterpriseid 


update pd
		set pd.rn = b.rn

from #MetadataDeptEids pd
left join
			(
			Select
			cmc_id,
			ROW_NUMBER() over (partition by cmc_id order by mso_staffenterpriseid,msl_staffenterpriseid,mso2_staffenterpriseid,msl2_staffenterpriseid) as rn
			from #MetadataDeptEids
			)b on b.cmc_id = pd.CMC_ID



	--select * from #MetadataDeptEids

	
IF OBJECT_ID('[ETL_Local_PROD].[Protocol].[MetadataStaffEids]') IS NOT NULL 
dROP TABLE [ETL_Local_PROD].[Protocol].[MetadataStaffEids]
select 
[cmc_id]
      ,[OriginalAssessorEid]
      ,[OriginalAssessor]
      ,[OriginalApproverEid]
      ,[OriginalApprover]
      ,[LatestAssessorEid]
      ,[LatestAssessor]
      ,[LatestApproverEid]
      ,[LatestApprover]
      ,[OriginalAssessorEmail]
      ,[OriginalApproverEmail]
      ,[LatestAssessorEmail]
      ,[LatestApproverEmail]
      ,[OriginalAssessorJobTitle]
      ,[OriginalApproverJobTitle]
      ,[LatestAssessorJobTitle]
      ,[LatestApproverJobTitle]
      ,[OriginalAssessorODS]
      ,[OriginalApproverODS]
      ,[LatestAssessorODS]
      ,[LatestApproverODS]
      ,[rn]
into  [ETL_Local_PROD].[Protocol].[MetadataStaffEids]
from #MetadataDeptEids

END

