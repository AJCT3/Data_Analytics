USE [ETL_Local_PROD]
GO

/****** Object:  View [Protocol].[Cache-MetadataDeptEids]    Script Date: 14/05/2020 14:07:24 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO








ALTER view [Protocol].[Cache-MetadataDeptEids] as
 --proper denormalisation of old system data MS 12.3.17
select * from
(
select
pd.cmc_id,
pdo2.DeptEnterpriseID as OriginalAssessorWorkbaseEid,
pdl2.DeptEnterpriseID as LatestAssessorWorkbaseEid,
pdo.DeptEnterpriseID as OriginalApproverWorkbaseEid,
pdl.DeptEnterpriseID as LatestApproverWorkbaseEid,
isnull(cast(pdo2.DeptName as nvarchar(max)),pd.ORIGINAL_WORKBASE) as OriginalAssessorWorkbase,
isnull(cast(pdl2.DeptName as nvarchar(max)),pd.LATEST_WORKBASE) as LatestAssessorWorkbase,
isnull(isnull(cast(pdo.DeptName as nvarchar(max)),pd.ORIGINAL_APPROVER_WORKBASE),'Not recorded') as OriginalApproverWorkbase,
isnull(isnull(cast(pdl.DeptName as nvarchar(max)),pd.LATEST_APPROVER_WORKBASE),'Not recorded') as LatestApproverWorkbase,
isnull(pdo2.LocalCMCOrgTypeDescription,
case 
  when Original_Prof_Group='Acute' then 'Acute Trust'
  when Original_Prof_Group='CMC' then 'CMC Team'
  when Original_Prof_Group='Community' then 'Community Trust'
  when Original_Prof_Group='GP' then 'General Practice'
  else ORIGINAL_PROF_GROUP end) as OriginalAssessorProfGroup,
isnull(pdl2.LocalCMCOrgTypeDescription,
case 
  when Latest_Prof_Group='Acute' then 'Acute Trust'
  when Latest_Prof_Group='CMC' then 'CMC Team'
  when Latest_Prof_Group='Community' then 'Community Trust'
  when Latest_Prof_Group='GP' then 'General Practice'
  else Latest_PROF_GROUP end) as LatestAssessorProfGroup,
isnull(pdo.LocalCMCOrgTypeDescription,
case 
  when Original_Approver_Prof_Group='Acute' then 'Acute Trust'
  when Original_Approver_Prof_Group='CMC' then 'CMC Team'
  when Original_Approver_Prof_Group='Community' then 'Community Trust'
  when Original_Approver_Prof_Group='GP' then 'General Practice'
  else ORIGINAL_Approver_PROF_GROUP end) as OriginalApproverProfGroup,
isnull(pdl.LocalCMCOrgTypeDescription,
case 
  when Latest_Approver_Prof_Group='Acute' then 'Acute Trust'
  when Latest_Approver_Prof_Group='CMC' then 'CMC Team'
  when Latest_Approver_Prof_Group='Community' then 'Community Trust'
  when Latest_Approver_Prof_Group='GP' then 'General Practice'
  else Latest_Approver_PROF_GROUP end) as LatestApproverProfGroup,
isnull(pdo2.DeptODSCode,Original_Workbase_ODS) as OriginalAssessorWorkbaseODS,
isnull(pdl2.DeptODSCode,Latest_Workbase_ODS) as LatestAssessorWorkbaseODS,
isnull(isnull(pdo.DeptODSCode,Original_Approver_Workbase_ODS),'Not recorded') as OriginalApproverWorkbaseODS,
isnull(isnull(pdl.DeptODSCode,Latest_Approver_Workbase_ODS),'Not recorded') as LatestApproverWorkbaseODS,
pdo2.WorkbaseEmail as OriginalAssessorWorkbaseEmail,
pdl2.WorkbaseEmail as LatestAssessorWorkbaseEmail,
pdo.WorkbaseEmail as OriginalApproverWorkbaseEmail,
pdl.WorkbaseEmail as LatestApproverWorkbaseEmail,
ROW_NUMBER() over (partition by pd.cmc_id order by pdo.deptenterpriseid,pdl.deptenterpriseid,pdo2.deptenterpriseid,pdl2.deptenterpriseid) as rn
from
protocol.OldSystemCarePlans pd
left join WorkbaseDQInfo pdo on pd.OriginalApproverWorkbaseID = pdo.DeptLocalCMCId
left join WorkbaseDQInfo pdl on pd.LatestApproverWorkbaseID = pdl.DeptLocalCMCId
left join WorkbaseDQInfo pdo2 on pd.OriginalWorkbaseId = pdo2.DeptLocalCMCId
left join WorkbaseDQInfo pdl2 on pd.LatestWorkbaseId = pdl2.DeptLocalCMCId

) sel1 where rn=1






GO


