USE [ETL_Local_PROD]
GO
/****** Object:  StoredProcedure [dbo].[AT__Activity_Data]    Script Date: 14/05/2020 14:13:39 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




create PROCEDURE [dbo].[AT_AuditApprovals-AllPublishedVersions_SP] 
-- Amended for PD Upgrade
AS
BEGIN
 

IF OBJECT_ID('[ETL_Local_PROD].[dbo].[AuditApprovals-AllPublishedVersions]') IS NOT NULL 
dROP TABLE [ETL_Local_PROD].[dbo].[AuditApprovals-AllPublishedVersions]

select
distinct
p.cmc_id,
p.VersionNumber,
p.#Patient,
ao.deptpdregistryid,
ao.StaffRegistryId,
 

null as ApproverEID,
cast(null as varchar(max)) as Approver,
cast(null as varchar(max)) as ApproverEmail,
cast(null as varchar(max)) as ApproverJobTitle,
cast(null as varchar(max)) as ApproverODS,


null as ApproverWorkbaseEid,
cast(null as varchar(max)) as ApproverWorkbase,
cast(null as varchar(max)) as ApproverProfGroup,
cast(null as varchar(255)) as ApproverWorkbaseODS,
cast(null as varchar(max)) as ApproverWorkbaseEmail
into [ETL_Local_PROD].[dbo].[AuditApprovals-AllPublishedVersions]

from [PatientDetail-AllPublishedVersions] p
left join [AuditPatient_CarePlan_New] ao on #ApprovedBy = ao.FromCarePlan
and ao.ActionType = 'publish'

 --select top 50 * from [AuditPatient_CarePlan_New]
 --select * from #APV
 
 IF OBJECT_ID('tempdb..#WorkbaseDQInfo') IS NOT NULL 
dROP TABLE #WorkbaseDQInfo
select
distinct
DeptLocalCMCId,
DeptEnterpriseID,
DeptName,
LocalCMCOrgTypeDescription,
DeptODSCode,
WorkbaseEmail,
a.deptpdregistryid

into #WorkbaseDQInfo
from WorkbaseDQInfo a 
inner join [ETL_Local_PROD].[dbo].[AuditApprovals-AllPublishedVersions] b on  b.deptpdregistryid = a.deptpdregistryid


--select * from #WorkbaseDQInfo


IF OBJECT_ID('tempdb..#AssessorDQInfo') IS NOT NULL 
dROP TABLE #AssessorDQInfo
select
distinct
ISNULL(StaffTitleDescription+' ','') + ISNULL(StaffForename+' ','') + StaffSurname as Approver,
StaffLocalCMCId,
deptenterpriseid,
StaffEnterpriseID,
AssessorEmail,
StaffProviderTypeDescription,
StaffODSCode,
a.StaffRegistryId,
a.deptpdregistryid

into #AssessorDQInfo
from AssessorDQInfo a 
inner join [ETL_Local_PROD].[dbo].[AuditApprovals-AllPublishedVersions] b on b.StaffRegistryId = a.StaffRegistryId
				and b.deptpdregistryid = a.deptpdregistryid
 


 update ao

		set ApproverEID = so.StaffEnterpriseID, 
			ao.Approver = so.Approver,
			ApproverEmail = so.AssessorEmail,
			ApproverJobTitle = so.StaffProviderTypeDescription,
			ApproverODS = so.StaffODSCode
			--,
			--ApproverWorkbaseEid = do.DeptEnterpriseID,
			--ApproverWorkbase = do.DeptName,
			--ApproverProfGroup = do.LocalCMCOrgTypeDescription,
			--ApproverWorkbaseODS = do.DeptODSCode,
			--ApproverWorkbaseEmail = do.WorkbaseEmail
  

 from [ETL_Local_PROD].[dbo].[AuditApprovals-AllPublishedVersions] ao
left join #AssessorDQInfo so on ao.StaffRegistryId = so.StaffRegistryId and ao.deptpdregistryid = so.deptpdregistryid
--left join #WorkbaseDQInfo do on ao.DeptPDRegistryId = do.DeptPDRegistryID

 update ao

		set ApproverWorkbaseEid = do.DeptEnterpriseID,
			ApproverWorkbase = do.DeptName,
			ApproverProfGroup = do.LocalCMCOrgTypeDescription,
			ApproverWorkbaseODS = do.DeptODSCode,
			ApproverWorkbaseEmail = do.WorkbaseEmail
  

 from [ETL_Local_PROD].[dbo].[AuditApprovals-AllPublishedVersions] ao
left join #WorkbaseDQInfo do on ao.DeptPDRegistryId = do.DeptPDRegistryID




 END