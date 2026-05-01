USE [ETL_Local_PROD]
GO

/****** Object:  View [dbo].[Cache-AuditApprovals-AllPublishedVersions]    Script Date: 15/05/2020 06:51:10 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


ALTER view [dbo].[Cache-AuditApprovals-AllPublishedVersions] as
select
p.cmc_id,
p.VersionNumber,
p.#Patient,
so.StaffEnterpriseID as ApproverEID,
ISNULL(so.StaffTitleDescription+' ','') + ISNULL(so.StaffForename+' ','') + so.StaffSurname as Approver,
so.AssessorEmail as ApproverEmail,
so.StaffProviderTypeDescription as ApproverJobTitle,
do.DeptEnterpriseID as ApproverWorkbaseEid,
do.DeptName as ApproverWorkbase,
do.LocalCMCOrgTypeDescription as ApproverProfGroup,
do.DeptODSCode as ApproverWorkbaseODS,
do.WorkbaseEmail as ApproverWorkbaseEmail,
so.StaffODSCode as ApproverODS
from [PatientDetail-AllPublishedVersions] p
left join [AuditPatient-CarePlan] ao on #ApprovedBy = ao.FromCarePlan
and ao.ActionType = 'publish'
left join AssessorDQInfo so on ao.StaffRegistryId = so.StaffRegistryId and ao.deptpdregistryid = so.deptpdregistryid
left join WorkbaseDQInfo do on ao.DeptPDRegistryId = do.DeptPDRegistryID



GO


