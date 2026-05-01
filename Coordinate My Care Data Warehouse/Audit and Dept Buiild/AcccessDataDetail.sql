USE [ETL_Local_PROD]
GO

 
 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_AccessDataDetail]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_AccessDataDetail]

select 
PatAuditID,
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
-- MS 11.3.16 add version information
ap.FromPatientSummary, ap.ToPatientSummary
 
from AuditPatient_New ap
join AT_Staff s on s.StaffRegistryId = ap.StaffRegistryId
join AT_PD_Dept d1 on d1.DeptPDRegistryId = ap.DeptPDRegistryId  
join Ind2Org io on io.StaffEnterpriseId = s.StaffEnterpriseId and io.DeptEnterpriseId = d1.DeptEnterpriseId


 
select
ap.Audit as PatAuditID,
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
-- MS 11.3.16 add version information
ap.FromPatientSummary, ap.ToPatientSummary

from AuditPatient ap
join Staff s on s.StaffRegistryId = ap.StaffRegistryId
join PDDepartment d1 on d1.DeptPDRegistryId = ap.DeptPDRegistryId
join Ind2Org io on io.StaffEnterpriseId = s.StaffEnterpriseId and io.DeptEnterpriseId = d1.DeptEnterpriseId





---Add indexes to table -------------------------------------------------------------------------
  /**   
	 ALTER TABLE [ETL_Local_PROD].[dbo].[AT_AccessDataDetail]
		ADD CONSTRAINT PK_AccessDetail PRIMARY KEY CLUSTERED (PatAuditID);

		      ALTER TABLE [ETL_Local_PROD].[dbo].[AT_AccessDataDetail]
				ALTER COLUMN [cmc_id] VARCHAR(75) ;



    CREATE INDEX ItemEnterprise
		ON [ETL_Local_PROD].[dbo].[AT_AccessDataDetail]  (PatAuditID, cmc_id);
 
	CREATE INDEX ID_Access_Date
		ON [ETL_Local_PROD].[dbo].[AT_AccessDataDetail]   (cmc_id, [Access Type],ActivityDate);
 
 **/
------------------------------------------------------------------------------------------------- 
 
 select top 5 * from [ETL_Local_PROD].[dbo].[AT_AccessDataDetail]


 
 select
'Yes' as OnNewSystem,
year(ActivityDate) as [Year],
datename(month,ActivityDate) as [Activity Month Name],
[User Name],
cast([ActivityDate] as date) as ActivityDate,
pa.cmc_id, 
surname, left(gender,1) as Gender, 
dob, 
OriginalWorkbaseEId, 
isnull(CCG,'(Care plan not currently published)') as CCG,
case [Access Type] when 'custom-consent_removed' then 'delete' else REPLACE([Access Type],'custom-','') end as [Access Type],
MONTH(ActivityDate) as [Month],
case
  when TeamType = 'Acute Trust' and CMCRoleDescription = 'isUrgentCare' then 'A&E'
  when rtrim(TeamType) = 'CCG' then 'Community Trust'
  else TeamType end as TeamType,
ActivityEnterpriseId,
name as ActivityTeam,
StaffEnterpriseId
from [ETL_Local_PROD].[dbo].[AT_AccessDataDetail] pa with (nolock)
left join PatientDetailSpan demo on demo.CMC_ID = pa.cmc_id
-- Exclude view and revise activities by originating team on day of origination
where CAST(activitydate as DATE) <> cast(Add_Date as date) or OriginalWorkbaseEId <> ActivityEnterpriseId or REPLACE([Access Type],'custom-','') not in ('view','revise')

