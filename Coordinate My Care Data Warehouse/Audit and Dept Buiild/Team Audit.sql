




 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_TeamAudit]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_TeamAudit]
select
*
into
[ETL_Local_PROD].[dbo].[AT_TeamAudit]
from
(
select
pa.PatAuditID,
'Yes' as OnNewSystem,
[User Name],
[ActivityDate],
ROW_NUMBER() over (partition by pa.cmc_id  order by [ActivityDate]) as ActivityOrder,
pa.cmc_id, 
surname, left(gender,1) as Gender, dob, OriginalWorkbaseEId, isnull(CCG,'(Care plan not currently published)') as CCG,
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

union all
select
cast( (20199+ ROW_NUMBER() over ( order by pa.cmc_id,ActivityDate )) as varchar(36)) as PatAuditID,
'No' as OnNewSystem,
[User Name],
[ActivityDate],
ROW_NUMBER() over (partition by pa.cmc_id  order by [ActivityDate]) as ActivityOrder,
pa.cmc_id, 
surname, gender, dob, d2.DeptEnterpriseID as originalworkbaseeid, isnull(ccg,isnull(PCT,'Not recorded')) as ccg,
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
  else [Access Type] end +
  case when pd.cmc_id is null then ' (later re-entered on new system)'
  else '' end as [Access Type],
MONTH(ActivityDate) as [Month],
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
d.DeptEnterpriseID as ActivityEnterpriseId,
d.DeptName as ActivityTeam,
s.StaffEnterpriseID
from Protocol.AccessDataDetail2 pa with (nolock)
join PDDepartment d on pa.ActivityDepartmentId = d.DeptLocalCMCId
left join Protocol.PatientDemographics demo on demo.CMC_ID = pa.cmc_id
left join PDDepartment d2 on cast(demo.Original_Workbase_Id as varchar(25)) = d2.DeptLocalCMCId
left join (select cmc_id,ccg from PatientDetailSpan) pd on pd.CMC_ID = pa.cmc_id
left join Staff s on pa.PrimaryStaffId = s.StaffLocalCMCId
-- Exclude view and revise activities by originating team on day of origination
where ((CAST(pa.ActivityDate as DATE) <> cast(Add_Date as date)
-- deal with non-numeric local cmc ids MS 29.5.17
or ActivityDepartmentId <> cast(Original_Workbase_Id as varchar)
or [Access Type] not in ('InsertEpisode','UpdateEpisode','Update Record','View Record','ViewEpisode')))
-- Exclude creation of first episode (which may be on subsequent day to creation of record, in old system)
and ((cast(ActivityDate as date) <> cast(Date_Original_Assessment as date) or [Access Type] not in ('Insert Episode','Update Episode','Update Record')))
) sel1
