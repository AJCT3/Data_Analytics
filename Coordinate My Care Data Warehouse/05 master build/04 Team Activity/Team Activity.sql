




 
 use ETL_Local_PROD
 
GO
/****** Object:  StoredProcedure [dbo].[NewCache]    Script Date: 07/01/2020 13:12:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






Alter PROCEDURE [dbo].[AT_MASTERBUILD_Team_Audit_Build] 

AS
BEGIN





  --if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_AuditApprovals]') is not null
	truncate table [ETL_Local_PROD].[dbo].[AT_AuditApprovals]
	insert into  [ETL_Local_PROD].[dbo].[AT_AuditApprovals]
	select  
	p.cmc_id,
	OriginalApprovedBy,
	LatestApprovedBy,
	cast(ao.StaffRegistryId as nvarchar(255)) as From_StaffRegistryId,
	cast(ao.deptpdregistryid as nvarchar(255))  as From_deptpdregistryid,
	cast(al.StaffRegistryId as nvarchar(255))  as To_StaffRegistryId,
	cast(al.deptpdregistryid as nvarchar(255))  as To_deptpdregistryid,
	--so.StaffEnterpriseID as OriginalApproverEID,
	null as OriginalApproverEID,
	--sl.StaffEnterpriseID as LatestApproverEID,
	null as LatestApproverEID,
	--ISNULL(so.StaffTitleDescription+' ','') + ISNULL(so.StaffForename+' ','') + so.StaffSurname as OriginalApprover,
	--ISNULL(sl.StaffTitleDescription+' ','') + ISNULL(sl.StaffForename+' ','') + sl.StaffSurname as LatestApprover,
	cast(null as varchar(max)) as OriginalApprover,
	cast(null as varchar(max)) as LatestApprover, 
	cast(null as varchar(max)) as OriginalApproverEmail,
	cast(null as varchar(max)) as LatestApproverEmail,
	cast(null as varchar(max)) as OriginalApproverJobTitle,
	cast(null as varchar(max)) as LatestApproverJobTitle,
	
	
	null as OriginalApproverWorkbaseEid,
	null as LatestApproverWorkbaseEid,
	cast(null as varchar(max)) as OriginalApproverWorkbase,
	cast(null as varchar(max)) as LatestApproverWorkbase,
	cast(null as varchar(max)) as OriginalApproverProfGroup,
	cast(null as varchar(max)) as LatestApproverProfGroup,
	cast(null as varchar(255)) as OriginalApproverWorkbaseODS,
	cast(null as varchar(255)) as LatestApproverWorkbaseODS,
	cast(null as varchar(max)) as OriginalApproverWorkbaseEmail,
	cast(null as varchar(max)) as LatestApproverWorkbaseEmail,
	cast(null as varchar(max)) as OriginalApproverODS,
	cast(null as varchar(max)) as LatestApproverODS

 

	from [ETL_Local_PROD].[dbo].[AT_Patient_General] p
	left join [AuditPatient-CarePlan] ao on ao.FromCarePlan = p.OriginalApprovedBy  
										and ao.ActionType = 'publish'
	left join [AuditPatient-CarePlan] al on al.FromCarePlan = p.LatestApprovedBy 
										and al.ActionType = 'publish'



	update r

			set r.OriginalApproverEID = so.StaffEnterpriseID,
				--r.LatestApproverEID = sl.StaffEnterpriseID,
				r.OriginalApprover = ISNULL(so.StaffTitleDescription+' ','') + ISNULL(so.StaffForename+' ','') + so.StaffSurname,
				--r.LatestApprover = ISNULL(sl.StaffTitleDescription+' ','') + ISNULL(sl.StaffForename+' ','') + sl.StaffSurname,
				r.OriginalApproverEmail = so.AssessorEmail,
				--r.LatestApproverEmail = sl.AssessorEmail,
				r.OriginalApproverJobTitle = so.StaffProviderTypeDescription ,
				--r.LatestApproverJobTitle = sl.StaffProviderTypeDescription,
				---- Add Approver ODS codes MS 11.3.17
				r.OriginalApproverODS = so.StaffODSCode
				--,
				--r.LatestApproverODS = sl.StaffODSCode


			from [ETL_Local_PROD].[dbo].[AT_AuditApprovals]r
 			left join [ETL_Local_PROD].[dbo].[AT_AssessorDQInfo]  so on so.StaffRegistryId	= r.From_StaffRegistryId 
																	and so.deptpdregistryid = r.From_deptpdregistryid 
		   --left join [ETL_Local_PROD].[dbo].[AT_AssessorDQInfo]  sl on sl.StaffRegistryId	= r.To_StaffRegistryId 
					--												and sl.deptpdregistryid = r.To_deptpdregistryid 
			where From_StaffRegistryId is not null
			
	


	

	update t

			set  t.LatestApproverEID = sl.StaffEnterpriseID,
				t.LatestApprover = ISNULL(sl.StaffTitleDescription+' ','') + ISNULL(sl.StaffForename+' ','') + sl.StaffSurname,
				t.LatestApproverEmail = sl.AssessorEmail,
				t.LatestApproverJobTitle = sl.StaffProviderTypeDescription,
				-- Add Approver ODS codes MS 11.3.17
				t.LatestApproverODS = sl.StaffODSCode


			from [ETL_Local_PROD].[dbo].[AT_AuditApprovals]t
  
		   left join [ETL_Local_PROD].[dbo].[AT_AssessorDQInfo]  sl on sl.StaffRegistryId	= t.To_StaffRegistryId 
																	and sl.deptpdregistryid = t.To_deptpdregistryid 
			where To_StaffRegistryId is not null
			







	--select count(*) from #AssessorDQInfo


	update s

			set s.OriginalApproverWorkbaseEid = do.DeptEnterpriseID,
				s.LatestApproverWorkbaseEid = dl.DeptEnterpriseID,
				s.OriginalApproverWorkbase = do.DeptName,
				s.LatestApproverWorkbase = dl.DeptName,
				s.OriginalApproverProfGroup = do.LocalCMCOrgTypeDescription,
				s.LatestApproverProfGroup = dl.LocalCMCOrgTypeDescription,
				s.OriginalApproverWorkbaseODS = do.DeptODSCode,
				s.LatestApproverWorkbaseODS = dl.DeptODSCode,
				s.OriginalApproverWorkbaseEmail = do.WorkbaseEmail,
				s.LatestApproverWorkbaseEmail = dl.WorkbaseEmail
	 
			from [ETL_Local_PROD].[dbo].[AT_AuditApprovals]s
			left join #WorkbaseDQInfo do on do.DeptPDRegistryID = s.From_deptpdregistryid 
			left join #WorkbaseDQInfo dl on dl.DeptPDRegistryID = s.To_deptpdregistryid 



			update x 
			
					set x.OriginalApproverWorkbaseEId = y.OriginalApproverWorkbaseEid,
						x.OriginalApprover = y.OriginalApprover,
						x.OriginalApproverJobTitle = y.OriginalApproverJobTitle,
						x.OriginalApproverWorkbase = y.OriginalApproverWorkbase,
						x.OriginalApproverWorkbaseODS = y.OriginalApproverWorkbaseODS,
						x.Original_Approver_Prof_Group = y.OriginalApproverWorkbaseODS			
			
			from [ETL_Local_PROD].[dbo].[AT_Patient_General]x
			left join [ETL_Local_PROD].[dbo].[AT_AuditApprovals]y on y.cmc_id = x.cmc_id  




			--select top 80 * from [ETL_Local_PROD].[dbo].[AT_AuditApprovals]

			--select * from [ETL_Local_PROD].[dbo].[AT_Patient_General]
    if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_AccuratelyEnteredBy]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_AccuratelyEnteredBy]

			SELECT [CMC_ID]
      ,ap.PatAuditID as AuditId
      ,ToPatientSummary as [PatientSummary]
      ,[ActionTime]
      ,[ActionType]
      ,d.*,s.*

	  into [ETL_Local_PROD].[dbo].[AT_AccuratelyEnteredBy]

  FROM [ETL_Local_PROD].[dbo].[AuditPatient_New] ap
  left join AT_Staff s on ap.StaffRegistryId = s.StaffRegistryId
  left join [AT_PD_Dept] d on ap.DeptPDRegistryId = d.DeptPDRegistryId
  where ActionType in ('create','revise')






      if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_EpisodeTeams]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_EpisodeTeams]
  --[dbo].[EpisodeTeams]
--as
 
select 
p.cmc_id,
replace(eb.actiontype,'revise','update') as ActivityType,
eb.DeptEnterpriseId as ActivityDeptId,
-- MS 21.2.16 add episode date so we can correct Update handling on Demo Stats report
ActionTime as EpisodeDate
-- MS 3.2.16 change from createdby to enteredby for consistency with PatientDetail logic
-- MS 20.2.16 use accurate source for EnteredBy information

into [ETL_Local_PROD].[dbo].[AT_EpisodeTeams]

from [ETL_Local_PROD].[dbo].[AT_AccuratelyEnteredBy] eb
join [ETL_Local_PROD].[dbo].[AT_Patient_General] p on eb.cmc_id = p.cmc_id 



select * from [ETL_Local_PROD].[dbo].[AT_DisambiguatedOriginatingTeams]


      if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_DisambiguatedOriginatingTeams]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_DisambiguatedOriginatingTeams]
--[Reporting].[DisambiguatedOriginatingTeams] as
select 
distinct 
name7 + isnull('/' + case when name6 = 'COORDINATE MY CARE TEAM' then null else name6 end,'') + ISNULL(', '+ods7,'') as team,
Name7 as TeamName,
Name6 as CCG,
eid7,
activitydeptid as original_workbase_id,
activitydeptid
into [ETL_Local_PROD].[dbo].[AT_DisambiguatedOriginatingTeams]
from [ETL_Local_PROD].[dbo].[AT_EpisodeTeams] e 
join AT_Dept_Heirarchy h on e.activitydeptid = h.eid7



	IF OBJECT_ID('tempdb..#TempCMCCG') IS NOT NULL 
	dROP TABLE #TempCMCCG
select 
cmc_id,
ccg 
into #TempCMCCG
from 
[ETL_Local_PROD].[dbo].[AT_Patient_General]

 

 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_TeamAudit]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_TeamAudit]
select
*
into
[ETL_Local_PROD].[dbo].[AT_TeamAudit]
from
(
select
pa.ADAuditID as PatAuditID,
'Yes' as OnNewSystem,
[User Name],
[ActivityDate],
ROW_NUMBER() over (partition by pa.cmc_id  order by [ActivityDate]) as ActivityOrder,
pa.cmc_id, 
surname, 
left(gender,1) as Gender, 
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
left join [ETL_Local_PROD].[dbo].[AT_Patient_General] demo on demo.CMC_ID = pa.cmc_id
-- Exclude view and revise activities by originating team on day of origination
where CAST(activitydate as DATE) <> cast(Add_Date as date) or OriginalWorkbaseEId <> ActivityEnterpriseId or REPLACE([Access Type],'custom-','') not in ('view','revise')

--union all
--select
--cast( (20199+ ROW_NUMBER() over ( order by pa.cmc_id,ActivityDate )) as varchar(36)) as PatAuditID,
--'No' as OnNewSystem,
--[User Name],
--[ActivityDate],
--ROW_NUMBER() over (partition by pa.cmc_id  order by [ActivityDate]) as ActivityOrder,
--pa.cmc_id, 
--surname, 
--gender, 
--dob, 
--d2.DeptEnterpriseID as originalworkbaseeid, 
--isnull(ccg,isnull(PCT,'Not recorded')) as ccg,
--case [Access Type]
--  when 'Access Denied Record' then 'access denied (restricted)'
--  when 'approved' then 'approved by clinician'
--  when 'Associate PDS Record' then 'connect to PDS record'
--  when 'Auto Associate PDS Record' then 'auto connect to PDS record'
--  when 'Auto Dissociate PDS Record' then 'auto disconnect from PDS record'
--  when 'cancelled' then 'discard'
--  when 'completed' then 'completed post approval'
--  when 'created' then 'create care plan version'
--  when 'Delete Record' then 'delete'
--  when 'Dissociate PDS Record' then 'disconnect from PDS record'
--  when 'Hard Delete Record' then 'hard delete'
--  when 'Insert Record' then 'create demographics'
--  when 'Insert Episode' then 'create care plan version'
--  when 'mark-dup' then 'marked as duplicate'
--  when 'Update Episode' then 'save care plan version'
--  when 'Update Record' then 'update demographics'
--  when 'Print Episode' then 'print care plan version'
--  when 'Print Record' then 'print'
--  when 'published' then 'publish'
--  when 'rejected' then 'rejected by clinician'
--  when 'Restrict Record' then 'restrict'
--  when 'View Record' then 'view'
--  when 'View Episode' then 'view care plan version'
--  else [Access Type] end +
--  case when pd.cmc_id is null then ' (later re-entered on new system)'
--  else '' end as [Access Type],
--MONTH(ActivityDate) as [Month],
--case rtrim(TeamType)
--  when '111' then '111 Provider'
--  when 'Acute' then 'Acute Trust'
--  when 'Ambulance' then 'Ambulance Trust'
--  when 'CMC' then 'CMC Team'
--  when 'CCG' then 'Community Trust'
--  when 'Community' then 'Community Trust'
--  when 'GP' then 'General Practice'
--  when 'OOH' then 'Out Of Hour GP Provider'
--  else TeamType end as TeamType,
--d.DeptEnterpriseID as ActivityEnterpriseId,
--d.DeptName as ActivityTeam,
--s.StaffEnterpriseID
--from Protocol.AccessDataDetail2 pa with (nolock)
--join AT_PD_Dept d on pa.ActivityDepartmentId = d.DeptLocalCMCId
--left join Protocol.PatientDemographics demo on demo.CMC_ID = pa.cmc_id
--left join AT_PD_Dept d2 on cast(demo.Original_Workbase_Id as varchar(25)) = d2.DeptLocalCMCId
--left join #TempCMCCG pd on pd.CMC_ID = pa.cmc_id
--left join at_staff s on pa.PrimaryStaffId = s.StaffLocalCMCId
---- Exclude view and revise activities by originating team on day of origination
--where ((CAST(pa.ActivityDate as DATE) <> cast(Add_Date as date)
---- deal with non-numeric local cmc ids MS 29.5.17
--or ActivityDepartmentId <> cast(Original_Workbase_Id as varchar)
--or [Access Type] not in ('InsertEpisode','UpdateEpisode','Update Record','View Record','ViewEpisode')))
---- Exclude creation of first episode (which may be on subsequent day to creation of record, in old system)
--and ((cast(ActivityDate as date) <> cast(Date_Original_Assessment as date) or [Access Type] not in ('Insert Episode','Update Episode','Update Record')))
) sel1






      if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_DisambiguatedActivityTeams]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_DisambiguatedActivityTeams]

--LTER view [Reporting].[Cache-DisambiguatedActivityTeams] as
select distinct * into [ETL_Local_PROD].[dbo].[AT_DisambiguatedActivityTeams] 
from

(select 
distinct name7 + isnull('/' + case when name6 = 'COORDINATE MY CARE TEAM' then null else name6 end,'') + ISNULL(', '+ods7,'') as team,
Name7 as TeamName,
Name6 as CCG,
eid7,
activityenterpriseid as ActivityDepartmentID
 from [ETL_Local_PROD].[dbo].[AT_TeamAudit] a 
 join AT_Dept_Heirarchy h on activityenterpriseid = h.eid7

union all

select 
distinct name7 + isnull('/' + case when name6 = 'COORDINATE MY CARE TEAM' then null else name6 end,'') + ISNULL(', '+ods7,'') as team,
Name7 as TeamName,
Name6 as CCG,
eid7,
eid7 as ActivityDepartmentID
 from AT_Dept_Heirarchy h where typedesc7 in ('Out Of Hour GP Provider','111 Provider','Ambulance Trust')
-- add login teams if not already present MS 12.4.16

union all

select 
distinct name7 + isnull('/' + case when name6 = 'COORDINATE MY CARE TEAM' then null else name6 end,'') + ISNULL(', '+ods7,'') as team,
Name7 as TeamName,
Name6 as CCG,
eid7,
eid7 as ActivityDepartmentID
 from [ETL_Local_PROD].[dbo].[AT_AuditAuthentication] a 
 join [AT_PD_Dept] d on d.DeptPDRegistryID = a.OrganizationRegistryID
 join AT_Dept_Heirarchy h on h.eid7 = d.DeptEnterpriseID
-- add availability service callers if not already present MS 12.4.16

union all

select 
distinct name7 + isnull('/' + case when name6 = 'COORDINATE MY CARE TEAM' then null else name6 end,'') + ISNULL(', '+ods7,'') as team,
Name7 as TeamName,
Name6 as CCG,
eid7,
eid7 as ActivityDepartmentID
 from (select distinct ODS from ServiceSearch) ss   ---ooooooooooooooooooooooooooooooOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOooooooooooooooooooooooooooooooooooooooOOOOOOOOOOOOOOOOooooooooooooo
 join [AT_PD_Dept] d on d.DeptODSCode = ss.ODS
 join AT_Dept_Heirarchy h on h.eid7 = d.DeptEnterpriseID
-- add generic availability service callers if not already present MS 9.5.17

union all

select 
distinct name7 + isnull('/' + case when name6 = 'COORDINATE MY CARE TEAM' then null else name6 end,'') + ISNULL(', '+ods7,'') as team,
Name7 as TeamName,
Name6 as CCG,
eid7,
eid7 as ActivityDepartmentID
 from (select distinct ODS from ServiceSearchGeneric) ss  ---ooooooooooooooooooooooooooooooOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOooooooooooooooooooooooooooooooooooooooOOOOOOOOOOOOOOOOooooooooooooo
 join [AT_PD_Dept] d on d.DeptODSCode = ss.ODS
 join AT_Dept_Heirarchy h on h.eid7 = d.DeptEnterpriseID) sel1



 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

 --[dbo].[Cache-AvailabilityLog] as
--select ItemId as id,
--cast(actiontime as datetime) as EventDateTime,
--cast(actiontime as date) as EventDate,
--case when MRNs='0' then 'N' else 'Y' end as match,
--'EMIS' as Service, DeptEnterpriseID, Team
--from ServiceSearch a
---- logic here heeds to take close dates into account and also to follow Gareth's instructions re filtering
--join (select deptodscode,deptenterpriseid,ROW_NUMBER() over (PARTITION by deptodscode order by deptodscode) as rn from PDDepartment) d on a.ODS = DeptODSCode and d.rn=1
---- enforce uniqueness MS 29.5.16
--join (select *,
--row_number() over (partition by activitydepartmentid order by team) rn 
--from Reporting.DisambiguatedActivityTeams) at on at.ActivityDepartmentID = d.DeptEnterpriseID and at.rn=1







 --select * from [ETL_Local_PROD].[dbo].[AT_DisambiguatedActivityTeams] where eid7 = 100043405


-- select * from (
--select
--LoginDate,
--LoginTime,
--LogoutTime,
--isnull(Team,'(Organisation not identifiable at login time)') as Team,
--isnull(UserId,'(User Id not identifiable at login time)') + isnull(' (' + case StaffProviderTypeDescription when '' then NULL else StaffProviderTypeDescription end + ')','') as UserId,
--case Domain when '%HS_EMIS' then 'EMIS' when '%HS_Smartcard' then 'Smartcard' when '%HS_PC' then 'Portal' else 'CMC' end as Authentication,
--LoginId,
--ActionType,
--case
--  when Domain <> '%HS_CC' then ''
--  when isMobile = 'TRUE' or (isMobile is null and (OSFamily = 'Android' or DeviceFamily = 'iPhone')) then 'Mobile'
--  when left(loginid,6) ='System' then 'Auto-Flagging'
--  else 'Desktop' end as App
--from (
--select * from AuditPatientAuthentication
--where ActionType in ('login','direct','view','create','revise','failed')
--and LoginDate between cast(@FromLoginDate as date) and cast(@LoginDate as date)
--and case Domain when '%HS_EMIS' then 'EMIS' when '%HS_PC' then 'Portal' when '%HS_Smartcard' then 'Smartcard' else 'CMC' end = @Authentication
--) ap
--left join [AT_PD_Dept] on OrganizationRegistryId = DeptPDRegistryId
--left join [ETL_Local_PROD].[dbo].[AT_DisambiguatedActivityTeams] on ActivityDepartmentId = DeptEnterpriseId
--left join AT_Staff on UserRegistryId = StaffRegistryId
--left join Load.Devices d on ap.UserAgent = d.UserAgent
--) s1
--where @Authentication <> 'CMC' or @CMCApp = '*' or App = @CMCApp
--order by 
--case actiontype when 'failed' then 0 when 'login' then 1 when 'direct' then 2 when 'create' then 3 when 'revise' then 4 else 5 end, logindate,authentication,team,userid,loginid				

		End