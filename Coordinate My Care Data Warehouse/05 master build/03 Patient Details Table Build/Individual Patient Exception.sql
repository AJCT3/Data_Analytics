USE [ETL_Local_PROD]
GO

/****** Object:  View [dbo].[PDDeptEmails]    Script Date: 02/10/2020 11:16:14 ******/
 
 		IF OBJECT_ID('tempdb..#DEptEmails') IS NOT NULL 
		dROP TABLE #DEptEmails
SELECT 
distinct 
DeptEnterpriseId,
em.Address as Email

into #DEptEmails

FROM [ETL_PROD].[dbo].[CMC_Location] lo 
join AT_PD_Dept d on lo.OrganizationEID = deptenterpriseid
-- change CMC_Location_Contacts/CMC_ContactInfo_Emails to CMC_Location_Emails
join ETL_PROD.dbo.CMC_Location_Emails cp on cp.Location = Lo.ItemId
join ETL_PROD.dbo.CMC_Email em on cp.Email = em.ItemId
left join ETL_PROD.dbo.CMC_Location_DateSpan lods on lo.ItemId = lods.Location
left join ETL_PROD.dbo.CMC_DateSpan ds on lods.DateSpan = ds.ItemId
-- ensure ind->org info is omitted from this view MS 6.4.16
where lo.IndividualEID is null
-- exclude deleted and expired/unstarted locations MS 25.8.16
and lo.Deleted is null
-- exclude FLAGGING emails MS 17.2.17
and em.TypeCodedValue <> 'FLAGGING'
and (ds.StartDate is null or CAST(startdate as date) <= CAST(getdate() as DATE))
and (ds.EndDate is null or  CAST(enddate as date) > CAST(getdate() as DATE))
 

  		IF OBJECT_ID('tempdb..#PDOrgtoOrg') IS NOT NULL 
		dROP TABLE #PDOrgtoOrg
 select 
 * 
 
 into #PDOrgtoOrg

 from AT_ORGtoORG
-- 'Member' in 141001 MS 28.9.16
where Org2OrgType is null
or Org2OrgType = 'Member'




 		IF OBJECT_ID('tempdb..#PatientDQInfo') IS NOT NULL 
		dROP TABLE #PatientDQInfo

 select pd.*,
 rtrim(isnull(eb1.StaffTitleDescription + ' ','') + isnull(eb1.StaffForename + ' ','') + isnull(eb1.StaffSurname,'')) as Original_Assessor,
 eb1.DeptName as Original_Workbase,
 rtrim(isnull(isnull(eb.StaffTitleDescription,ae.StaffTitleDescription) + ' ','') +
  isnull(isnull(eb.StaffForename,ae.StaffForename) + ' ','') +
  isnull(isnull(eb.StaffSurname,ae.StaffSurname),'')) as Latest_Assessor,
  'Completed' as LatestAssessmentStatus,
isnull(eb.DeptName,ae.DeptName) as Latest_Workbase,
 
rvi.StaffProviderTypeDescription as PlannedReviewerJobTitle,
rvi.DeptName as PlannedReviewerWorkbase,
rvi.DeptODSCode as PlannedReviewerWorkbaseODS,
rvi.LocalCMCOrgTypeDescription as PlannedReviewerProfGroup,
PDSSurgery,
PDSCCG,
PDSEid,
PDSEmail,
pds.[Record Type],
pds.[Familiy name Output] as PDSSurname,
pds.[Given Name Output] as PDSForename,
case
  when pds.[Gender Output] = 1 then 'Male'
  when pds.[Gender Output] = 2 then 'Female'
  when pds.[Gender Output] is null then 'Not identified'
  else 'Unknown' end as PDSGender,
pds.[Postcode Output] as PDSPostcode,
CONVERT(varchar(25),cast(pds.[Date of Birth Output] as DATE),106) as PDSDoB

into #PatientDQInfo

from [ETL_Local_PROD].[dbo].[AT_Patient_General] pd
left join Load.PDS pds on pd.CMC_ID = pds.[Local PID]
left join
  (select DeptName as PDSSurgery, Parent as PDSCCG, DeptEnterpriseID as PDSEid, DeptODSCode,
     ROW_NUMBER() over (PARTITION by DeptODSCode order by DeptODSCode) as rn
   from AT_PD_Dept pd join #PDOrgtoOrg po
   on deptenterpriseid = ChildOrganizationEID) d on pds.[Reg  GP] = d.DeptODSCode and d.rn=1
left join
  (select Email as PDSEmail, DeptEnterpriseId as PDSEmailEid,
     ROW_NUMBER() over (PARTITION by DeptEnterpriseId order by DeptEnterpriseId) as rn
   from #DEptEmails) de on PDSEmailEId = PDSEId and de.rn=1
left join AT_StaffDeptContext eb1 on eb1.ProviderOrgContext = pd.OriginalEnteredBy
left join AT_StaffDeptContext eb on eb.ProviderOrgContext = pd.LatestEnteredBy
left join AccurateEnteredBy ae on ae.AuditId = LatestEnteredBy
left join AT_StaffDeptContext rvi on rvi.ProviderOrgContext = PlannedReviewer
   --[DataQuality].[NotPublished] as

   	IF OBJECT_ID('tempdb..#NotPublished') IS NOT NULL 
		dROP TABLE #NotPublished

select
p.CMC_ID,
case
  when DoD is not null then '1.Deceased on CMC'
  when DoD_PDS is not null then '2.Deceased on PDS'
  when OriginalAssessmentStatus is null or OriginalAssessmentStatus <> 'Completed'
    then '3.First Episode never Finalised'
  when hm.cmc_id is not null then '4.Not edited past original Adastra system'
  when cast(date_Latest_assessment as date) > CAST('2015-11-22' as date)
    or p.cmc_id in (28590,28286,26655,18346,28284,28351)
    then '5.Created or finalised on 23 or 24 Nov 2015'
  when am.cmc_id is not null then '6.Migrated but not yet published'
  else '' end as patient,
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
WorktrayName as Assessor,
WorktrayEmail as AssessorEmail,
WorktrayWorkbase as Workbase,
WorktrayWorkbaseEmail as WorkbaseEmail,
WorkbaseDeptId as WorkbaseId


into #NotPublished

from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join Protocol.HistoricMigrationAssessment hm on hm.cmc_id = p.cmc_id
left join (select distinct cmc_id from AuditMigration) am on am.cmc_id = p.CMC_ID

where OnNewSystem = 'No'


   	IF OBJECT_ID('tempdb..#OriginalAndLatest') IS NOT NULL 
		dROP TABLE #OriginalAndLatest
select
p.cmc_id,
'Originator, ' +
isnull(convert(varchar(15),date_original_approval,106) + ': ' +
ISNULL(aa.originalapprover,ms.originalapprover)
+ isnull(', ' + ISNULL(aa.originalapproveremail,ms.originalapproveremail),'') 
+ isnull(' of ' + ISNULL(aa.originalapproverworkbase,me.originalapproverworkbase),'')  
+ isnull(', ' + ISNULL(aa.originalapproverworkbaseemail,me.originalapproverworkbaseemail),''),
isnull(convert(varchar(15),date_original_assessment,106) + ': ' +
isnull(ms.originalassessor, rtrim(isnull(eb1.StaffTitleDescription + ' ','') + isnull(eb1.StaffForename + ' ','') + isnull(eb1.StaffSurname,'')) ),'(not identified)')
+ isnull(', ' + ms.originalassessoremail,'') 
+ isnull(' of ' + ISNULL(me.originalassessorworkbase, eb1.DeptName),'')  
+ isnull(', ' + me.originalassessorworkbaseemail,'')
)
as Original,
'Latest change: ' +
isnull(convert(varchar(15),date_latest_approval,106) + ': ' +
ISNULL(aa.latestapprover,ms.latestapprover)
+ isnull(', ' + ISNULL(aa.latestapproveremail,ms.latestapproveremail),'') 
+ isnull(' of ' + ISNULL(aa.latestapproverworkbase,me.latestapproverworkbase),'')  
+ isnull(', ' + ISNULL(aa.latestapproverworkbaseemail,me.latestapproverworkbaseemail),''),
isnull(convert(varchar(15),date_latest_assessment,106) + ': ' +
isnull(ms.latestassessor, rtrim(isnull(isnull(eb.StaffTitleDescription,ae.StaffTitleDescription) + ' ','') +
  isnull(isnull(eb.StaffForename,ae.StaffForename) + ' ','') +
  isnull(isnull(eb.StaffSurname,ae.StaffSurname),''))),'(not identified)')
+ isnull(', ' + ms.latestassessoremail,'') 
+ isnull(' of ' + ISNULL(me.latestassessorworkbase,isnull(eb.DeptName,ae.DeptName)),'')  
+ isnull(', ' + me.latestassessorworkbaseemail,'')
)
as Latest

into #OriginalAndLatest

from  [ETL_Local_PROD].[dbo].[AT_Patient_General_Historic_Inc] p
left join AuditApprovals aa on p.CMC_ID=aa.cmc_id
left join protocol.metadatastaffeids ms on p.CMC_ID=ms.cmc_id
left join protocol.metadatadepteids me on p.CMC_ID=me.cmc_id
left join AT_StaffDeptContext eb1 on eb1.ProviderOrgContext = p.OriginalEnteredBy
left join AT_StaffDeptContext eb on eb.ProviderOrgContext = p.LatestEnteredBy
left join AccurateEnteredBy ae on ae.AuditId = LatestEnteredBy
left join AT_StaffDeptContext rvi on rvi.ProviderOrgContext = PlannedReviewer
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- [DataQuality].[Cache-IndividualPatientExceptions] as
---- Amended for PD Upgrade

--select * from [ETL_Local_PROD].[dbo].[AT_IndividualPatientExceptions]

   	IF OBJECT_ID('[ETL_Local_PROD].[dbo].[AT_IndividualPatientExceptions]') IS NOT NULL 
		dROP TABLE [ETL_Local_PROD].[dbo].[AT_IndividualPatientExceptions]



select
[Exception Type],
p.cmc_id,
OnNewSystem,
surname,
gender,
p.patient,
DoB,
[Latest Date],
p.CCG,
p.DoD,
Assessor,
Workbase,
rtrim(isnull(gpi.StaffForename + ' ','') + isnull(gpi.StaffSurname,'')) as GP,
isnull('CMC practice: ' + GP_Practice + ', ','') + 'PDS practice: ' + ISNULL(case PDSSurgery when '' then '(not identified)' else pdssurgery end,'(not identified)') + isnull(' (' + PDSCCG + ')','') as Surgery,
isnull('CMC practice email: ' + gpe.Email + ', ','') + 'PDS practice email: ' + isnull(PDSEmail,'(none identified)') as PracticeEmail,
WorkbaseEmail,
AssessorEmail,
WorkbaseId,
-- Add originator and latest change information for Gerard MS 15.1.17
Original, Latest

into [ETL_Local_PROD].[dbo].[AT_IndividualPatientExceptions]

from
(
select
'0A.Not Published on New System-Deceased on CMC' as [Exception Type],
* from #NotPublished where Patient='1.Deceased on CMC'
union all
select
'0B.Not Published on New System-Deceased on PDS but not on CMC' as [Exception Type],
* from #NotPublished where Patient='2.Deceased on PDS'
union all
select
'0C.Not Published on New System-First Episode Never Finalised' as [Exception Type],
* from #NotPublished where Patient='3.First Episode never Finalised'
union all
select
'0D.Not Published on New System-Not edited past original Adastra system' as [Exception Type],
* from #NotPublished where Patient='4.Not edited past original Adastra system'
union all
select
'0E.Not Published on New System-Created or finalised on 23 or 24 Nov 2015' as [Exception Type],
* from #NotPublished where Patient='5.Created or finalised on 23 or 24 Nov 2015'
union all
select
'0F.Not Published on New System-Migrated but not yet published' as [Exception Type],
* from #NotPublished where Patient='6.Migrated but not yet published'

union all
SELECT
'1.First Episode in Draft - Patient Living on CMC and on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(OriginalAssessmentStatus,'No Episode') as [Patient],
isnull(Date_Original_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,original_assessor) as Assessor,
WorktrayEmail as AssessorEmail,
isnull(WorktrayWorkbase,original_workbase) as Workbase,
WorktrayWorkbaseEmail as WorkbaseEmail,
WorkbaseDeptId as WorkbaseId
from #PatientDQInfo p

left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
where OriginalAssessmentStatus = 'Draft'
and DoD is null and DoD_PDS is null

union all
SELECT
'1A.First Episode in Draft - Patient Deceased on CMC or on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(OriginalAssessmentStatus,'No Episode') as [Patient],
isnull(Date_Original_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,original_assessor) as Assessor,
WorktrayEmail as AssessorEmail,
isnull(WorktrayWorkbase,original_workbase) as Workbase,
WorktrayWorkbaseEmail as WorkbaseEmail,
WorkbaseDeptId as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
where OriginalAssessmentStatus = 'Draft'
and (DoD is not null or DoD_PDS is not null) 

union all
SELECT
'1B.First Episode Awaiting Approval - Patient Living on CMC and on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(OriginalAssessmentStatus,'No Episode') +
', see worktray info to right for approver' as [Patient],
isnull(Date_Original_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,original_assessor) as Assessor,
WorktrayEmail as AssessorEmail,
isnull(WorktrayWorkbase,original_workbase) as Workbase,
WorktrayWorkbaseEmail as WorkbaseEmail,
WorkbaseDeptId as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
where OriginalAssessmentStatus = 'Awaiting Approval'
and DoD is null and DoD_PDS is null

union all
SELECT
'1C.First Episode Awaiting Approval - Patient Deceased on CMC or on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(OriginalAssessmentStatus,'No Episode') +
', see worktray info to right for approver' as [Patient],
isnull(Date_Original_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,original_assessor) as Assessor,
WorktrayEmail as AssessorEmail,
isnull(WorktrayWorkbase,original_workbase) as Workbase,
WorktrayWorkbaseEmail as WorkbaseEmail,
WorkbaseDeptId as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
where OriginalAssessmentStatus = 'Awaiting Approval'
and (DoD is not null or DoD_PDS is not null) 

union all
SELECT
'2.Demographics Only - Patient Living on CMC and on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(OriginalAssessmentStatus,'No Episode') as [Patient],
isnull(Date_Original_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,original_assessor) as Assessor,
WorktrayEmail as AssessorEmail,
isnull(WorktrayWorkbase,original_workbase) as Workbase,
WorktrayWorkbaseEmail as WorkbaseEmail,
WorkbaseDeptId as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
where OriginalAssessmentStatus is null
and DoD is null and DoD_PDS is null

union all
SELECT
'2A.Demographics Only - Patient Deceased on CMC or on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(OriginalAssessmentStatus,'No Episode') as [Patient],
isnull(Date_Original_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,original_assessor) as Assessor,
WorktrayEmail as AssessorEmail,
isnull(WorktrayWorkbase,original_workbase) as Workbase,
WorktrayWorkbaseEmail as WorkbaseEmail,
WorkbaseDeptId as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
where OriginalAssessmentStatus is null
and (DoD is not null or DoD_PDS is not null) 

union all


SELECT
'3.No GP Practice - Patient Living on CMC and on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') + ', ' +
'PDS Surgery: ' + isnull([PDSSurgery] + ', PDS CCG: ' + [PDSCCG],'(none available)') as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,latest_assessor) as Assessor,
WorktrayEmail as AssessorEmail,
isnull(WorktrayWorkbase,latest_workbase) as Workbase,
WorktrayWorkbaseEmail as WorkbaseEmail,
WorkbaseDeptId as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
where GP_Practice is null
and DoD is null and DoD_PDS is null

union all
SELECT
'3A.No GP Practice - Patient Deceased on CMC or on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') + ', ' +
'PDS Surgery: ' + isnull([PDSSurgery] + ', PDS CCG: ' + [PDSCCG],'(none available)') as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,latest_assessor) as Assessor,
WorktrayEmail as AssessorEmail,
isnull(WorktrayWorkbase,latest_workbase) as Workbase,
WorktrayWorkbaseEmail as WorkbaseEmail,
WorkbaseDeptId as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
where GP_Practice is null
and (DoD is not null or DoD_PDS is not null) 

UNION ALL
SELECT
'4.Hidden Details (Assessment Approved) - Patient Living on CMC and on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') + ', ' +
'In worktray of ' + WorktrayName + ', ' + WorktrayWorkbase as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,latest_assessor) as Assessor,
WorktrayEmail as AssessorEmail,
isnull(WorktrayWorkbase,latest_workbase) as Workbase,
WorktrayWorkbaseEmail as WorkbaseEmail,
WorkbaseDeptId as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
where LatestAssessmentStatus = 'Assessment Approved'
and DoD is null and DoD_PDS is null

union all

SELECT
 '4A.Hidden Details (Assessment Approved) - Patient Deceased on CMC or on PDS' as [Exception Type],
 p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') + ', ' +
'In worktray of ' + WorktrayName + ', ' + WorktrayWorkbase as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,latest_assessor) as Assessor,
WorktrayEmail as AssessorEmail,
isnull(WorktrayWorkbase,latest_workbase) as Workbase,
WorktrayWorkbaseEmail as WorkbaseEmail,
WorkbaseDeptId as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
where LatestAssessmentStatus = 'Assessment Approved'
and (DoD is not null or DoD_PDS is not null) 

UNION ALL

SELECT
'5.Actual Place of Death Not Completed (Patient Deceased on CMC)' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where DeathLocation is null
and DoD  is not null 

UNION ALL
SELECT
'6.Blank or Invalid Forename - Patient Living on CMC and on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where (forename is null or forename like '%"%')
and DoD is null and DoD_PDS is null

UNION ALL

SELECT
'6A.Blank or Invalid Forename - Patient Deceased on CMC or on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where (forename is null or forename like '%"%')
and (DoD is not null or DoD_PDS is not null) 

UNION ALL

SELECT
'7.Marked Deceased on Spine but not on CMC' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') + ', ' +
'Date of Death on Spine: ' + convert(varchar, dod_PDS, 106) as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where DoD_PDS is not null
and DoD is null 

UNION ALL
SELECT
'8.Missing NHS Number & No Match on Spine - Patient Living on CMC' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where [record type] = '21'
and DoD is null and DoD_PDS is null

UNION ALL
SELECT
'8A.Missing NHS Number & No Match on Spine - Patient Deceased on CMC' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where [record type] = '21'
and (DoD is not null or DoD_PDS is not null) 

UNION ALL

SELECT
'9.Missing NHS Number & Multiple Matches on Spine - Patient Living on CMC' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where [record type] = '22'
and DOD is null and DoD_PDS is null

UNION ALL


SELECT
'9A.Missing NHS Number & Multiple Matches on Spine - Patient Deceased on CMC' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where [record type] = '22'
and (DoD is not null or DoD_PDS is not null) 

UNION ALL

SELECT
'D.Cross Border Patients - Patient Living on CMC and on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where CommissioningArea = 'Cross Border'
and DoD is null and DoD_PDS is null

UNION ALL

SELECT
'DA.Cross Border Patients - Patient Deceased on CMC or on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where CommissioningArea = 'Cross Border'
and (DoD is not null or DoD_PDS is not null) 

UNION ALL


SELECT 'E.Marked Deceased on CMC but not on Spine' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(DoD,Date_Latest_Assessment) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where PDS_NHS_Number = NHS_Number
and DoD is not null
and DoD_PDS is null
and cast(DoD as date) <= cast(PDS_Reconciliation_Date as date)

UNION ALL


SELECT
'F.Incorrect or Missing NHS Number, Successful Match on Spine - Patient Living on CMC and on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where [Record Type] in ('20','33') and (NHS_Number is null or NHS_Number <> PDS_NHS_Number)
and DoD is null and DoD_PDS is null

UNION ALL

SELECT
'FA.Incorrect or Missing NHS Number, Successful Match on Spine - Patient Deceased on CMC or on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where [Record Type] in ('20','33') and (NHS_Number is null or NHS_Number <> PDS_NHS_Number)
and (DoD is not null or DoD_PDS is not null) 

UNION ALL

SELECT
'G.Added ''GP'' not a practice GP - Patient Living on CMC and on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where CommissioningArea = 'Unknown'
and DoD is null and DoD_PDS is null

UNION ALL

SELECT
'GA.Added ''GP'' not a practice GP - Patient Deceased on CMC or on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where CommissioningArea = 'Unknown'
and (DoD is not null or DoD_PDS is not null) 

UNION ALL

SELECT
'H.Inappropriate Consent Setting - Patient Living on CMC and on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') + ', ' +
'Consent: ' + Consent as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where Consent in ('5.Refused','5.Withdrawn')
and DoD is null and DoD_PDS is null

UNION ALL

SELECT
'HA.Inappropriate Consent Setting - Patient Deceased on CMC or on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') + ', ' +
'Consent: ' + Consent as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where Consent in ('5.Refused','5.Withdrawn')
and (DoD is not null or DoD_PDS is not null) 

UNION ALL

SELECT
'P1.Invalid NHS Number & No Match on Spine - Patient Living on CMC' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where [record type] = '31'
and DoD is null and DoD_PDS is null

UNION ALL


SELECT
'P1A.Invalid NHS Number & No Match on Spine - Patient Deceased on CMC' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where [record type] = '31'
and (DoD is not null or DoD_PDS is not null) 

UNION ALL


SELECT
'P2.Invalid NHS Number, Multiple Matches on Spine - Patient Living on CMC' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where [record type] = '32'
and DoD is null and DoD_PDS is null

UNION ALL

SELECT
'P2A.Invalid NHS Number, Multiple Matches on Spine - Patient Deceased on CMC' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where [record type] = '32'
and (DoD is not null or DoD_PDS is not null) 

UNION ALL

SELECT
'P3.Insufficient Data for Spine Check - Patient Living on CMC' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where [record type] = '81'
and DoD is null and DoD_PDS is null

UNION ALL

SELECT
'P3A.Insufficient Data for Spine Check - Patient Deceased on CMC' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where [record type] = '81'
and (DoD is not null or DoD_PDS is not null) 

UNION ALL

SELECT
'P4B.Surname Mismatch - Patient Living on CMC and on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') + ', ' +
'PDS Surname: ' + PDSSurname as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where PDS_NHS_Number = NHS_Number and Surname <> PDSSurname
and DoD is null and DoD_PDS is null

UNION ALL

SELECT
'P4C.Surname Mismatch - Patient Deceased on CMC or on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') + ', ' +
'PDS Surname: ' + PDSSurname as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where PDS_NHS_Number = NHS_Number and Surname <> PDSSurname
and (DoD is not null or DoD_PDS is not null) 

UNION ALL

SELECT
'P4D.Forename Mismatch - Patient Living on CMC and on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
'PDS Forename: ' + PDSForename + ', ' +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where PDS_NHS_Number = NHS_Number and Forename <> PDSForename and Forename <> PDSForename
and DoD  is null and DoD_PDS is null

UNION ALL

SELECT
'P4E.Forename Mismatch - Patient Deceased on CMC or on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
'PDS Forename: ' + PDSForename + ', ' +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where PDS_NHS_Number = NHS_Number and Forename <> PDSForename and Forename <> PDSForename
and (DoD is not null or DoD_PDS is not null) 

UNION ALL

SELECT
'P5.Gender Mismatch - Patient Living on CMC and on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') + ', ' +
'PDS Gender: ' + PDSGender as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where PDS_NHS_Number = NHS_Number and left(Gender,1) <> left(PDSGender,1)
and DoD is null and DoD_PDS is null

UNION ALL


SELECT
'P5A.Gender Mismatch - Patient Deceased on CMC or on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') + ', ' +
'PDS Gender: ' + PDSGender as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where PDS_NHS_Number = NHS_Number and left(Gender,1) <> left(PDSGender,1)
and (DoD is not null or DoD_PDS is not null) 

UNION ALL

SELECT
'P6.Date of Birth Mismatch - Patient Living on CMC and on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') + ', ' +
'PDS Date of Birth: ' + PDSDoB as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where PDS_NHS_Number = NHS_Number and DoB <> PDSDoB
and Dod is null and DoD_PDS is null

UNION ALL

SELECT
'P6A.Date of Birth Mismatch - Patient Deceased on CMC or on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') + ', ' +
'PDS Date of Birth: ' + PDSDoB as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where PDS_NHS_Number = NHS_Number and DoB <> PDSDoB
and (DoD is not null or DoD_PDS is not null) 

UNION ALL

SELECT
'P7.Postcode Mismatch - Patient Living on CMC and on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'PDS Postcode: ' + PDSPostcode + ', ' +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where PDS_NHS_Number = NHS_Number and replace(Main_Postcode,' ','') <> replace(PDSPostcode,' ','')
and DoD is null and DoD_PDS is null

UNION ALL

SELECT
'P7A.Postcode Mismatch - Patient Deceased on CMC or on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'PDS Postcode: ' + PDSPostcode + ', ' +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where PDS_NHS_Number = NHS_Number and replace(Main_Postcode,' ','') <> replace(PDSPostcode,' ','')
and (DoD is not null or DoD_PDS is not null) 

UNION ALL

SELECT
'P8.Date of Death Mismatch' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode') + ', ' +
'CMC Date of Death: ' + convert(varchar, DoD, 106) + ', ' +
'PDS Date of Death: ' + convert(varchar, DoD_PDS, 106) as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where PDS_NHS_Number = NHS_Number
and (DoD is not null and DoD_PDS is not null
and DoD <> DOD_PDS) 

UNION ALL

SELECT
'P9.Surgery Mismatch - Patient Living on CMC and on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
isnull('Postcode on PDS: ' + PDSPostcode + ', ','') +
-- Add surgery values to exception details MS 10.3.16
isnull('Surgery on CMC: ' + GP_Practice + ', ','') +
isnull('Surgery on PDS: ' + pdssurgery + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where GP_Practice is not null and PDSSurgery is not null and GP_Practice <> PDSSurgery
and DoD is null and DoD_PDS is null

UNION ALL

SELECT
'P9A.Surgery Mismatch - Patient Deceased on CMC or on PDS' as [Exception Type],
p.CMC_ID,
isnull('NHS Number on CMC: ' + NHS_Number + ', ','') +
isnull('NHS Number on PDS: ' + PDS_NHS_Number + ', ','') +
isnull('Forename: ' + forename + ', ','') +
isnull('Postcode on CMC: ' + Main_Postcode + ', ','') +
isnull('Postcode on PDS: ' + PDSPostcode + ', ','') +
'Status: ' + isnull(LatestAssessmentStatus,'No Episode')  as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
where GP_Practice is not null and PDSSurgery is not null and GP_Practice <> PDSSurgery
and (DoD is not null or DoD_PDS is not null) 

UNION ALL

SELECT
'I.CPR/Ceiling of Treatment discrepancy - Patient Living on CMC and on PDS' as [Exception Type],
p.CMC_ID,
'CPR Decision: ' + isnull(rtrim(case when p.CARDIO_YN='Decision not yet made' then 'Decision not yet made so defaults to Yes' else p.CARDIO_YN end),'Not recorded so defaults to Yes') + ', ' +
'Ceiling of Treatment: ' + isnull(rtrim(p.CEILTREAT),'Not recorded') as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
join Reference.CPRCoT cc on case when p.CARDIO_YN is null then 'Not recorded' else RTRIM(p.cardio_yn) end = cc.CARDIO_YN and isnull(rtrim(p.CEILTREAT),'') = isnull(cc.CEILTREAT,'')
where DoD is null and DoD_PDS is null

UNION ALL

SELECT
'IA.CPR/Ceiling of Treatment discrepancy - Patient Deceased on CMC or on PDS' as [Exception Type],
p.CMC_ID,
'CPR Decision: ' + isnull(rtrim(case when p.CARDIO_YN='Decision not yet made' then 'Decision not yet made so defaults to Yes' else p.CARDIO_YN end),'Not recorded so defaults to Yes') + ', ' +
'Ceiling of Treatment: ' + isnull(rtrim(p.CEILTREAT),'Not recorded') as [Patient],
isnull(Date_Latest_Assessment,Add_Date) as [Latest Date],
isnull(DOD,DoD_PDS) as DoD,
isnull(WorktrayName,PlannedReviewer) as Assessor,
isnull(WorktrayEmail,AssessorEmail) as AssessorEmail,
isnull(WorktrayWorkbase,PlannedReviewerWorkbase) as Workbase,
isnull(WorktrayWorkbaseEmail,WorkbaseEmail) as WorkbaseEmail,
isnull(WorkbaseDeptId,sd.DeptEnterpriseId) as WorkbaseId
from #PatientDQInfo p
left join
  (select cmc_id,workbase as WorktrayWorkbase,name as WorktrayName,
          email as WorktrayEmail,workbase_email as WorktrayWorkbaseEmail,
          workbase_id as WorkbaseDeptId
   from Protocol.WorktrayAllocations) w on w.cmc_id = p.cmc_id and OnNewSystem = 'No'
left join AT_StaffDeptContext sd on sd.providerorgcontext = p.PlannedReviewer
left join AT_AssessorDQInfo a on sd.StaffEnterpriseID = a.StaffEnterpriseID
-- MS 3.11.16 indorg
and sd.deptenterpriseid = a.deptenterpriseid
left join AT_WorkbaseDQInfo wb on sd.DeptEnterpriseID = wb.DeptEnterpriseID 
left join Reference.CPRCoT cc on case when p.CARDIO_YN is null then 'Not recorded' else RTRIM(p.cardio_yn) end = cc.CARDIO_YN and isnull(rtrim(p.CEILTREAT),'') = isnull(cc.CEILTREAT,'')
where DoD is not null or DoD_PDS is not null

) sel1

join #PatientDQInfo p on sel1.cmc_id = p.cmc_id
left join #OriginalAndLatest ol on p.CMC_ID = ol.cmc_id
left join AT_PatientRegistered_GP gp on gp.patientsummary = p.PatientSummary
left join AT_StaffDeptContext gpi on gpi.ProviderOrgContext = gp.RegisteredGP
left join (select *,row_number() over (partition by DeptEnterpriseId order by Email) as rn from AT_Emails where Email like '%nhs.net') gpe on gpe.DeptEnterpriseId = gpi.DeptEnterpriseId and gpe.rn=1
 