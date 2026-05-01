USE [ETL_Local_PROD]
GO

/****** Object:  View [dbo].[Cache-PatientDetailFull]    Script Date: 05/03/2020 12:55:57 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



ALTER view [dbo].[Cache-PatientDetailFull] as
-- Columns with names starting # or ending _CDA are not shown on the All Data Business Continuity Report
select

-- Already derived
pd.*,

-- Identifiers
cast(PatientUUID as varchar(100)) as [UUID_CDA],
uu.Value as [TEMPLATE_UUID_CDA],

-- Genuine Available Care Plan Check
--case
--  when gpi.DeptName like '%TEST DOCTORS%'
--  or pd.cmc_id in (1791,15079)
--  or pd.NHS_Number in ('9990248753','9990248761','9990248796','9990248958',
--                       '9990248966','9990249040','9990249059','9990249172',
--                       '9990249199','9990402132','9990252947','9990252955',
--                       '9990243271','9990281025')
--  then 'Y' else 'N' end as #TestPatient,
CAST(NULL AS VARCHAR(1)) AS #TestPatient,
-- Metadata
CAST(NULL AS VARCHAR(MAX)) AS Original_Assessor,
CAST(NULL AS VARCHAR(MAX)) AS Original_Job_Title,
CAST(NULL AS VARCHAR(MAX)) AS Original_Workbase,
CAST(NULL AS VARCHAR(255)) AS Original_Workbase_ODS,
CAST(NULL AS VARCHAR(MAX)) AS Original_Prof_Group,
CAST(NULL AS INT) AS OriginalWorkbaseEId,
CAST(NULL AS VARCHAR(MAX)) AS OriginalWorkbaseEmail,
--rtrim(isnull(eb1.StaffTitleDescription + ' ','') + isnull(eb1.StaffForename + ' ','') + isnull(eb1.StaffSurname,'')) as Original_Assessor,
--eb1.StaffProviderTypeDescription as Original_Job_Title,
--eb1.DeptName as Original_Workbase,
--eb1.DeptODSCode as Original_Workbase_ODS,
--eb1.LocalCMCOrgTypeDescription as Original_Prof_Group,
--eb1.DeptEnterpriseID as OriginalWorkbaseEId,
--oe.Email as OriginalWorkbaseEmail,
ISNULL(ap.OriginalApprover,mse.OriginalApprover) as OriginalApprover,
isnull(ap.OriginalApproverJobTitle,mse.OriginalApproverJobTitle) as OriginalApproverJobTitle,
ISNULL(ap.OriginalApproverWorkbase,CAST(mde.OriginalApproverWorkbase AS VARCHAR(MAX))) as OriginalApproverWorkbase,
ISNULL(ap.OriginalApproverWorkbaseODS,mde.OriginalApproverWorkbaseODS) as OriginalApproverWorkbaseODS,
ISNULL(ap.OriginalApproverProfGroup,mde.OriginalApproverProfGroup) as Original_Approver_Prof_Group,
ISNULL(caST(ap.OriginalApproverWorkbaseEid AS VARCHAR(MAX)),mde.OriginalApproverWorkbaseEid) as OriginalApproverWorkbaseEId,

-- MS 18.3.17 Add original approver ODS code & role information
ISNULL(ap.OriginalApproverODS,mse.OriginalApproverODS) as OriginalApproverODS,
(select role from reference.RolesCDA r where r.job_title = isnull(ap.OriginalApproverJobTitle,mse.OriginalApproverJobTitle))
  as Original_Approver_Role,
isnull(ap.OriginalApproverJobTitle,mse.OriginalApproverJobTitle) as Original_Approver_Role_Description,
CAST(NULL AS VARCHAR(MAX)) AS Latest_Assessor,
CAST(NULL AS VARCHAR(MAX)) AS Latest_Job_Title,
CAST(NULL AS VARCHAR(255)) AS Latest_Assessor_Role,
CAST(NULL AS VARCHAR(MAX)) AS Latest_Assessor_Role_Description,
CAST(NULL AS VARCHAR(255)) AS Latest_Workbase_ODS,
CAST(NULL AS VARCHAR(MAX)) AS Latest_Prof_Group,
CAST(NULL AS INT) AS LatestWorkbaseEId,
CAST(NULL AS VARCHAR(MAX)) AS LatestWorkbaseEmail,

--rtrim(isnull(isnull(eb.StaffTitleDescription,ae.StaffTitleDescription) + ' ','') +
--  isnull(isnull(eb.StaffForename,ae.StaffForename) + ' ','') +
--  isnull(isnull(eb.StaffSurname,ae.StaffSurname),'')) as Latest_Assessor,
--isnull(eb.StaffProviderTypeDescription,ae.StaffProviderTypeDescription) as Latest_Job_Title,
-- Add Latest Assessor Role / Description MS 18.2.16
--(select role from reference.RolesCDA r where r.job_title = isnull(eb.StaffProviderTypeDescription,ae.StaffProviderTypeDescription))
--  as Latest_Assessor_Role,
--isnull(eb.StaffProviderTypeDescription,ae.StaffProviderTypeDescription) as Latest_Assessor_Role_Description,
--isnull(eb.DeptName,ae.DeptName) as Latest_Workbase,
--isnull(eb.DeptODSCode,ae.DeptODSCode) as Latest_Workbase_ODS,
--isnull(eb.LocalCMCOrgTypeDescription,ae.LocalCMCOrgTypeDescription) as Latest_Prof_Group,
--isnull(eb.DeptEnterpriseID,ae.DeptEnterpriseId) as LatestWorkbaseEId,
--le.Email as LatestWorkbaseEmail,
ISNULL(ap.LatestApprover,mse.LatestApprover) as LatestApprover,
isnull(ap.LatestApproverJobTitle,mse.LatestApproverJobTitle) as LatestApproverJobTitle,
ISNULL(ap.LatestApproverWorkbase,CAST(mde.LatestApproverWorkbase AS VARCHAR(MAX))) as LatestApproverWorkbase,
ISNULL(ap.LatestApproverWorkbaseODS,mde.LatestApproverWorkbaseODS) as LatestApproverWorkbaseODS,
ISNULL(ap.LatestApproverProfGroup,mde.LatestApproverProfGroup) as Latest_Approver_Prof_Group,
ISNULL(CAST(ap.LatestApproverWorkbaseEid AS VARCHAR(MAX)),mde.LatestApproverWorkbaseEid) as LatestApproverWorkbaseEId,


--select * from Protocol.MetadataDeptEids


-- MS 18.3.17 Add latest approver ODS code & role information
ISNULL(ap.LatestApproverODS,mse.LatestApproverODS) as LatestApproverODS,
(select role from reference.RolesCDA r where r.job_title = isnull(ap.LatestApproverJobTitle,mse.LatestApproverJobTitle))
  as Latest_Approver_Role,
isnull(ap.LatestApproverJobTitle,mse.LatestApproverJobTitle) as Latest_Approver_Role_Description,

-- Demographics
gc.Description as Gender,
case
  when age<18 then '0-17'
  when age>=18 and age<40 then '18-39'
  when age>=40 and age<50 then '40-49'
  when age>=50 and age<60 then '50-59'
  when age>=60 and age<70 then '60-69'
  when age>=70 and age<80 then '70-79'
  else '80+'
  end as #AgeBand,
msc.Description as Marital_Status,
etc.Description as Ethnicity,
rec.Description as Religion,
plc.Description as PrimaryLanguage,
-- Ignore 'Not Known' place of death - treat as not recorded MS 2.5.16
rtrim(case dlc.Description when 'Not Known' then NULL else dlc.Description end) as DODPLACE,
-- Trim trailing blanks MS 8.2.16
rtrim(dvc.Description) as Variance,
dr.DoD_Recorded,
case when pd.DOD_Demographics is null then 'Living' else 'Deceased' end as #DeceasedCMC,
datediff(day,cast(pd.add_date as date),cast(pd.DoD_Demographics as date)) as #DaysOnSystem_CMC,
datediff(day,cast(pd.add_date as date),cast(pd.DoD_PDS as date)) as #DaysOnSystem_PDS,
dbo.FirstName(Forename) as #First_Forename,
ad.Value as ADDRESSES_CDA,
case when ad.Value is null then null else dbo.htmlStripper(ad.Value) end as ADDRESSES,
ci.Value as CONTACT_INFO_CDA,
case when ci.Value is null then null else dbo.htmlStripper(ci.Value) end as CONTACT_INFO,
aa.Value as ALIASES_CDA,
case when aa.Value is null then null else dbo.htmlStripper(aa.Value) end as ALIASES,

-- GP Information
--rtrim(isnull(gpi.StaffForename + ' ','') + isnull(gpi.StaffSurname,'')) as GP,
CAST(NULL AS VARCHAR(MAX)) AS GP,
CAST(NULL AS VARCHAR(MAX)) AS GP_Title,
CAST(NULL AS VARCHAR(MAX)) AS SURGERY,
CAST(NULL AS VARCHAR(255)) AS  Practice_ODS,
CAST(NULL AS VARCHAR(MAX)) AS PracticeEmail,
CAST(NULL AS VARCHAR(MAX)) AS Practice_Address,
CAST(NULL AS VARCHAR(MAX)) AS Practice_Postcode,
--gpi.StaffTitleDescription as GP_Title,
--gpi.DeptName as Surgery,
--gpi.DeptODSCode as Practice_ODS,
--gpe.Email as PracticeEmail,
--gpa.CombinedAddress as Practice_Address,
--gpa.PostalCode as Practice_Postcode,
-- Add surgery dept enterprise id MS 23.2.16
--gpi.DeptEnterpriseId as PracticeEnterpriseId,
CAST(NULL AS INT) AS  PracticeEnterpriseId,
-- Add GP CDA information MS 18.2.16
--ISNULL(
--'<table>
--    <tbody>
--        <tr>
--            <th>GP Title</th>
--            <th>GP ODS</th>
--            <th>Surgery</th>
--            <th>Practice ODS</th>
--            <th>Practice Postcode</th>
--            <th>CCG</th>
--            <th>Practice Phone</th>
--            <th>Practice Mobile</th>
--        </tr>
--        <tr>
--            <td>' + ISNULL(gpi.StaffTitleDescription,'(N/A)') + '</td>
--            <td>' + ISNULL(gpi.StaffODSCode,'(N/A)') + '</td>
--            <td>' + ISNULL(gpi.DeptName,'(N/A)') + '</td>
--            <td>' + ISNULL(gpi.DeptODSCode,'(N/A)') + '</td>
--            <td>' + ISNULL(gpa.PostalCode,'(N/A)') + '</td>
--            <td>' + ISNULL(CCG,'(N/A)') + '</td>
--            <td>' + ISNULL(gpp.Telephone,'(N/A)') + '</td>
--            <td>' + ISNULL(gpm.Telephone,'(N/A)') + '</td>
--        </tr>
--    </tbody>
--</table>'
--,'') as GP_CDA,
cast( null as bigint) as GP_CDA,
-- Consent Information
rqc.Description as Req_Copy,
coc.Description as Consent,
--rtrim(isnull(rvi.StaffTitleDescription + ' ','') + isnull(rvi.StaffForename + ' ','') + isnull(rvi.StaffSurname,'')) as PlannedReviewer,
--rvi.StaffProviderTypeDescription as PlannedReviewerJobTitle,
--rvi.DeptName as PlannedReviewerWorkbase,
--rvi.DeptODSCode as PlannedReviewerWorkbaseODS,
--rvi.LocalCMCOrgTypeDescription as PlannedReviewerProfGroup,

-- Contacts
hs.Value as HSCContacts_CDA,
case when hs.Value is null then null else dbo.htmlStripper(hs.Value) end as HSCContacts,
per.Value as PersonalContacts_CDA,
case when per.Value is null then null else dbo.htmlStripper(per.Value) end as PersonalContacts,
lpa.Value as LPA_CDA,
case when lpa.Value is null then null else dbo.htmlStripper(lpa.Value) end as LPA,
case when nok.PatientSummary is null then 'N' else 'Y' end as NoKExists, 
case when lp.PatientSummary is null then 'N' else 'Y' end as PoAExists, 

-- Medical Background
--pri.StaffLocalCMCId as #Prognosis_Whom,
---- check this should not be login id
--rtrim(isnull(pri.StaffTitleDescription + ' ','') + isnull(pri.StaffForename + ' ','') + isnull(pri.StaffSurname,'')) as PrognosisClinician,
--pri.DeptName as PrognosisClinicianWorkbase,
---- add Prognosis Clinician Workbase ODS MS 18.3.17
--pri.DeptODSCode as PrognosisClinicianWorkbaseODS,
fapc.Description as PA_FAMPROD,
papc.Description as PA_PROD,
tfc.Description as ALT_PROGNOSIS,
sc.Description as Surprise,
adc.Description as ADRTExists,
fadc.Description as C_AWARE,
hdc.Description as COMMDIFF,
padc.Description as P_AWARE,
whc.Description as WHPERF,
di.Value as Diagnoses_CDA,
dbo.htmlStripper(di.Value) as Diagnoses,
-- Correct classified diagnosis handling MS 8.3.16
case dic.Description
  when 'Migrated' then demo.Classified_Diagnosis
-- Add details re primary and secondary sites MS 2.5.16
  when 'Cancer - Primary Site' then 'Cancer - Primary Site - ' + ddc.Description
  when 'Cancer - Secondary Site' then 'Cancer - Secondary Site - ' + ddc.Description
  else dic.Description end as Classified_Diagnosis,
dbo.DiagnosisCategory
(case dic.Description
  when 'Migrated' then demo.Classified_Diagnosis
-- Add details re primary and secondary sites MS 2.5.16
  when 'Cancer - Primary Site' then 'Cancer - Primary Site - ' + ddc.Description
  when 'Cancer - Secondary Site' then 'Cancer - Secondary Site - ' + ddc.Description
  else dic.Description end)
as DiagnosisCategory,
dd.Value as DISDET_CDA,
case when dd.Value is null then null else dbo.htmlStripper(dd.Value) end as DISDET,

-- CPR
cpc.Description as CARDIO_YN,
fdc.Description as RESUS_FAMILY,
pdc.Description as RESUS_PATIENT,
wac.Description as APPOINTWA,
cac.Description as VALIDAD,
ptac.Description as HAVECAP,
rtrim(isnull(isnull(cpri.StaffTitleDescription + ' ','') + cpri.StaffForename + ' ','') + isnull(cpri.StaffSurname,'')) as DNARNAME,
cpri.DeptName as CPRClinicianWorkbase,
-- MS 18.3.17 Add clinician workbase ODS code
cpri.DeptODSCode as CPRClinicianWorkbaseODS,
-- Add CPR Discussion flags MS 7.3.16
-- Remove them again - these are RESUS_FAMILY and RESUS_PATIENT ... MS 7.10.16
-- Social Background
dsc.Description as DS1500,
eqc.Description as EQUIP,
fsuc.Description as FAM_SUPPORT,
hhc.Description as HOMECARE,
prc.Description as CAREPLAN,

-- Medications
acc.Description as Anticoags,
inc.Description as Insulin,
opc.Description as OPIOID,
stc.Description as Steroids,
me.Value as Medications_CDA,
case when me.Value is null then null else dbo.htmlStripper(me.Value) end as Medications,
-- MS 18.3.17 Add AnticipatoryMedicationExists flag
case when (select count(*) from PatientMedications med where med.Patient = pd.#Patient and FrequencyDescription='As Needed') > 0 then 'Y' else 'N' end as AnticipatoryMedicationExists,
ag.Value as ALLERGIES_CDA,
case when ag.Value is null then null else dbo.htmlStripper(ag.Value) end as ALLERGIES,


-- Preferences
odc.Description as WISHES,
ppc.Description as PPC,
ppd1.Description as PPD1,
ppd2.description as PPD2,

-- Alerts
al.Value as ALERTS_CDA,
case when al.Value is null then null else dbo.htmlStripper(al.Value) end as ALERTS,

-- Symptoms
ltc.Description as CEILTREAT,
case when sy.Value is null then null else dbo.htmlStripper(sy.Value) end as #Symptoms,
-- Rest picked up from Symptoms pivot table
psy.*

from PatientDetail pd 
left join AllDataAddresses ad on ad.CMC_ID = pd.CMC_ID
left join AllDataAlerts al on al.CMC_ID = pd.CMC_ID
left join AllDataAliases aa on aa.CMC_ID = pd.CMC_ID
left join AllDataAllergies ag on ag.CMC_ID = pd.CMC_ID
left join AllDataContactInfo ci on ci.CMC_ID = pd.CMC_ID
left join AllDataContacts hs on hs.CMC_ID = pd.CMC_ID
left join AllDataDiagnoses di on di.CMC_ID = pd.CMC_ID
left join AllDataDisabilities dd on dd.CMC_ID = pd.CMC_ID
left join AllDataLPA lpa on lpa.CMC_ID = pd.CMC_ID
left join AllDataMedications me on me.CMC_ID = pd.CMC_ID
left join AllDataPersonalContacts per on per.CMC_ID = pd.CMC_ID
left join AllDataSymptoms sy on sy.CMC_ID = pd.CMC_ID
left join AllDataTemplateUUID uu on uu.CMC_ID = pd.CMC_ID
left join DoDRecorded dr on dr.Patient = pd.#Patient
-- Obtain approvals info from correct source MS 8.2.16
-- Change approvals sources for improved accuracy MS 15.1.17
left join AuditApprovals ap on ap.cmc_id = pd.cmc_id
left join Protocol.MetadataStaffEids mse on CONVERT(BIGINT,mse.cmc_id) = pd.cmc_id
left join Protocol.MetadataDeptEids mde on CONVERT(BIGINT,mde.cmc_id) = pd.cmc_id
left join StaffDeptContext cpri on cpri.ProviderOrgContext = #CPRBy
left join StaffDeptContext eb on eb.ProviderOrgContext = #LatestEnteredBy
-- Use AccurateEnteredBy info from audit log MS 20.2.16
--left join AccurateEnteredBy ae on ae.AuditId = #LatestEnteredBy
--left join StaffDeptContext eb1 on eb1.ProviderOrgContext = #OriginalEnteredBy
--left join StaffDeptContext gpi on gpi.ProviderOrgContext = #RegisteredGP
--left join StaffDeptContext pri on pri.ProviderOrgContext = #PrognosisBy
--left join StaffDeptContext rvi on rvi.ProviderOrgContext = #PlannedReviewer
--left join (select *,row_number() over (partition by DeptEnterpriseId order by Email) as rn from PDDeptEmails where Email like '%nhs.net') oe on oe.DeptEnterpriseId = eb1.DeptEnterpriseId and oe.rn=1
--left join (select *,row_number() over (partition by DeptEnterpriseId order by Email) as rn from PDDeptEmails where Email like '%nhs.net') le on le.DeptEnterpriseId = isnull(eb.DeptEnterpriseId,ae.DeptEnterpriseId) and le.rn=1
--left join (select *,row_number() over (partition by DeptEnterpriseId order by Email) as rn from PDDeptEmails where Email like '%nhs.net') gpe on gpe.DeptEnterpriseId = gpi.DeptEnterpriseId and gpe.rn=1
--left join (select *,row_number() over (partition by DeptEnterpriseId order by CombinedAddress) as rn from PDDeptAddresses) gpa on gpa.DeptEnterpriseId = gpi.DeptEnterpriseId and gpa.rn=1
--left join (select *,row_number() over (partition by DeptEnterpriseId order by Telephone) as rn from PDDeptPhones where left(Telephone,2) <> '07') gpp on gpp.DeptEnterpriseId = gpi.DeptEnterpriseId and gpp.rn=1
--left join (select *,row_number() over (partition by DeptEnterpriseId order by Telephone) as rn from PDDeptPhones where left(Telephone,2) = '07') gpm on gpm.DeptEnterpriseId = gpi.DeptEnterpriseId and gpm.rn=1
left join Symptoms psy on psy.#CMCId = pd.CMC_ID
left join CDA.PatientUUID pu on pu.cmc_id = pd.cmc_id
left join ETL_PROD.dbo.Coded_ADRTExists adc on adc.code = #ADRTExists
left join ETL_PROD.dbo.Coded_Anticoags acc on acc.code = #Anticoags
left join ETL_PROD.dbo.Coded_ClinicianAware cac on cac.code = #VALIDAD
left join ETL_PROD.dbo.Coded_ConsentType coc on coc.code = #Consent
left join ETL_PROD.dbo.Coded_CPRDecision cpc on cpc.code = #CARDIO_YN
left join ETL_PROD.dbo.Coded_DeathLocation dlc on dlc.code = #DODPLACE
left join ETL_PROD.dbo.Coded_DeathVariance dvc on dvc.code = #VARIANCE
left join ETL_PROD.dbo.Coded_DiagnosisCategory dic on dic.code = #Classified_Diagnosis
left join ETL_PROD.dbo.Coded_Diagnosis_DiagnosisCode ddc on ddc.code = #DiagnosisCode
left join ETL_PROD.dbo.Coded_DS1500 dsc on dsc.code = #DS1500
left join ETL_PROD.dbo.Coded_Equipment eqc on eqc.code = #EQUIP
left join ETL_PROD.dbo.Coded_EthnicGroup etc on etc.code = #Ethnicity
left join ETL_PROD.dbo.Coded_FamilyAwareDiagnosis fadc on fadc.code = #C_AWARE
left join ETL_PROD.dbo.Coded_FamilyAwarePrognosis fapc on fapc.code = #PA_FAMPROD
left join ETL_PROD.dbo.Coded_FamilyDiscussion fdc on fdc.code = #RESUS_FAMILY
left join ETL_PROD.dbo.Coded_FamilySupport fsuc on fsuc.code = #FAM_SUPPORT
left join ETL_PROD.dbo.Coded_Gender gc on gc.code = #Gender
left join ETL_PROD.dbo.Coded_HaveDisability hdc on hdc.code = #COMMDIFF
left join ETL_PROD.dbo.Coded_HomecareHelp hhc on hhc.code = #HOMECARE
left join ETL_PROD.dbo.Coded_Insulin inc on inc.code = #Insulin
left join ETL_PROD.dbo.Coded_LanguagePrimary plc on plc.code = #PrimaryLanguage
left join ETL_PROD.dbo.Coded_LevelOfTrtmnt ltc on ltc.code = #CEILTREAT
left join ETL_PROD.dbo.Coded_MaritalStatus msc on msc.code = #MaritalStatus
left join ETL_PROD.dbo.Coded_Opioids opc on opc.code = #OPIOID
left join ETL_PROD.dbo.Coded_OrganDonat odc on odc.code = #WISHES
left join ETL_PROD.dbo.Coded_PatientAwareDiagnosis padc on padc.code = #P_AWARE
left join ETL_PROD.dbo.Coded_PatientAwarePrognosis papc on papc.code = #PA_PROD
left join ETL_PROD.dbo.Coded_PatientDiscussion pdc on pdc.code = #RESUS_PATIENT
left join ETL_PROD.dbo.Coded_PatientReceipt prc on prc.code = #CAREPLAN
left join ETL_PROD.dbo.Coded_PreferPlace ppc on ppc.code = #PPC
left join ETL_PROD.dbo.Coded_PreferPlace ppd1 on ppd1.code = #PPD1
left join ETL_PROD.dbo.Coded_PreferPlace ppd2 on ppd2.code = #PPD2
left join ETL_PROD.dbo.Coded_PtAbleToDecide ptac on ptac.code = #HAVECAP
left join ETL_PROD.dbo.Coded_Religion rec on rec.code = #Religion
left join ETL_PROD.dbo.Coded_ReqCopy rqc on rqc.code = #Req_Copy
left join ETL_PROD.dbo.Coded_Steroids stc on stc.code = #Steroids
left join ETL_PROD.dbo.Coded_Surprise sc on sc.code = #Surprise
left join ETL_PROD.dbo.Coded_TimeFrameUnits tfc on tfc.code = #ALT_PROGNOSIS
left join ETL_PROD.dbo.Coded_WelfareAttorney wac on wac.code = #APPOINTWA
left join ETL_PROD.dbo.Coded_WHOPerf whc on whc.code = #WHPERF
left join (select distinct PatientSummary from PatientLPA) lp on lp.PatientSummary = pd.#PatientSummary
left join (select distinct PatientSummary from PatientPersonalContacts
           where Relationship = 'NOK' group by PatientSummary) nok on nok.PatientSummary = pd.#PatientSummary
left join (select cmc_id,classified_diagnosis from Protocol.PatientDemographics) demo on demo.CMC_ID = pd.CMC_ID

GO


