USE [ETL_Local_PROD]
GO
/****** Object:  StoredProcedure [dbo].[AT__MetadataDeptEids_SP]    Script Date: 01/06/2020 09:18:14 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




alter PROCEDURE [dbo].[AT__Cache-PatientDetailFull_AllPublishedVersions] 
-- Amended for PD Upgrade
AS
BEGIN

 
 IF OBJECT_ID('[ETL_Local_PROD].[dbo].[PatientDetailFull-AllPublishedVersions]') IS NOT NULL 
 drop table [ETL_Local_PROD].[dbo].[PatientDetailFull-AllPublishedVersions]
-- Columns with names starting # are not shown on the Comparison Report
select

-- Already derived
pd.*,

-- Genuine Available Care Plan Check
case
  when gpi.DeptName like '%TEST DOCTORS%'
  or pd.cmc_id in (1791,15079)
  or pd.NHS_Number in ('9990248753','9990248761','9990248796','9990248958',
                       '9990248966','9990249040','9990249059','9990249172',
                       '9990249199','9990402132','9990252947','9990252955',
                       '9990243271','9990281025')
  then 'Y' else 'N' end as #TestPatient,

-- Metadata
rtrim(isnull(isnull(eb.StaffTitleDescription,ae.StaffTitleDescription) + ' ','') +
  isnull(isnull(eb.StaffForename,ae.StaffForename) + ' ','') +
  isnull(isnull(eb.StaffSurname,ae.StaffSurname),'')) as Assessor,
isnull(eb.StaffProviderTypeDescription,ae.StaffProviderTypeDescription) as Job_Title,
isnull(eb.DeptName,ae.DeptName) as Workbase,
isnull(eb.DeptODSCode,ae.DeptODSCode) as Workbase_ODS,
isnull(eb.LocalCMCOrgTypeDescription,ae.LocalCMCOrgTypeDescription) as Prof_Group,
isnull(eb.DeptEnterpriseID,ae.DeptEnterpriseId) as WorkbaseEId,
le.Email as WorkbaseEmail,
-- correct Approver handling MS 2.4.17
ISNULL(ap.Approver,mse.LatestApprover) as Approver,
isnull(ap.ApproverJobTitle,mse.LatestApproverJobTitle) as ApproverJobTitle,
ISNULL(ap.ApproverWorkbase,mde.LatestApproverWorkbase) as ApproverWorkbase,
ISNULL(ap.ApproverWorkbaseODS,mde.LatestApproverWorkbaseODS) as ApproverWorkbaseODS,
ISNULL(ap.ApproverProfGroup,mde.LatestApproverProfGroup) as Approver_Prof_Group,
ISNULL(ap.ApproverWorkbaseEid,mde.LatestApproverWorkbaseEid) as ApproverWorkbaseEId,

-- Demographics
gc.Description as Gender,
msc.Description as Marital_Status,
etc.Description as Ethnicity,
rec.Description as Religion,
plc.Description as PrimaryLanguage,
-- Ignore 'Not Known' place of death - treat as not recorded MS 2.5.16
rtrim(case dlc.Description when 'Not Known' then NULL else dlc.Description end) as DODPLACE,
rtrim(dvc.Description) as Variance,
dr.DoD_Recorded,
dbo.FirstName(Forename) as #First_Forename,
case when ad.Value is null then null else dbo.htmlStripper(ad.Value) end as ADDRESSES,
case when ci.Value is null then null else dbo.htmlStripper(ci.Value) end as CONTACT_INFO,
case when aa.Value is null then null else dbo.htmlStripper(aa.Value) end as ALIASES,

-- GP Information
rtrim(isnull(gpi.StaffForename + ' ','') + isnull(gpi.StaffSurname,'')) as GP,
gpi.StaffTitleDescription as GP_Title,
gpi.DeptName as Surgery,
gpi.DeptODSCode as Practice_ODS,
gpe.Email as PracticeEmail,
gpa.CombinedAddress as Practice_Address,
gpa.PostalCode as Practice_Postcode,
gpi.DeptEnterpriseId as PracticeEnterpriseId,

-- Consent Information
rqc.Description as Req_Copy,
coc.Description as Consent,
rtrim(isnull(rvi.StaffTitleDescription + ' ','') + isnull(rvi.StaffForename + ' ','') + isnull(rvi.StaffSurname,'')) as PlannedReviewer,
rvi.StaffProviderTypeDescription as PlannedReviewerJobTitle,
rvi.DeptName as PlannedReviewerWorkbase,
rvi.DeptODSCode as PlannedReviewerWorkbaseODS,
rvi.LocalCMCOrgTypeDescription as PlannedReviewerProfGroup,

-- Contacts
case when hs.Value is null then null else dbo.htmlStripper(hs.Value) end as HSCContacts,
case when per.Value is null then null else dbo.htmlStripper(per.Value) end as PersonalContacts,
case when lpa.Value is null then null else dbo.htmlStripper(lpa.Value) end as LPA,
case when nok.PatientSummary is null then 'N' else 'Y' end as NoKExists, 
case when lp.PatientSummary is null then 'N' else 'Y' end as PoAExists, 

-- Medical Background
rtrim(isnull(pri.StaffTitleDescription + ' ','') + isnull(pri.StaffForename + ' ','') + isnull(pri.StaffSurname,'')) as PrognosisClinician,
pri.DeptName as PrognosisClinicianWorkbase,
fapc.Description as PA_FAMPROD,
papc.Description as PA_PROD,
tfc.Description as ALT_PROGNOSIS,
sc.Description as Surprise,
adc.Description as ADRTExists,
fadc.Description as C_AWARE,
hdc.Description as COMMDIFF,
padc.Description as P_AWARE,
whc.Description as WHPERF,
dbo.htmlStripper(di.Value) as Diagnoses,
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
-- remove CPRDiscussion* as duplicates MS 19.10.16

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
case when me.Value is null then null else dbo.htmlStripper(me.Value) end as Medications,
case when ag.Value is null then null else dbo.htmlStripper(ag.Value) end as ALLERGIES,

-- Preferences
odc.Description as WISHES,
ppc.Description as PPC,
ppd1.Description as PPD1,
ppd2.description as PPD2,

-- Alerts
case when al.Value is null then null else dbo.htmlStripper(al.Value) end as ALERTS,

-- Symptoms
ltc.Description as CEILTREAT,
case when sy.Value is null then null else dbo.htmlStripper(sy.Value) end as #Symptoms,
-- Rest picked up from Symptoms pivot table
psy.*


into [ETL_Local_PROD].[dbo].[PatientDetailFull-AllPublishedVersions]

from [PatientDetail-AllPublishedVersions] pd 
left join [AllDataAddresses-APV] ad on ad.Patient = pd.#Patient
left join [AllDataAlerts-APV] al on al.Patient = pd.#Patient
left join [AllDataAliases-APV] aa on aa.Patient = pd.#Patient
left join [AllDataAllergies-APV] ag on ag.Patient = pd.#Patient
left join [AllDataContactInfo-APV] ci on ci.Patient = pd.#Patient
left join [AllDataContacts-APV] hs on hs.Patient = pd.#Patient
left join [AllDataDiagnoses-APV] di on di.Patient = pd.#Patient
left join [AllDataDisabilities-APV] dd on dd.Patient = pd.#Patient
left join [AllDataLPA-APV] lpa on lpa.Patient = pd.#Patient
left join [AllDataMedications-APV] me on me.Patient = pd.#Patient
left join [AllDataPersonalContacts-APV] per on per.Patient = pd.#Patient
left join [AllDataSymptoms-APV] sy on sy.CarePlan = pd.#Care_Plan
left join DoDRecorded dr on dr.Patient = pd.#Patient
-- Correct Approved By handling MS 2.4.17
left join [AuditApprovals-AllPublishedVersions] ap on ap.#patient = pd.#patient
left join Protocol.MetadataStaffEids mse on mse.cmc_id = pd.cmc_id
left join Protocol.MetadataDeptEids mde on mde.cmc_id = pd.cmc_id
left join StaffDeptContext cpri on cpri.ProviderOrgContext = #CPRBy
left join StaffDeptContext eb on eb.ProviderOrgContext = #EnteredBy
left join AccurateEnteredBy ae on ae.AuditId = #EnteredBy
left join StaffDeptContext gpi on gpi.ProviderOrgContext = #RegisteredGP
left join StaffDeptContext pri on pri.ProviderOrgContext = #PrognosisBy
left join StaffDeptContext rvi on rvi.ProviderOrgContext = #PlannedReviewer
left join (select *,row_number() over (partition by DeptEnterpriseId order by Email) as rn from PDDeptEmails where Email like '%nhs.net') le on le.DeptEnterpriseId = isnull(eb.DeptEnterpriseId,ae.DeptEnterpriseId) and le.rn=1
left join (select *,row_number() over (partition by DeptEnterpriseId order by Email) as rn from PDDeptEmails where Email like '%nhs.net') gpe on gpe.DeptEnterpriseId = gpi.DeptEnterpriseId and gpe.rn=1
left join (select *,row_number() over (partition by DeptEnterpriseId order by CombinedAddress) as rn from PDDeptAddresses) gpa on gpa.DeptEnterpriseId = gpi.DeptEnterpriseId and gpa.rn=1
left join [Symptoms-APV] psy on psy.#SymptomCarePlan = pd.#Care_Plan
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


END


