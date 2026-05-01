USE [ETL_Local_PROD]
GO

/****** Object:  View [dbo].[Cache-PatientDetailHistoric]    Script Date: 14/10/2019 09:11:48 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






ALTER view [dbo].[Cache-PatientDetailHistoric] as
-- Amended for Portal Initiate (SELF symptom)
-- Denormalise properly MS 11.3.17
select
top 5
-- Fill gaps and add CDA and GP information MS 14.2.16

-- Identifiers
-- MS 3.7.16 bigint
cast(demo.CMC_ID as bigint) as CMC_ID,
NHS_Number,
'No' as OnNewSystem,
0 as VersionNumber,
-- MS 2.7.16 bigint
cast(demo.CMC_ID as bigint) as GenusId,
0 as FirstVersionNumber,
'' as #PatientSummary,
'' as #Patient,
'' as #Care_Plan,
CAST('2015-11-25' as DATE) as DataLoadDate,

-- Genuine Available Care Plan Check
-- 7 = Consent Withdrawn
'N' as #IsSoftDeleted,

-- Metadata
DateLastSaved,
'' as #OriginalEnteredBy,
Date_Original_Assessment,
Add_Date,
'' as #OriginalApprovedBy,
OriginalAssessmentStatus,
Original_Approval_Date as Date_Original_Approval,
'' as #LatestEnteredBy,
Date_Latest_Assessment,
'' as #LatestApprovedBy,
LatestAssessmentStatus,
Latest_Approval_Date as Date_Latest_Approval,

-- Demographics
-- 'Null' title code MS 7.3.16
'' as #TITLE,
TITLE,
FORENAME,
'' as MiddleName,
SURNAME,
PreferredName as PreferredName,
convert(varchar(25),cast(DoB as date),106) as DoB,
na.Age,
'' as #GENDER,
'' as #MARITALSTATUS,
'' as #ETHNICITY,
LivingCondDetails,
'' as #RELIGION,
'' as #PrimaryLanguage,
PrimaryLangDetails,
convert(varchar(25),cast(DoD_Demographics as date),106) as DoD_Demographics,
'' as #DODPLACE,
INF_DEATH,
OTHERPS,
'' as #VARIANCE,
OTHERPSA,
#Restricted,
'' as PDSOverride,
-- pick up recent reconciliations MS 6.4.16
isnull(pds.DoD_PDS,demo.dod_pds) as DOD_PDS,
isnull(pds.DeceasedPDS,demo.DeceasedPDS) as #DeceasedPDS,
isnull(pds.PDS_NHS_Number,demo.pds_nhs_number) as PDS_NHS_Number,
isnull(pds.PDS_Status,demo.PDS_Status) as PDS_Status,
isnull(pds.PDS_Reconciliation_Date,demo.PDS_Reconciliation_Date) as PDS_Reconciliation_Date,
MAIN_ADDRESS as #MAIN_ADDRESS,
MAIN_POSTCODE as #MAIN_POSTCODE,
PRIMARY_ADDRESS as #PRIMARY_ADDRESS,
PRIMARY_POSTCODE as #PRIMARY_POSTCODE,
CURRENT_ADDRESS as #CURRENT_ADDRESS,
CURRENT_POSTCODE as #CURRENT_POSTCODE,
SECONDARY_ADDRESS as #SECONDARY_ADDRESS,
SECONDARY_POSTCODE as #SECONDARY_POSTCODE,
Home_Phone as #Home_Phone,
Mobile_Phone as #Mobile_Phone,
Work_Phone as #Work_Phone,
Email as #Email,
#Other_Phone,

-- GP Information
'' as #RegisteredGP,
pp.CCG,
pp.CommissioningArea,
'' as #London_CCG_ODS,
-- 'null' CCG ODS code MS 7.3.16
'' as CCG_ODS,

-- Consent Information
'' as #ConsentedBy,
ConsentedOn,
'' as #CONSENT,
MC_DET,
'' as #REQ_COPY,
'' as #PlannedReviewer,
convert(varchar(25),cast(REVIEW as date),106) as REVIEW,

-- Contacts
'' as POADocLocation,

-- Medical Background
'' as #PrognosisBy,
'' as PA_FAMPRODDETAILS,
'' as #PA_FAMPROD,
'' as PA_PRODDETAILS,
'' as #PA_PROD,
'' as TimeFrame,
'' as #ALT_PROGNOSIS,
'' as #Surprise,
convert(varchar(25),cast(DATE_PROGNOSIS as date),106) as DATE_PROGNOSIS,
'' as ADRTDetails,
'' as #ADRTExists,
COMMDIFF_Y as COMM_DIFF_DETAIL,
C_AWAREDETAILS,
'' as #C_AWARE,
'' as #COMMDIFF,
'' as #CEILTREAT,
CT_DET,
SIGNIFICANT_MEDICAL,
P_AWAREDETAILS,
'' as #P_AWARE,
'' as #WHPERF,
convert(varchar(25),cast(WHP_DATE as date),106) as WHP_DATE,
'' as #Classified_Diagnosis,
'' as #DiagnosisCode,

-- CPR
convert(varchar(25),cast(RevDate as date),106) as REVIEW_DATE,
'' as ChildInvolv,
'' as ChildParentConsult,
'' as #CPRBy,
'' as #VALIDAD,
POSITION,
convert(varchar(25),cast(DNARDATE1 as date),106) as DNARDATE1,
'' as CourtOrder,
ORDER_YES,
'' as DNACPRFormUploaded,
'' as #CARDIO_YN,
convert(varchar(25),cast(CPRDECDATE as date),106) as CPRDECDATE,
'' as #RESUS_FAMILY,
RESUS_FAMDET,
'' as FamilyDiscussionTime,
'' as HasBeenAgreed,
'' as JudgeCourt,
'' as JudgeCourtLocation,
'' as JudgeCourtTime,
-- Currently no non-null values
NAMEMEM,
'' as #RESUS_PATIENT,
RESUS_PATIENTDET,
'' as PatientDiscussionTime,
'' as #HAVECAP,
'' as #APPOINTWA,
SumMainClinicalP as CLINPROB,
DNARNAME1,
DNARNAME2,
DNARNAME3,
convert(varchar(25),cast(DNARDATE2 as date),106) as DNARDATE2,
convert(varchar(25),cast(DNARDATE3 as date),106) as DNARDATE3,
convert(varchar(25),cast(DNARDATE4 as date),106) as DNARDATE4,

-- Social Background
'' as #DS1500,
'' as #EQUIP,
EQUIP_Y as EQUIP_DETAIL,
'' as #FAM_SUPPORT,
FAM_SUPPORT_Y,
'' as #HOMECARE,
EQUIP_Y as HOMECARE_DET,
'' as #CAREPLAN,
CAREPLAN_Y as CAREPLAN_DETAIL,

-- Medications
'' as #Anticoags,
'' as #Insulin,
'' as MedListLocation,
'' as #OPIOID,
MED_OTH,
'' as #Steroids,

-- Preferences
CULTURAL,
FAMILY_AWAR,
'' as #WISHES,
WISHES_YES,
PERCARE_PLAN,
'' as #PPC,
PPDDiscuss,
'' as PlaceCareDet,
'' as #PPD1,
PPCDiscuss,
'' as PlaceDeath1Det,
'' as #PPD2,
'' as PlaceDeath2Det,

-- Identifiers
[UUID_CDA],
[TEMPLATE_UUID_CDA],

-- Genuine Available Care Plan Check
'N' as #TestPatient,

-- Metadata
OriginalAssessor as Original_Assessor,
OriginalAssessorJobTitle as Original_Job_Title,
OriginalAssessorWorkbase as Original_Workbase,
OriginalAssessorWorkbaseODS as Original_Workbase_ODS,
OriginalAssessorProfGroup as Original_Prof_Group,
OriginalAssessorWorkbaseEID as OriginalWorkbaseEId,
OriginalAssessorWorkbaseEmail as OriginalWorkbaseEmail,
OriginalApprover,
OriginalApproverJobTitle,
OriginalApproverWorkbase,
OriginalApproverWorkbaseODS,
OriginalApproverProfGroup as Original_Approver_Prof_Group,
OriginalApproverWorkbaseEId,
-- MS 18.3.17 Add original approver ODS code & role information
OriginalApproverODS,
(select role from reference.RolesCDA r where r.job_title = OriginalApproverJobTitle)
  as Original_Approver_Role,
OriginalApproverJobTitle as Original_Approver_Role_Description,

LatestAssessor as Latest_Assessor,
LatestAssessorJobTitle as Latest_Job_Title,
-- Add Latest Assessor Role / Description MS 18.2.16
r.role as Latest_Assessor_Role,
LatestAssessorJobTitle as Latest_Assessor_Role_Description,
LatestAssessorWorkbase as Latest_Workbase,
LatestAssessorWorkbaseODS as Latest_Workbase_ODS,
LatestAssessorProfGroup as Latest_Prof_Group,
LatestAssessorWorkbaseEID as LatestWorkbaseEId,
LatestAssessorWorkbaseEMail as LatestWorkbaseEmail,
LatestApprover,
LatestApproverJobTitle,
LatestApproverWorkbase,
LatestApproverWorkbaseODS,
LatestApproverProfGroup as Latest_Approver_Prof_Group,
LatestApproverWorkbaseEId,
-- MS 18.3.17 Add latest approver ODS code & role information
LatestApproverODS,
(select role from reference.RolesCDA r where r.job_title = LatestApproverJobTitle)
  as Latest_Approver_Role,
LatestApproverJobTitle as Latest_Approver_Role_Description,

-- Demographics
case Gender when 'F' then 'Female' when 'M' then 'Male' else Gender end as Gender,
case
  when na.Age<18 then '0-17'
  when na.Age>=18 and na.Age<40 then '18-39'
  when na.Age>=40 and na.Age<50 then '40-49'
  when na.Age>=50 and na.Age<60 then '50-59'
  when na.Age>=60 and na.Age<70 then '60-69'
  when na.Age>=70 and na.Age<80 then '70-79'
  else '80+'
  end as #AgeBand,
Marital_Status,
demo.Ethnicity,
Religion,
PrimaryLanguage,
case DODPLACE
  when '1.Home' then 'Home' 
  when '2.Care Home/Nursing' then 'Care Home/Nursing'
  when '2.Care Home/Residential' then 'Care Home/Residential' 
  when '3.Hospice' then 'Hospice' 
  when '4.Community Hospital' then 'Community Hospital'
  when '4.Hospital' then 'Hospital'
  when '5.Other' then 'Other' 
  when '6.Not recorded' then NULL
  else DODPLACE end as DODPLACE,
-- Tidy up Variance picklist MS 8.2.16
case Variance
  when '1.Patient changed mind' then 'Patient Changed Mind'
  when '2.Carer changed mind' then 'Carer Changed Mind'
  when '3.Care package breakdown' then 'Care Package Breakdown'
  when '4.Hospice not available' then 'Hospice Not Available'
  when '5.Sudden deterioration/death' then 'Sudden Deterioration / Death'
  when '6.Other' then 'Other'
  when '7.Not recorded' then NULL
  else rtrim(Variance)
  end as Variance,
DoD_Recorded,
DeceasedCMC as #DeceasedCMC,
DaysOnSystem_CMC as #DaysOnSystem_CMC,
isnull(pds.DaysOnSystem_PDS,demo.DaysOnSystem_PDS) as #DaysOnSystem_PDS,
First_Forename as #First_Forename,
ADDRESSES_CDA,
case when ADDRESSES_CDA is null then null else dbo.htmlStripper(ADDRESSES_CDA) end as ADDRESSES,
CONTACT_INFO_CDA,
case when CONTACT_INFO_CDA is null then null else dbo.htmlStripper(CONTACT_INFO_CDA) end as CONTACT_INFO,
ALIASES_CDA,
case when ALIASES_CDA is null then null else dbo.htmlStripper(ALIASES_CDA) end as ALIASES,

-- GP Information
isnull(ISNULL(StaffForename+' ','') + staffsurname,GP) as GP,
isnull(gps.StaffTitleDescription,GP_TITLE) as GP_Title,
isnull(gpd.DeptName,Surgery) as Surgery,
isnull(gpd.DeptODSCode,Practice_ODS) as Practice_ODS,
isnull(gpd.WorkbaseEmail,PracticeEmail) as PracticeEmail,
isnull(gpa.CombinedAddress,Practice_Address) as Practice_Address,
isnull(gpa.PostalCode,Practice_Postcode) as Practice_Postcode,
-- Add surgery dept enterprise id MS 23.2.16
gpd.DeptEnterpriseID as PracticeEnterpriseId,
ISNULL(
'<table>
    <tbody>
        <tr>
            <th>GP Title</th>
            <th>GP ODS</th>
            <th>Surgery</th>
            <th>Practice ODS</th>
            <th>Practice Postcode</th>
            <th>CCG</th>
            <th>Practice Phone</th>
            <th>Practice Mobile</th>
        </tr>
        <tr>
            <td>' + ISNULL(isnull(gps.StaffTitleDescription, GP_TITLE),'(N/A)') + '</td>
            <td>' + ISNULL(gps.StaffODSCode,'(N/A)') + '</td>
            <td>' + ISNULL(isnull(gpd.DeptName,Surgery),'(N/A)') + '</td>
            <td>' + ISNULL(isnull(gpd.DeptODSCode,Practice_ODS),'(N/A)') + '</td>
            <td>' + ISNULL(isnull(gpa.PostalCode,Practice_Postcode),'(N/A)') + '</td>
            <td>' + ISNULL(isnull(pp.CCG,PCT),'(N/A)') + '</td>
            <td>' + ISNULL(isnull(gpp.Telephone,PracticePhone),'(N/A)') + '</td>
            <td>' + ISNULL(isnull(gpm.Telephone,PracticeMobile),'(N/A)') + '</td>
        </tr>
    </tbody>
</table>'
,'') as GP_CDA,

-- Consent Information
Req_Copy,
case consent
  when '1.Parent/legal guardian' then 'The patient is aged 17 years or younger, and their parent/legal guardian has agreed to the creation of a personalised care plan and the sharing of information as above'
  when '1.Patient 16-17' then 'The patient is aged 16 or 17 years and has given their own consent'
  when '1.Patient consents' then 'The patient is an adult (18+) and has agreed to the creation of a personalised care plan and sharing of information as above'
  when '2.Best interest applies' then 'The patient is an adult (18+) but lacks mental capacity to make the above decisions. A clinical decision has been made in their best interest in consultation with the family/carers, if possible'
  when '2.Best interest under 18' then 'The patient is aged 17 years or younger. It has not been possible to obtain consent of a parent/legal guardian, but a clinical decision has been made in their best interest'
  when '3.Power of Attorney' then 'The patient is an adult (18+) but lacks mental capacity to make the above decisions. Consent has been given by an appointed person with Lasting Power of Attorney for health and welfare'
  when '4.LSL-Consent Field Not Migrated' then 'LSL-Consent Field Not Migrated' 
  when '4.Yes (unspecified)' then 'Yes (unspecified)'
  when '5.Refused' then 'Refused'
  when '5.Withdrawn' then 'Withdrawn'
  when '6.Not recorded' then NULL
else consent end as Consent,
'' as PlannedReviewer,
'' as PlannedReviewerJobTitle,
'' as PlannedReviewerWorkbase,
'' as PlannedReviewerWorkbaseODS,
'' as PlannedReviewerProfGroup,

-- Contacts
hs.Value as HSCContacts_CDA,
case when hs.Value is null then null else dbo.htmlStripper(hs.Value) end as HSCContacts,
prs.Value as PersonalContacts_CDA,
case when prs.Value is null then null else dbo.htmlStripper(prs.Value) end as PersonalContacts,
lp.Value as LPA_CDA,
case when lp.Value is null then null else dbo.htmlStripper(lp.Value) end as LPA,
NoKExists, 
PoAExists, 

-- Medical Background
Prognosis_Whom as #Prognosis_Whom,
'' as PrognosisClinician,
'' as PrognosisClinicianWorkbase,
-- add Prognosis Clinician Workbase ODS MS 18.3.17
'' as PrognosisClinicianWorkbaseODS,
PA_FAMPROD,
PA_PROD,
case when PROGNOSIS_ALT = 'Not recorded' then NULL else PROGNOSIS_ALT end as ALT_PROGNOSIS,
'' as Surprise,
'' as ADRTExists,
C_AWARE,
COMMDIFF,
P_AWARE,
case WHPERF
  when '1.0 - fully active and more or less as you were before your illness' then '0. Fully Active And More Or Less As You Were Before Illness'
  when '2.1 - cannot carry out heavy physical work, but can do anything else' then '1. Cannot Carry Out Heavy Physical Work, But Can Do Anything Else'
  when '3.2 - up and about more than half the day; you can look after yourself' then '2. Up And About More Than Half A Day, You Can Also Look After Yourself, But Are Not Well Enough To Work'
  when '4.3 - in bed or sitting in a chair for more than half the day you need some help in looking after yourself' then '3. In Bed Or Sitting In A Chair For More Than Half The Day. Will Need Some Help In Looking After Yourself'
  when '5.4 - in bed or a chair all the time and need a lot of looking after' then '4. In Bed Or Chair Most Of The Time And Needs A Lot Of Looking After'
  when '6.5 - patient has died' then 'Dead'
  when '6.Not recorded' then NULL
  else WHPERF end as WHPERF,
Diagnoses_CDA,
Diagnoses,
Classified_Diagnosis,
dbo.DiagnosisCategory(Classified_Diagnosis) as DiagnosisCategory,
DISDET_CDA,
case when DISDET_CDA is null then NULL else dbo.HTMLStripper(DISDET_CDA) end as DISDET,

-- CPR
case CARDIO_YN
  when '1.Yes' then 'Yes'
  when '2.No' then 'No'
  when '3.Decision not yet made' then 'Decision Not Yet Made'
  when '4.Not recorded' then NULL
  else CARDIO_YN end as CARDIO_YN,
-- match picklist MS 18.3.17
case rtrim(resus_family)
when '1.Yes' then 'Yes'
when 'Not Yet Discussed With Family /Carer' then 'No - not yet discussed'
when '3.Not yet discussed with family/carer' then 'No - not yet discussed'
when 'No' then 'No - not yet discussed'
when '2.No' then 'No - not yet discussed'
when 'Family/Carer Not Wishing To Have Discussion' then 'No - not willing to discuss'
when '4.Family/carer not wishing to have discussion' then 'No - not willing to discuss'
when '7.Not recorded' then NULL
when '6.No family/carer to discuss with' then 'No Family /Carer To Discuss With'
when '5.Patient not wishing family/carer to be involved in the discussion' then 'Patient Not Wishing Family /Carer To Be Involved In The Discussion'
else rtrim(resus_family) end as RESUS_FAMILY,
-- match picklist MS 18.3.17
case rtrim(resus_patient)
when '1.Yes' then 'Yes'
when 'Patient not able to discuss' then 'No - not able to discuss'
when '5.Patient not able to discuss' then 'No - not able to discuss'
when 'Not yet discussed with patient' then 'No - not yet discussed'
when '3.Not yet discussed with patient' then 'No - not yet discussed'
when 'No' then 'No - not yet discussed'
when '2.No' then 'No - not yet discussed'
when 'Patient not wishing to have discussion' then 'No - not willing to discuss'
when '4.Patient not wishing to have discussion' then 'No - not willing to discuss'
when '6.Not recorded' then NULL
else rtrim(resus_patient) end as RESUS_PATIENT,
APPOINTWA,
VALIDAD,
HAVECAP,
DNARNAME,
'' as CPRClinicianWorkbase,
-- MS 18.3.17 Add clinician workbase ODS code
'' as CPRClinicianWorkbaseODS,
-- Add CPR Discussion flags MS 7.3.16 - answer not known for care plans not on new system
-- Remove them again MS 19.10.16 - they are RESUS_FAMILY and RESUS_PATIENT!
-- Social Background
-- match picklist 18.3.17
case DS1500
when 'N' then 'No'
when 'Y' then 'Yes'
when 'DN' then 'Don''t Know'
else DS1500 end as DS1500,
EQUIP,
-- match picklist 18.3.17
case FAM_SUPPORT
when 'N' then 'No'
when 'Y' then 'Yes'
when 'DN' then 'Don''t Know'
else FAM_SUPPORT end as FAM_SUPPORT,
-- match picklist 18.3.17
case HOMECARE
when '3.No' then 'No'
when '2.Yes - family provided' then 'Yes - Family Provided'
when '1.Yes - provided' then 'Yes - Provided'
when '4.Not recorded' then NULL
else HOMECARE end as HOMECARE,
-- match picklist 18.3.17
case CAREPLAN
when '3.No' then 'No'
when '2.Yes - continuing care funded' then 'Yes - NHS funded'
when '4.Don''t know' then 'Don''t Know'
when '1.Yes - social services funded' then 'Yes - social services funded'
when '5.Not recorded' then NULL
else CAREPLAN end as CAREPLAN,

-- Medications
'' as Anticoags,
'' as Insulin,
OPIOID,
'' as Steroids,
case
  when currentmedications_cda is null and jicmedications_cda is null then NULL
  else isnull(currentmedications_cda+'<p>','') + isnull(jicmedications_cda+'<p>','') end as medications_cda,
case
  when currentmedications_cda is null and jicmedications_cda is null then NULL
  else dbo.htmlStripper(isnull(currentmedications_cda+'<p>','') + isnull(jicmedications_cda+'<p>','')) end as medications,
-- MS 18.3.17 Add AnticipatoryMedicationExists flag
case when jicmedications_cda is null then 'N' else 'Y' end as AnticipatoryMedicationExists,
ALLERGIES_CDA,
case when ALLERGIES_CDA is null then null else dbo.htmlStripper(ALLERGIES_CDA) end as ALLERGIES,

-- Preferences
-- match picklist 18.3.17
case WISHES
when 'N' then 'No'
when 'Y' then 'Yes'
when 'DN' then 'Don''t Know'
else WISHES end as WISHES,
case PPC
  when '1.Home' then 'Home'
  when '2.Care Home' then 'Care Home'
  when '3.Hospice' then 'Hospice' 
  when '4.Community Hospital' then 'Community Hospital' 
  when '4.Hospital' then 'Hospital' 
  when '5.Other' then 'Other'
  when '6.Not yet discussed' then 'Not Yet Discussed' 
  when '7.No wish to state' then 'Patient Not Wishing to State Preference' 
  when '7.Not able to discuss' then 'Patient Not Able to Discuss'
  when '8.Not recorded' then NULL
  else PPC end as PPC,
case PPD1
  when '1.Home' then 'Home'
  when '2.Care Home' then 'Care Home'
  when '3.Hospice' then 'Hospice' 
  when '4.Community Hospital' then 'Community Hospital' 
  when '4.Hospital' then 'Hospital' 
  when '5.Other' then 'Other'
  when '6.Not yet discussed' then 'Not Yet Discussed' 
  when '7.No wish to state' then 'Patient Not Wishing to State Preference' 
  when '7.Not able to discuss' then 'Patient Not Able to Discuss'
  when '8.Not recorded' then NULL
  else PPD1 end as PPD1,  
case PPD2
  when '1.Home' then 'Home'
  when '2.Care Home' then 'Care Home'
  when '3.Hospice' then 'Hospice' 
  when '4.Community Hospital' then 'Community Hospital' 
  when '4.Hospital' then 'Hospital' 
  when '5.Other' then 'Other'
  when '6.Not yet discussed' then 'Not Yet Discussed' 
  when '7.No wish to state' then 'Patient Not Wishing to State Preference' 
  when '7.Not able to discuss' then 'Patient Not Able to Discuss'
  when '8.Not recorded' then NULL
  else PPD2 end as PPD2,  

-- Alerts
ALERTS_CDA,
case when ALERTS_CDA is null then null else dbo.htmlStripper(ALERTS_CDA) end as ALERTS,

-- Symptoms
-- MS 17.10.16 normalise Ceiling of Treatment
case CEILTREAT
when '1.Full, active treatment, including CPR' then 'Full active treatment including CPR' 
when '2.Full, active treatment, including in acute hospital setting' then 'Full active treatment including in acute hospital setting, but not CPR'
when '3.Treatment of any reversible conditions, including acute hospital setting if needed, but not for any ventilation or CPR' then 'Treatment of any reversible conditions (including acute hospital setting if needed) but not for any ventilation or CPR'
when '4.Treatment of any reversible conditions but only in the home/hospice setting – keep comfortable' then 'Treatment of any reversible conditions but only in the home/hospice setting: keep comfortable' 
when '5.Symptomatic treatment only – keep comfortable' then 'Symptomatic treatment only: keep comfortable'
when '6.Other' then 'Other'
when '7.Not recorded' then NULL
else CEILTREAT end as CEILTREAT,
'' as #Symptoms,
'' as #CMCId,
'' as [ANXIETY],
'' as [ANXIETY_DET],
[BREATHLESSNESS] as [BREATH],
BREATHL_DET as [BREATH_DET],
[BREATHLESSNESS],
BREATHL_DET as [BREATHLESSNESS_DET],
'' as [CARDIAC],
'' as [CARDIAC_DET],
'' as [DELIRIUM],
'' as [DELIRIUM_DET],
GRAD_DET as [GRADDET],
[GRADDET_DET],
'' as GRAD_DET,
'' as GRAD_DET_DET,
MAJHAEM as [HAEM],
MAJHAEM_DET as [HAEM_DET],
INFECTIONS as [INFECT],
INFECTIONS_DET as [INFECT_DET],
'' as INFECTIONS,
'' as INFECTIONS_DET,
[MAJHAEM],
[MAJHAEM_DET],
'' as [MEDPROB],
'' as [MEDPROB_DET],
'' as [MENTALEMER],
'' as [MENTALEMER_DET],
'' as [MND],
'' as [MND_DET],
[NAUSVOM],
[NAUSVOM_ET] as NAUSVOM_DET,
[OTH1],
[OTH1_DET],
PAINCRISES as [PAIN],
PAINCRISES_DET as [PAIN_DET],
[PAINCRISES],
[PAINCRISES_DET],
RESPIRATORY as [RESP],
[RESP_DET],
'' as RESPIRATORY,
'' as RESPIRATORY_DET,
RESTLESSNESS as [REST],
RESTLESSNESS_DET as [REST_DET],
[RESTLESSNESS],
[RESTLESSNESS_DET],
SEIZURES as [SEIZE],
SEIZURES_DET as [SEIZE_DET],
[SEIZURES],
[SEIZURES_DET],
--New in 141001 MS 18.11.16
'' as [SELF],
'' as [SELF_DET],
SUDDEN_DET as [SUDDENDET],
SUDDEN_DET_DET as [SUDDENDET_DET],
'' as SUDDEN_DET,
'' as SUDDEN_DET_DET,
NAUSVOM as [VOM],
NAUSVOM_ET as [VOM_DET],
'' as [WORSLIVER],
'' as [WORSLIVER_DET],
'' as [WORSMOBIL],
'' as [WORSMOBIL_DET],
'' as [WORSORAL],
'' as [WORSORAL_DET],
'' as [WORSRENAL],
'' as [WORSRENAL_DET],
'' as [#dummy#]
from Protocol.OldSystemCarePlans demo
left join Protocol.CurrentAnswersPivot ca on demo.cmc_id = ca.cmc_id
left join (select cmc_id,isnull(
  DateDiff(year,DoB,GETDATE()) -
    case
      when dbo.Date(year(GETDATE()),month(DoB),day(DoB)) <
        GETDATE() then 0
      else 1 end,-1) as age from Protocol.OldSystemCarePlans) na on demo.cmc_id = na.cmc_id 
left join Protocol.PatientPractice pp on demo.CMC_ID = pp.cmc_id
left join Protocol.MetadataStaffEids ms on demo.CMC_ID = ms.cmc_id
left join Protocol.Metadatadepteids md on demo.CMC_ID = md.cmc_id
left join reference.RolesCDA r on r.job_title = LatestAssessorJobTitle
left join AllDataContactsHistoric hs on hs.CMC_ID = demo.CMC_ID
left join AllDataLPAHistoric lp on lp.CMC_ID = demo.CMC_ID
left join AllDataPersonalHistoric prs on prs.CMC_ID = demo.CMC_ID
-- Pick up subsequent PDS deaths where relevant care plans reconciled MS 6.4.16
left join PDSHistoric pds on pds.cmc_id = demo.CMC_ID
left join (select *,ROW_NUMBER() over (PARTITION by deptlocalcmcid order by deptenterpriseid) as rn from WorkbaseDQInfo) gpd on cast(PracticeExternalID as varchar(max)) = gpd.DeptLocalCMCId and gpd.rn=1
left join (select *,ROW_NUMBER() over (PARTITION by stafflocalcmcid order by staffenterpriseid) as rn from AssessorDQInfo) gps on GPStaffId = gps.StaffLocalCMCId and gps.rn=1
left join (select *, ROW_NUMBER() over (PARTITION by deptenterpriseid order by postalcode) as rn from PDDeptAddresses) gpa on gpa.DeptEnterpriseId = gpd.DeptEnterpriseID and gpa.rn=1
left join (select *, ROW_NUMBER() over (PARTITION by deptenterpriseid order by telephone) as rn from PDDeptPhones p where p.TelephoneType = 'Business Phone') gpp on gpp.DeptEnterpriseId = gpd.DeptEnterpriseID and gpp.rn=1
left join (select *, ROW_NUMBER() over (PARTITION by deptenterpriseid order by telephone) as rn from PDDeptPhones p where p.TelephoneType = 'Mobile') gpm on gpm.DeptEnterpriseId = gpd.DeptEnterpriseID and gpm.rn=1
where demo.CMC_ID not in (select pd.cmc_id from PatientDetail pd)
-- correct handling of NHS number comparisons
and (isnull(isnull(pds.PDS_NHS_Number,demo.pds_nhs_number),NHS_Number) is null
or isnull(isnull(pds.PDS_NHS_Number,demo.pds_nhs_number),NHS_Number) not in (select pd.NHS_Number from PatientDetail pd))





GO


