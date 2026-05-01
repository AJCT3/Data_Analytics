USE [ETL_Local_PROD]
GO
 
select

-- Identifiers
po.CMC_ID,
NHS_Number,
'Yes' as OnNewSystem,
lv.VersionNumber,
lv.GenusId,
fv.VersionNumber as FirstVersionNumber,
ps.itemid as #PatientSummary,
p.itemid as #Patient,
cp.ItemId as #Care_Plan,
convert(date,rc.runcomplete,103) as DataLoadDate,

-- Genuine Available Care Plan Check
-- 7 = Consent Withdrawn
case when c.Type = '7' then 'Y' else 'N' end as #IsSoftDeleted,

-- Metadata
cp.DateLastSaved,
cp1.EnteredBy as #OriginalEnteredBy,
-- Simplified version
isnull(cast(demo.Date_Original_Assessment as datetime),oa.Date_Original_Assessment) as Date_Original_Assessment,
isnull(cast(demo.Add_Date as datetime),au.ADD_DATE) as Add_Date,
cp1.LastApprovedBy as #OriginalApprovedBy,
'Completed' as OriginalAssessmentStatus,
-- Correct original approval timestamp MS 19.3.17
isnull(cast(demo.Date_original_Approval as datetime),
oap.Date_Original_Approval) as Date_Original_Approval,
isnull(la.AuditId,cp1.EnteredBy) as #LatestEnteredBy,
isnull(la.ActionTime,cast(demo.Date_Latest_Assessment as datetime)) as Date_Latest_Assessment,
cp.LastApprovedBy as #LatestApprovedBy,
'Completed' as LatestAssessmentStatus,
cp.LastApprovedTime as Date_Latest_Approval,

-- Demographics
-- Add title code MS 7.3.16
NamePrefix as #Title,
npc.Description as TITLE,
n.GivenName as FORENAME,
n.MiddleName,
-- deal with duff apostrophes MS 11.2.17
replace(n.FamilyName,'&apos;','''') as SURNAME,
n.CCPreferredName as PreferredName,
convert(varchar(25),cast(p.DateOfBirth as date),106) as DoB,
isnull(
  DateDiff(year,p.DateOfBirth,getdate()) -
    case
      when dbo.Date(year(getdate()),month(p.DateOfBirth),day(p.DateOfBirth)) <
        getdate() then 0
      else 1 end,-1) as Age,
p.Gender as #GENDER,
p.MaritalStatus as #MARITALSTATUS,
p.EthnicGroup as #ETHNICITY,
p.LivingCondDetails,
p.Religion as #RELIGION,
p.PrimaryLanguage as #PrimaryLanguage,
p.PrimaryLangDetails,
convert(varchar(25),cast(p.DateOfDeath as date),106) as DoD_Demographics,
p.DeathLocation as #DODPLACE,
p.DeathSourceInfo as INF_DEATH,
p.DeathLocationOther as OTHERPS,
p.DeathVariance as #VARIANCE,
p.DeathVarianceOther as OTHERPSA,
case p.IsProtected when 0 then 'N' else 'Y' end as #Restricted,
p.PDSOverride,
-- Currently no non-null values
case
  when pds.[fact of death] is not null and pds.[Fact of Death] = 'D' then
    CONVERT(VARCHAR(11), cast(pds.[date of death] as date), 106)
  else null end as DoD_PDS,
case [Fact of Death]
  when 'D' then 'Deceased'
  else 'Living' end as #DeceasedPDS,
pds.[Trace Result NHS Number] as PDS_NHS_Number,
case 
  when pds.[Record Type] is null then
    case
      when pn.NHS_Number is null then '03.Not present, not yet traced'
      else '02.Present, not yet traced' end 
  when pds.[Record Type] = '30' then '01.Present and verified'
  when pds.[Record Type] = '20' then '01.Missing but successfully identified via trace'
  when pds.[Record Type] = '33' then '01.Present, but replacement indicated by trace'
  when pds.[Record Type] = '40' then '01.Present, but replacement indicated by trace'
  else
    case
      when pn.NHS_Number is null then '04.Missing, trace attempted, no match or multiple matches found'
      else '04.Present, trace attempted, no match or multiple matches found' end
  end as PDS_Status,
-- See http://www.datadictionary.nhs.uk/data_dictionary/data_field_notes/n/nhs/nhs_number_status_indicator_code_de.asp?query=nha for details of codes 01,02,03,04. Our use of DBS (batch trace) prevents an accurate alignment
(select CONVERT(VARCHAR(11), [Latest Add Date Reconciled], 106) from PDSStatistics) as PDS_Reconciliation_Date,

pad.CombinedAddress as #MAIN_ADDRESS,
pad.PostalCode as #MAIN_POSTCODE,
pad.CombinedAddress as #PRIMARY_ADDRESS,
pad.PostalCode as #PRIMARY_POSTCODE,
cad.CombinedAddress as #CURRENT_ADDRESS,
cad.PostalCode as #CURRENT_POSTCODE,
sad.CombinedAddress as #SECONDARY_ADDRESS,
sad.PostalCode as #SECONDARY_POSTCODE,
hct.Home_Phone as #Home_Phone,
mct.Mobile_Phone as #Mobile_Phone,
wct.Work_Phone as #Work_Phone,
ect.Email as #Email,
oct.Other_Phone as #Other_Phone,

-- GP Information
gp.RegisteredGP as #RegisteredGP,
gp.CCG,
gp.CommissioningArea,
gp.London_CCG_ODS as #London_CCG_ODS,
-- Add more general CCG ODS code MS 7.3.16
gp.CCG_ODS,

-- Consent Information
c.Clinician as #ConsentedBy,
convert(varchar(25),cast(c.DateObtained as date),106) as ConsentedOn,
c.Type as #CONSENT,
c.Comments as MC_DET,
p.ReqCopy as #REQ_COPY,
cp.PlannedReviewer as #PlannedReviewer,
convert(varchar(25),cast(cp.PlannedReviewTime as date),106) as REVIEW,

-- Contacts
c.POADocLocation,

-- Medical Background
pr.Clinician as #PrognosisBy,
pr.FamilyAwareProgDetails as PA_FAMPRODDETAILS,
pr.FamilyAwarePrognosis as #PA_FAMPROD,
pr.PatientAwareProgDetails as PA_PRODDETAILS,
pr.PatientAwarePrognosis as #PA_PROD,
pr.TimeFrame,
pr.TimeFrameUnits as #ALT_PROGNOSIS,
pr.Surprise as #Surprise,
convert(varchar(25),cast(pr.UpdatedOn as date),106) as DATE_PROGNOSIS,
mb.ADRTDetails,
mb.ADRTExists as #ADRTExists,
mb.DisabilityDetails as COMM_DIFF_DETAIL,
mb.FamilyAwareDiagDetails as C_AWAREDETAILS,
mb.FamilyAwareDiagnosis as #C_AWARE,
mb.HaveDisability as #COMMDIFF,
mb.LevelOfTrtmnt as #CEILTREAT,
mb.LevelOfTrtmntDetails as CT_DET,
mb.OtherSignifHx as SIGNIFICANT_MEDICAL,
mb.PatientAwareDiagDetails as P_AWAREDETAILS,
mb.PatientAwareDiagnosis as #P_AWARE,
mb.WHOPerf as #WHPERF,
convert(varchar(25),cast(mb.WHOPerfTime as date),106) as WHP_DATE,
-- Handle primary and secondary cancers properly MS 2.5.16
dc.DiagnosisCategory as #Classified_Diagnosis,
dc.DiagnosisCode as #DiagnosisCode,

-- CPR
convert(varchar(25),cast(cpr.CPRReviewDate as date),106) as REVIEW_DATE,
cpr.ChildInvolv,
-- Currently no non-null values
cpr.ChildParentConsult,
-- Currently no non-null values
cpr.Clinician as #CPRBy,
cpr.ClinicianAware as #VALIDAD,
cpr.AdditionalDetail as POSITION,
convert(varchar(25),cast(cpr.ClinicianTime as date),106) as DNARDATE1,
cpr.CourtOrder,
-- Currently no non-null values
cpr.DNACPRFormLocation as ORDER_YES,
cpr.DNACPRFormUploaded,
-- Currently no non-null values
cpr.Decision as #CARDIO_YN,
convert(varchar(25),cast(cpr.DecisionTime as date),106) as CPRDECDATE,
cpr.FamilyDiscussion as #RESUS_FAMILY,
cpr.FamilyDiscussionComments as RESUS_FAMDET,
convert(varchar(25),cast(cpr.FamilyDiscussionTime as date),106) as FamilyDiscussionTime,
case cpr.HasBeenAgreed when 0 then 'N' else 'Y' end as HasBeenAgreed,
cpr.JudgeCourt,
-- Currently no non-null values
cpr.JudgeCourtLocation,
-- Currently no non-null values
cpr.JudgeCourtTime,
-- Currently no non-null values
cpr.OtherTeamMemb as NAMEMEM,
cpr.PatientDiscussion as #RESUS_PATIENT,
cpr.PatientDiscussionComments as RESUS_PATIENTDET,
convert(varchar(25),cast(cpr.PatientDiscussionTime as date),106) as PatientDiscussionTime,
cpr.PtAbleToDecide as #HAVECAP,
cpr.WelfareAttourney as #APPOINTWA,
cpr.WhyCPRInapp as CLINPROB,
ap1.ApproverName as DNARNAME1,
ap2.ApproverName as DNARNAME2,
ap3.ApproverName as DNARNAME3,
convert(varchar(25),cast(ap1.ApprovalTime as date),106) as DNARDATE2,
convert(varchar(25),cast(ap2.ApprovalTime as date),106) as DNARDATE3,
convert(varchar(25),cast(ap3.ApprovalTime as date),106) as DNARDATE4,
-- Are CPR approvers limited to 3 by the UI? Think they may be. Have assumed so.

-- Social Background
pkg.DS1500 as #DS1500,
pkg.Equipment as #EQUIP,
pkg.EquipmentNotes as EQUIP_DETAIL,
pkg.FamilySupport as #FAM_SUPPORT,
pkg.FamilySupportNotes as FAM_SUPPORT_Y,
pkg.HomecareHelp as #HOMECARE,
pkg.HomecareHelpNotes as HOMECARE_DET,
pkg.PatientReceipt as #CAREPLAN,
pkg.PatientReceiptNotes as CAREPLAN_DETAIL,

-- Medications
ms.Anticoags as #Anticoags,
ms.Insulin as #Insulin,
ms.MedListLocation,
ms.Opioids as #OPIOID,
ms.OtherInfo as MED_OTH,
ms.Steroids as #Steroids,

-- Preferences
pf.CulturalRelNeeds as CULTURAL,
pf.FamilyAwarePref as FAMILY_AWAR,
pf.OrganDonat as #WISHES,
pf.OrganDonatDet as WISHES_YES,
pf.PatientWishes as PERCARE_PLAN,
pf.PlaceCare as #PPC,
pf.PlaceCareDet as PPDDiscuss,
pf.PlaceCareDet,
-- unconfuse things
pf.PlaceDeath1 as #PPD1,
pf.PlaceDeath1Det as PPCDiscuss,
-- unconfuse things
pf.PlaceDeath1Det,
pf.PlaceDeath2 as #PPD2,
pf.PlaceDeath2Det

from LatestVersion lv
join FirstVersion fv on fv.GenusID = lv.GenusID
join ETL_PROD.dbo.CMC_CarePlan cp on lv.CarePlan = cp.ItemId
join ETL_PROD.dbo.CMC_CarePlan cp1 on fv.CarePlan = cp1.ItemId
join ETL_PROD.dbo.CMC_PatientSummary ps on lv.PatientSummary = ps.ItemID
join ETL_PROD.dbo.CMC_Patient p on ps.Patient = p.ItemId
join ETL_PROD.dbo.CMC_Name n on p.Name = n.itemid
join CMCIDs po on po.Patient = ps.ItemID
join NHSNumbers pn on pn.Patient = ps.ItemID
left join PatientRegisteredGP gp on gp.patientsummary = ps.ItemId
left join ETL_PROD.dbo.CMC_CarePackage pkg on pkg.ItemId = ps.CarePackage
left join ETL_PROD.dbo.CMC_Consent c on c.ItemId = p.Consent
left join ETL_PROD.dbo.CMC_CPR cpr on ps.CPR = cpr.ItemId
left join ETL_PROD.dbo.CMC_MedicalBackground mb on ps.MedicalBackground = mb.ItemId
left join ETL_PROD.dbo.CMC_MedicationSummary ms on ms.ItemId = ps.MedicationSummary
left join ETL_PROD.dbo.CMC_PatientSummary_PatientPreferences psf on ps.ItemId = psf.PatientSummary
left join ETL_PROD.dbo.CMC_PatientPreference pf on psf.PatientPreference = pf.ItemId
left join ETL_PROD.dbo.CMC_Prognosis pr on ps.Prognosis = pr.ItemId
left join (select * from PatientCPRApprovers where rn=1) ap1 on cpr.ItemId = ap1.CPR
left join (select * from PatientCPRApprovers where rn=2) ap2 on cpr.ItemId = ap2.CPR
left join (select * from PatientCPRApprovers where rn=3) ap3 on cpr.ItemId = ap3.CPR
-- Are there always only three CPR approvers??
-- Correct address handling MS 20.2.16
left join (select Patient,CombinedAddress,PostalCode,ROW_NUMBER() over (partition by Patient order by CCFromTime desc) as rn from PatientAddresses where CCAddressUse = 'MAIN'
and (CCToTime is null or CAST(getdate() as date) < CAST(cctotime as date))
and (CCFromTime is null or CAST(getdate() as date) >= CAST(ccfromtime as date))) pad on ps.Patient = pad.Patient and pad.rn=1
left join (select Patient,CombinedAddress,PostalCode,ROW_NUMBER() over (partition by Patient order by CCFromTime desc) as rn from PatientAddresses where CCAddressUse = 'SECO'
and (CCToTime is null or CAST(getdate() as date) < CAST(cctotime as date))
and (CCFromTime is null or CAST(getdate() as date) >= CAST(ccfromtime as date))) sad on ps.Patient = sad.Patient and sad.rn=1
left join (select Patient,CombinedAddress,PostalCode,ROW_NUMBER() over (partition by Patient order by CCFromTime desc) as rn from PatientAddresses where CCAddressUse in ('CURR','TEMP')
and (CCToTime is null or CAST(getdate() as date) < CAST(cctotime as date))
and (CCFromTime is null or CAST(getdate() as date) >= CAST(ccfromtime as date))) cad on ps.Patient = cad.Patient and cad.rn=1
left join (select Patient,ContactValue as Home_Phone,ROW_NUMBER() over (partition by Patient order by ContactNo) as rn from PatientContactInfo where ContactType = 'HOME') hct on ps.Patient = hct.Patient and hct.rn=1
left join (select Patient,ContactValue as Mobile_Phone,ROW_NUMBER() over (partition by Patient order by ContactNo) as rn from PatientContactInfo where ContactType = 'MOBILE') mct on ps.Patient = mct.Patient and mct.rn=1
left join (select Patient,ContactValue as Work_Phone,ROW_NUMBER() over (partition by Patient order by ContactNo) as rn from PatientContactInfo where ContactType = 'WORK') wct on ps.Patient = wct.Patient and wct.rn=1
left join (select Patient,ContactValue as Email,ROW_NUMBER() over (partition by Patient order by ContactNo) as rn from PatientContactInfo where ContactType = 'EMAIL') ect on ps.Patient = ect.Patient and ect.rn=1
left join (select Patient,ContactValue as Other_Phone,ROW_NUMBER() over (partition by Patient order by ContactNo) as rn from PatientContactInfo where ContactType = 'OTHER') oct on ps.Patient = oct.Patient and oct.rn=1
left join ETL_PROD.dbo.Coded_NamePrefix npc on npc.code = n.NamePrefix
-- Correct diagnosis join MS 21.2.16
-- Correct it properly MS 18.3.17
left join (select CMC_ID,Patient,DiagnosisCategory,DiagnosisCode,
ROW_NUMBER() over (partition by Patient order by case when MainDiagnosis=1 then 1 else 0 end desc,case when DiagnosisCategoryDescription<>'Migrated' then diagnosisitemid else 'Q' end) as rn
from PatientDiagnoses) dc on dc.Patient = po.Patient and dc.rn=1
-- Pick up correct CMC_Run_Complete line MS 18.3.17
left join (select top 1 * from ETL_PROD.dbo.CMC_RUN_COMPLETE where ItemId=1) rc on 1=1

left join (select CMC_ID,ActionTime as Add_Date,ROW_NUMBER() over (PARTITION by cmc_id order by actiontime) as rn from AuditPatient where ActionType = 'create') au on au.cmc_id = po.CMC_ID and au.rn=1
-- allow for pts marked duplicated on 23.11.15
left join (
select cmc_id,add_date,Date_Original_Assessment,Date_Latest_Assessment,original_approval_date as date_original_approval from Protocol.OldSystemCarePlans
union all
select cmc_id,add_date,Date_Original_Assessment,Date_Latest_Assessment,original_approval_date as date_original_approval from Protocol.OldSystemCarePlansMigratedDuplicates
) demo on demo.CMC_ID = po.CMC_ID
-- Work around mysterious EnteredOn values MS 10.2.16
left join (select CMC_ID,ActionTime as Date_Original_Assessment,ROW_NUMBER() over (PARTITION by cmc_id order by actiontime) as rn from AuditPatient where ActionType = 'create') oa on oa.cmc_id = po.CMC_ID and oa.rn=1
-- Work around incorrect LastApprovedTime values MS 19.3.17
left join (select CMC_ID,ActionTime as Date_Original_Approval,ROW_NUMBER() over (PARTITION by cmc_id order by actiontime) as rn from AuditPatient apa where ActionType = 'Publish') oap on oap.cmc_id = po.CMC_ID and oap.rn=1
-- Correct latest assessment handling MS 20.2.16
left join AccurateEnteredBy la on la.patientsummary = ps.ItemId
left join Load.PDS on pds.[Local PID] = po.CMC_ID









GO


