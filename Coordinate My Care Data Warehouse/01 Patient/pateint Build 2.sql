USE [ETL_Local_PROD]
GO
 

  --select top 5* from [ETL_PROD].[dbo].[CMC_PatientSummary_CarePlan] where  patientSummary in(	'PS||100015671||3','PS||100015671||4')

   if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_CMCID]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_CMCID]
  select 
Patient, 
cast(n.PatientNumber as bigint) as CMC_ID 
into [ETL_Local_PROD].[dbo].[AT_CMCID]
from ETL_PROD.dbo.CMC_Patient_PatientNumbers pn
join ETL_PROD.dbo.CMC_PatientNumber n on n.ItemId = pn.PatientNumber
where AssigningAuthority = 'CMC'

 



if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_NHSNumbers]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_NHSNumbers]
select 
Patient, 
n.PatientNumber as NHS_Number 
into [ETL_Local_PROD].[dbo].[AT_NHSNumbers]
from ETL_PROD.dbo.CMC_Patient_PatientNumbers pn
join ETL_PROD.dbo.CMC_PatientNumber n on n.ItemId = pn.PatientNumber
where AssigningAuthority = 'NHS'







	if OBJECT_ID ('Tempdb..#PatTele') is not null
	drop table #PatTele
		SELECT
		*
		INTO #PatTele
		FROM
		(
		  SELECT
		no.PatientNumber as CMC_ID,
		ps.Patient,
		ContactType,ContactValue,
		ROW_NUMBER() over (partition by no.PatientNumber,ContactType order by pa.PATIENT DESC) as ContactNo,
		ct.Description as ContactTypeDescription
		from ETL_PROD.dbo.CMC_PatientSummary ps
		join ETL_PROD.dbo.CMC_Patient p on ps.Patient = p.ItemId
		join ETL_PROD.dbo.CMC_Patient_PatientNumbers po on po.Patient = ps.ItemID
		join ETL_PROD.dbo.CMC_PatientNumber no on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
		join ETL_PROD.dbo.CMC_Patient_ContactInfo pa on p.itemid = pa.Patient
		join ETL_PROD.dbo.CMC_ContactInfo a on a.ItemID = pa.ContactInfo 
		left join ETL_PROD.dbo.Coded_ContactType ct on a.ContactType = ct.code

		--where no.PatientNumber = 100013863
		)D WHERE ContactNo = 1

		--select * from #PatTele  where cmc_id = 100007061
		--select * from [ETL_Local_PROD].[dbo].[AT_Patient_General] where cmc_id = 100007061
		--select * from (select cmc_id,Count(*) as Total from [ETL_Local_PROD].[dbo].[AT_Patient_General] group by CMC_ID )d where d.Total >1 



--Multiple lines per patient  this is eliminated by the current CMC record (derived from #Versions (each cmc care record for each pateint has a different patient number (ps.Patient)
	if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_PatAddress]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_PatAddress]

	SELECT
	Address,
	Null as Main_LastAddressflag,
	Null as Second_LastAddressflag,
	Null as Crr_Temp_LastAddressflag,
	no.PatientNumber as CMC_ID,
	ROW_NUMBER() over (partition by no.PatientNumber order by ps.Patient) as PatientNumber,
 
	ps.Patient,
	nm.GivenName + ' ' + Nm.FamilyName as PatName,
	a.StreetLine, 
	a.Line2, 
	a.City, 
	a.County, 
	a.PostalCode,
	a.CCAddressUse, 
	a.CCDwellingType,
	a.CCFromTime, 
	a.CCToTime,
	a.CCLivingConditions,
	a.CCKeySafeDetails, 
	a.CCResidenceNotes,
	ISNULL(a.StreetLine,'') +
	  case 
		when a.StreetLine is not null and rtrim(a.StreetLine) <> '' and a.Line2 is not null and rtrim(a.Line2) <> ''then ', ' 
			else '' 
			end + ISNULL(a.Line2,'') + 
		case 
			when a.City is not null and rtrim(a.City) <> '' then ', ' 
			else '' end +ISNULL(a.City,'') + 
		case 
			when a.County is not null and rtrim(a.County) <> '' then ', ' 
			else '' end +ISNULL(a.County,'') 
	as CombinedAddress,
 
	ROW_NUMBER() over (partition by ps.Patient order by Address,a.CCFromTime) as AddressNo,
	au.Description as CCAddressUseDescription,
	dt.Description as CCDwellingTypeDescription,
	-- MS 19.2.16 add living conditions description
	lc.Description as CCLivingConditionsDescription

	into [ETL_Local_PROD].[dbo].[AT_PatAddress]

	from ETL_PROD.dbo.CMC_PatientSummary ps
	join ETL_PROD.dbo.CMC_Patient p on ps.Patient = p.ItemId
	join ETL_PROD.dbo.CMC_Patient_PatientNumbers po on po.Patient = ps.ItemID
	join ETL_PROD.dbo.CMC_PatientNumber no on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
	join ETL_PROD.dbo.CMC_Patient_Addresses pa on p.itemid = pa.Patient
	join ETL_PROD.dbo.CMC_Address a on pa.Address = a.ItemID
	left join ETL_PROD.dbo.Coded_AddressUse au on a.CCAddressUse = au.code
	left join ETL_PROD.dbo.Coded_DwellingType dt on a.CCDwellingType = dt.code
	left join ETL_PROD.dbo.Coded_LivingConditions lc on a.CCLivingConditions = lc.code
	left join [ETL_PROD].[dbo].[CMC_Name]Nm on nm.ItemId = p.ItemId
	--WHERE no.PatientNumber IN (
	--							100015671, --This returns two rows for the same person but it seems one is generates for each care plan
	--							100015678
	--							)
 
	 ORDER BY no.PatientNumber, a.StreetLine



	 --select top 500 * from ETL_PROD.dbo.CMC_Patient where name in ( 'PS||100007994||1','PS||100007994||3')






	 update r
			set r.Main_LastAddressflag = case  when s.LastAddress is not null then 1 else null end,
				r.Second_LastAddressflag = case  when t.LastAddress is not null then 1 else null end,
				r.Crr_Temp_LastAddressflag = case  when u.LastAddress is not null then 1 else null end

	 from [ETL_Local_PROD].[dbo].[AT_PatAddress] r
	 left join 
				(
				select
				CMC_ID,
				max(PatientNumber) as LastAddress

				from [ETL_Local_PROD].[dbo].[AT_PatAddress]
				where CCAddressUse = 'MAIN'
			
				group by CMC_ID
				)s on s.CMC_ID = r.CMC_ID 
				and s.LastAddress = r.PatientNumber
	 left join 
				(
				select
				CMC_ID,
				max(PatientNumber) as LastAddress

				from [ETL_Local_PROD].[dbo].[AT_PatAddress]
				where CCAddressUse = 'SECO'
				group by CMC_ID
				)t on t.CMC_ID = r.CMC_ID 
				and t.LastAddress = r.PatientNumber
	 left join 
				(
				select
				CMC_ID,
				max(PatientNumber) as LastAddress

				from [ETL_Local_PROD].[dbo].[AT_PatAddress]
				where CCAddressUse in ('CURR','TEMP')
				group by CMC_ID
				)u on u.CMC_ID = r.CMC_ID 
				and u.LastAddress = r.PatientNumber




	 --select * from #PatAddress where Patient = 'PS||100011841||4'
	--select *  from [ETL_Local_PROD].[dbo].[AT_PatAddress] where cmc_id = 100007994
	--			where 
	--			--CCAddressUse = 'MAIN'
	--			--and 
	--			cmc_id = 100007994

 





   if OBJECT_ID ('Tempdb..#PatLPA') is not null
 drop table #PatLPA

 SELECT
no.PatientNumber as CMC_ID,PatientSummary,
Name,FromTime,Comments,
SupportContact,
ROW_NUMBER() over (partition by patientsummary order by SupportContact) as LPANo

into #PatLPA

from ETL_PROD.dbo.CMC_PatientSummary ps
join ETL_PROD.dbo.CMC_Patient_PatientNumbers po on po.Patient = ps.ItemID
join ETL_PROD.dbo.CMC_PatientSummary_SupportContacts psc on ps.ItemId = psc.PatientSummary
join ETL_PROD.dbo.CMC_SupportContact sc on psc.SupportContact = sc.itemid
join ETL_PROD.dbo.CMC_PatientNumber no on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
where SupportContactType = 'POA'

--select * from #PatLPA where cmc_ID = 100013863

--  if OBJECT_ID ('Tempdb..#PatTele') is not null
-- drop table #PatTele
-- SELECT
--CMC_ID, p.SupportContact, LPANo,
--i.ContactType, i.ContactValue,
--ROW_NUMBER() over (partition by cmc_id,LPANo order by ContactInfo) as ContactInfoNo

--into #PatTele

--from #PatLPA p
--join ETL_PROD.dbo.CMC_SupportContact_ContactInfo sci on p.SupportContact = sci.SupportContact
--join ETL_PROD.dbo.CMC_ContactInfo i on sci.ContactInfo = i.ItemID
 --select * from #PatLPA
 --select * from #PatTele
  --select * from #PatTele where cmc_ID = 100013863
  --100013863




 


--SELECT * FROM ETL_PROD.dbo.CMC_ContactInfo WHERE ItemID =   100013863

--SELECT 
--* 
--FROM ETL_PROD.dbo.CMC_Patient P
--LEFT JOIN ETL_PROD.dbo.CMC_Patient_ContactInfo pa ON p.itemid = pa.Patient
--join ETL_PROD.dbo.CMC_ContactInfo a on a.ItemID = pa.ContactInfo 



    if OBJECT_ID ('Tempdb..#PDDept') is not null
	drop table #PDDept
	 select 
	  * 
	 into #PDDept
	 from [ETL_Local_PROD].[dbo].[AT_Dept] where DeptSource = 'PD'


--#PDDept


 
if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_AddressGP]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_AddressGP]
select 
case when d.AddressOrder = 1 then 1 else null end as CurrentAddressFlag,
d.*
into [ETL_Local_PROD].[dbo].[AT_AddressGP]
from
(
SELECT 
distinct 
convert(date,a.creationDateTime) as CreationDate,
convert(date,ds.StartDate) as StartDate,
convert(date,ds.EndDate) as EndDate,
row_number() over (partition by DeptEnterpriseId order by coalesce(a.creationDateTime,convert(date,ds.StartDate))desc) as AddressOrder,
DeptEnterpriseId,
a.StreetLine, 
a.Line2, 
a.City, 
a.County, 
a.PostalCode,
ISNULL(a.StreetLine,'') +
  case when a.StreetLine is not null and rtrim(a.StreetLine) <> '' 
       and a.Line2 is not null and rtrim(a.Line2) <> ''
       then ', ' else '' end + 
ISNULL(a.Line2,'') + case when a.City is not null and rtrim(a.City) <> '' then ', ' else '' end +
ISNULL(a.City,'') + case when a.County is not null and rtrim(a.County) <> '' then ', ' else '' end +
ISNULL(a.County,'') as CombinedAddress

FROM [ETL_PROD].[dbo].[CMC_Location] lo join #PDDept d on lo.OrganizationEID = deptenterpriseid
join ETL_PROD.dbo.CMC_Address a on lo.AddressEID = a.PDEnterpriseID
left join etl_PROD.dbo.CMC_Location_DateSpan lods on lo.ItemId = lods.Location
left join etl_PROD.dbo.CMC_DateSpan ds on lods.DateSpan = ds.ItemId
-- ensure ind->org info is omitted from this view MS 6.4.16
where lo.IndividualEID is null
-- exclude deleted and expired/unstarted locations MS 25.8.16
and lo.Deleted is null
and (ds.StartDate is null or CAST(startdate as date) <= CAST(getdate() as DATE))
and (ds.EndDate is null or  CAST(enddate as date) > CAST(getdate() as DATE))

 
) d 



if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_Emails]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_Emails]
select
 distinct
 
 case 
 when em.Address like '%nhs.net' then row_number() over (partition by DeptEnterpriseId order by Email) else null end as EmailOrder , 
 DeptEnterpriseId,
 em.Address as Email,
 convert(date,ds.StartDate) as StartDate,
 convert(date,ds.EndDate) as EndDate

 into [ETL_Local_PROD].[dbo].[AT_Emails]

  FROM [ETL_PROD].[dbo].[CMC_Location] lo join PDDepartment d on lo.OrganizationEID = deptenterpriseid
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



 if OBJECT_ID ('Tempdb..#TelephoneGP') is not null
 drop table #TelephoneGP

SELECT 
distinct 
case when left(isnull(ph.FullNumber,ph.TelephoneNumber) ,2) <> '07' then row_number() over (partition by DeptEnterpriseId order by case when tt.Description = 'Business Phone' then 1 else left(isnull(ph.FullNumber,ph.TelephoneNumber),5) end )else null end as No_07_PhoneOrder,
case when left(isnull(ph.FullNumber,ph.TelephoneNumber) ,2) = '07' then row_number() over (partition by DeptEnterpriseId order by case when tt.Description = 'Business Phone' then 1 else left(isnull(ph.FullNumber,ph.TelephoneNumber),5) end )else null end as Yes_07_PhoneOrder,
DeptEnterpriseId, 
isnull(ph.FullNumber,ph.TelephoneNumber) as Telephone, 
tt.Description as TelephoneType,
convert(date, creationDateTime) as CreationDate,
convert(date,ds.StartDate) as StartDate,
convert(date,ds.EndDate) as EndDate

into #TelephoneGP

  FROM [ETL_PROD].[dbo].[CMC_Location] lo join PDDepartment d on lo.OrganizationEID = deptenterpriseid
-- change CMC_Location_Contacts/CMC_ContactInfo_Phones to CMC_Location_Phones  
  join ETL_PROD.dbo.CMC_Location_Phones cp on cp.Location = Lo.ItemId
  join ETL_PROD.dbo.CMC_Telecom ph on cp.Phone = ph.ItemId
left join ETL_PROD.dbo.CMC_Location_DateSpan lods on lo.ItemId = lods.Location
left join ETL_PROD.dbo.CMC_DateSpan ds on lods.DateSpan = ds.ItemId
left join etl_prod.dbo.Coded_TelecomType tt on tt.Code = ph.TypeCodedValue
-- ensure ind->org info is omitted from this view MS 6.4.16
where lo.IndividualEID is null
-- exclude deleted and expired/unstarted locations MS 25.8.16
and lo.Deleted is null
and (ds.StartDate is null or CAST(ds.startdate as date) <= CAST(getdate() as DATE))
and (ds.EndDate is null or  CAST(ds.enddate as date) > CAST(getdate() as DATE))

order by DeptEnterpriseId


--A dynamic column table for the symptoms

 if OBJECT_ID ('Tempdb..#Sample') is not null
 drop table  #Sample

CREATE TABLE #Sample
(
    CMC_ID varchar(max)
    , CarePlan varchar(36)
    , Symptom varchar(255)
    , SymptomNo  Int
	,SymptomDescription varchar(max)
);

DECLARE @SQL VARCHAR(MAX) = 'SELECT CMC_ID';

INSERT INTO #Sample  
SELECT
no.PatientNumber as CMC_ID,
pscp.CarePlan,
sp.Symptom,
--sp.NoteText,
convert(int,ROW_NUMBER() over (partition by pscp.CarePlan order by sp.Symptom)) as SymptomNo,
sc.Description as SymptomDescription
from ETL_PROD.dbo.CMC_PatientSummary ps
join ETL_PROD.dbo.CMC_Patient p on ps.Patient = p.ItemId
join ETL_PROD.dbo.CMC_Patient_PatientNumbers po on po.Patient = ps.ItemID
join ETL_PROD.dbo.CMC_PatientNumber no on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
join ETL_PROD.dbo.CMC_PatientSummary_CarePlan pscp on pscp.PatientSummary = ps.ItemId
join ETL_PROD.dbo.CMC_CarePlan_SymptomPlans cps on pscp.CarePlan = cps.CarePlan
join ETL_PROD.dbo.CMC_SymptomPlan sp on cps.SymptomPlan = sp.ItemId
left join ETL_PROD.dbo.Coded_Symptom sc on sp.Symptom = sc.code
where sp.NoteText is not null
--and  no.PatientNumber = '100022416'

;
if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_Symptoms]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_Symptoms]
SELECT @SQL += '
    , MAX(CASE WHEN SymptomNo = ' + convert(varchar,SymptomNo) + ' THEN Symptom END) AS [SymptomNo_' + convert(varchar,SymptomNo) + ']
    , MAX(CASE WHEN SymptomNo = ' + convert(varchar,SymptomNo) + ' THEN SymptomDescription END) AS [SymptomDescription_' + convert(varchar,SymptomNo) + ']'
FROM (SELECT DISTINCT  SymptomNo  AS SymptomNo FROM #Sample) AS T
ORDER BY   T.SymptomNo  ;

SET @SQL += ' into [ETL_Local_PROD].[dbo].[AT_Symptoms] FROM #Sample GROUP BY CMC_ID;';


EXECUTE (@SQL);

DROP TABLE #Sample;

--select * from ##Symptoms

--drop table ##Symptoms

if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_PatDiagnosis]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_PatDiagnosis]
 
	SELECT
	no.PatientNumber as CMC_ID,
	ps.Patient,
	DiagnosisCode,
	MainDiagnosis,
	OnsetTime,
	ToTime,
	FamilyAware,
	PatientAware,
	Comments,
	DiagnosisCategory,
	ROW_NUMBER() over (partition by patientsummary order by Diagnosis) as DiagnosisNo,
	ROW_NUMBER() over (partition by ps.Patient order by case when MainDiagnosis =1 then 1 else 0 end desc,case when dcc.Description <>'Migrated' then dia.ItemId else 'Q' end) as PatDiagRN,--For Final patient Table
	-- Add descriptions MS 18.2.16
	dic.Description as DiagnosisDescription,
	dcc.Description as DiagnosisCategoryDescription,
	-- Add diagnosis item id MS 12.9.16
	dia.ItemId as DiagnosisItemId 

	into [ETL_Local_PROD].[dbo].[AT_PatDiagnosis]

	from ETL_PROD.dbo.CMC_PatientSummary ps
	join ETL_PROD.dbo.CMC_Patient_PatientNumbers po on po.Patient = ps.ItemID
	join ETL_PROD.dbo.CMC_PatientSummary_Diagnoses pdia on ps.ItemId = pdia.PatientSummary
	join ETL_PROD.dbo.CMC_Diagnosis dia on pdia.Diagnosis = dia.ItemId
	join ETL_PROD.dbo.CMC_PatientNumber no on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
	join ETL_PROD.dbo.Coded_Diagnosis_DiagnosisCode dic on dia.DiagnosisCode = dic.Code
	join ETL_PROD.dbo.Coded_DiagnosisCategory dcc on dia.DiagnosisCategory = dcc.Code


---Patient alerts also has multiple rowsa - one for every cmc record created by patient
 if OBJECT_ID ('Tempdb..#PatAlert') is not null
 drop table #PatAlert
SELECT
no.PatientNumber as CMC_ID,
ps.Patient,
a.Alert,AlertType,FromTime,ToTime,Comments,
ROW_NUMBER() over (partition by patientsummary order by pa.Alert) as AlertNo,
ac.Description as AlertDescription,
at.Description as AlertTypeDescription

into #PatAlert

from ETL_PROD.dbo.CMC_PatientSummary ps
join ETL_PROD.dbo.CMC_Patient_PatientNumbers po on po.Patient = ps.ItemID
join ETL_PROD.dbo.CMC_PatientSummary_Alerts pa on ps.ItemId = pa.PatientSummary
join ETL_PROD.dbo.CMC_Alert a on pa.Alert = a.ItemId
join ETL_PROD.dbo.CMC_PatientNumber no on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
left join etl_PROD.dbo.Coded_AlertCode ac on a.alert = ac.code
left join etl_PROD.dbo.Coded_AlertType at on a.alerttype = at.code




 if OBJECT_ID ('Tempdb..#PatAlias') is not null
	drop table #PatAlias

	SELECT 
	sel1.*,
	ROW_NUMBER() over (partition by Patient order by Surname,Forename,MiddleName,Title) as AliasNo
	into #PatAlias
	from 
	(
		select 
		distinct
		no.PatientNumber as CMC_ID,
		ps.Patient,
		a.NamePrefix as Title,
		np.Description as TitleDescription,
		a.GivenName as Forename,
		a.MiddleName,
		a.FamilyName as Surname
		from ETL_PROD.dbo.CMC_PatientSummary ps
		join ETL_PROD.dbo.CMC_Patient p on ps.Patient = p.ItemId
		join ETL_PROD.dbo.CMC_Patient_PatientNumbers po on po.Patient = ps.ItemID
		join ETL_PROD.dbo.CMC_PatientNumber no on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
		join ETL_PROD.dbo.CMC_Patient_Aliases pa on p.itemid = pa.Patient
		join ETL_PROD.dbo.CMC_Name a on pa.Alias = a.ItemID
		left join ETL_PROD.dbo.Coded_NamePrefix np on a.NamePrefix = np.Code
	) sel1

	--select * from #PatAlias
	 
  if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_StaffDeptContext]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_StaffDeptContext]
 
select 
poc.ItemId as ProviderOrgContext, 
poc.Organization as POC_Org,
s.*, 
d.*

 into [ETL_Local_PROD].[dbo].[AT_StaffDeptContext]

from ETL_PROD.dbo.CMC_ProviderOrgContext poc
left join ETL_PROD.dbo.CMC_IndividualProvider ip on poc.Provider = ip.ItemId
left join ETL_PROD.dbo.CMC_Individual i on i.PDRegistryID = ip.RegistryID
left join [ETL_Local_PROD].[dbo].[AT_Staff] s on s.Individual = i.ItemID
left join [ETL_Local_PROD].[dbo].[AT_Dept]  d on d.Organization = poc.Organization  
-- Exclude LastClinicalApprover rows introduced in 15.1 release 
where ip.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
AND ip.ItemId not like 'PS|%|%|%|%|%|LCA'
AND poc.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
AND poc.ItemId not like 'PS|%|%|%|%|%|LCA'

 
 










   if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_Accurate_Entered_by]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_Accurate_Entered_by]
	SELECT [CMC_ID]
      ,PatAuditID as AuditId
      ,ToPatientSummary as [PatientSummary]
      ,[ActionTime]
      ,[ActionType]
       ,cast(null as Varchar(max)) as [DeptName]
      ,cast(null as Varchar(255)) as [OrganizationType]
      ,cast(null as Varchar(max)) as [OrganizationTypeDescription]
      ,cast(null as Varchar(36)) as [Organization]
      ,cast(null as Varchar(25)) as [DeptSource]
      ,null as [DeptEnterpriseID]
      ,[DeptPDRegistryID]
      ,cast(null as Varchar(255)) as [DeptLocalCMCId]
      ,cast(null as Varchar(255)) as [LocalCMCOrgType]
      ,cast(null as Varchar(max)) as [LocalCMCOrgTypeDescription]
      ,cast(null as Varchar(255)) as [DeptODSCode]
      ,cast(null as date) as [DeptOpenDate]
      ,cast(null as date) as [DeptCloseDate]
      ,null as [StaffEnterpriseID]
      ,cast(null as Varchar(36)) as [Individual]
      ,cast(null as Varchar(max)) as [StaffTitle]
      ,cast(null as Varchar(max)) as [StaffTitleDescription]
      ,cast(null as Varchar(max)) as [StaffForename]
      ,cast(null as Varchar(max)) as [StaffMiddleName]
      ,cast(null as Varchar(max)) as [StaffSurname]
      ,cast(null as Varchar(max)) as [StaffODSCode]
      ,cast(null as Varchar(max)) as [StaffLocalCMCId]
      ,cast(null as  date) as [StaffCreatedDate]
      ,cast(null as Varchar(255)) as [StaffActive]
      ,cast(null as Varchar(max)) as [StaffActiveDescription]
      ,cast(null as Varchar(max)) as [StaffDescription]
      ,cast(null as Varchar(255)) as [StaffProviderType]
      ,cast(null as Varchar(max)) as [StaffProviderTypeDescription]
      ,cast(null as Varchar(255)) as [StaffRegistryId]
      ,cast(null as Varchar(max)) as [StaffUserId]
      ,cast(null as Varchar(36)) as [StaffUserClinician]

	  into [ETL_Local_PROD].[dbo].[AT_Accurate_Entered_by]

	FROM [ETL_Local_PROD].[dbo].[AuditPatient_New]ap
  --left join [ETL_Local_PROD].[dbo].[AT_Staff] s on ap.StaffRegistryId = s.StaffRegistryId
  --left join [ETL_Local_PROD].[dbo].[AT_Dept] d on ap.DeptPDRegistryId = d.DeptPDRegistryId
	where ActionType in ('create','revise')



	update r
			
			set r.DeptName = d.DeptName,
				r.[OrganizationType] = d.OrganizationType,
				r.[OrganizationTypeDescription] = d.[OrganizationTypeDescription],
				r.[Organization] = d.[Organization],
				r.[DeptSource] = d.[DeptSource],
				r.[DeptEnterpriseID] = d.[DeptEnterpriseID],
				r.[DeptLocalCMCId] = d.[DeptLocalCMCId],
				r.[LocalCMCOrgType] = d.[LocalCMCOrgType],
				r.[LocalCMCOrgTypeDescription] = d.[LocalCMCOrgTypeDescription],
				r.[DeptODSCode] = d.[DeptODSCode],
				r.[DeptOpenDate] = d.[DeptOpenDate],
				r.[DeptCloseDate] = d.[DeptCloseDate]
			

	from [ETL_Local_PROD].[dbo].[AT_Accurate_Entered_by]r
	--left join [ETL_Local_PROD].[dbo].[AT_Staff] s on s.StaffRegistryId = r.StaffRegistryId
    left join #PDDept d on d.DeptPDRegistryId = r.DeptPDRegistryId 




	update r
			
			set r.[StaffEnterpriseID] = s.[StaffEnterpriseID],
				r.[Individual] = s.[Individual],
				r.[StaffTitle] = s.[StaffTitle],
				r.[StaffTitleDescription] = s.[StaffTitleDescription],
				r.[StaffForename] = s.[StaffForename],
				r.[StaffMiddleName] = s.[StaffMiddleName],
				r.[StaffSurname] = s.[StaffSurname],
				r.[StaffODSCode] = s.[StaffODSCode],
				r.[StaffLocalCMCId] = s.[StaffLocalCMCId],
				r.[StaffCreatedDate] = s.[StaffCreatedDate],
				r.[StaffActive] = s.[StaffActive],
				r.[StaffActiveDescription] = s.[StaffActiveDescription],
				r.[StaffDescription] = s.[StaffDescription],
				r.[StaffProviderType] = s.[StaffProviderType],
				r.[StaffProviderTypeDescription] = s.[StaffProviderTypeDescription],
				r.[StaffRegistryId] = s.[StaffRegistryId],
				r.[StaffUserId] = s.[StaffUserId],
				r.[StaffUserClinician] = s.[StaffUserClinician]


	from [ETL_Local_PROD].[dbo].[AT_Accurate_Entered_by]r
	left join [ETL_Local_PROD].[dbo].[AT_Staff] s on s.StaffRegistryId = r.StaffRegistryId
   





  --select top 10000 *  from [ETL_Local_PROD].[dbo].[AT_Accurate_Entered_by]

--select top 10 * from [ETL_Local_PROD].[dbo].[AT_Staff] 
--select top 10 * from [ETL_Local_PROD].[dbo].[AT_Dept]
--select count(*)FROM [ETL_Local_PROD].[dbo].[AuditPatient_New] where ActionType in ('create','revise')
   /** table Work...


  ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_New]
   ADD CONSTRAINT PK_AuditTransactionOrder_ItemID PRIMARY KEY CLUSTERED (PatAuditID);

  ALTER INDEX PK_AuditTransactionOrder_ItemID ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  DISABLE
  go
  ALTER INDEX PK_AuditTransactionOrder_ItemID ON [ETL_Local_PROD].[dbo].[AuditPatient_New] REBUILD
  go

  select top 5000 * from [ETL_Local_PROD].[dbo].[AuditPatient_New] 
  SELECT MAX(LEN(Role)) FROM [ETL_Local_PROD].[dbo].[AuditPatient_New] 

  ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_New] 
  ADD 
      OverAllOrder int, 
	 ActionTypeOrder int;
	

  ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_New] 
  ALTER COLUMN Role NVARCHAR(100) NOT NULL;

    ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_New] 
  ALTER COLUMN ToPatientSummary NVARCHAR(75) ;

      ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_New] 
  ALTER COLUMN [GenusId] NVARCHAR(75) ;

  CREATE INDEX CMCID_ActionType
ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  (CMC_ID, ActionType);

  CREATE INDEX Role_ActionType
ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  (Role, ActionType);

  CREATE INDEX CareRecord_Date
ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  (CMC_ID, ActionTime);

  CREATE INDEX CareRecord_PatAuditIndex
ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  (CMC_ID, PatAuditID);


  ALTER TABLE [Sales].[SalesOrderDetail] ADD  CONSTRAINT [PK_SalesOrderDetail_SalesOrderID_SalesOrderDetailID] PRIMARY KEY CLUSTERED 
(
    [SalesOrderID] ASC,
    [SalesOrderDetailID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

**/

  --select * from #PatientHSCContacts where CMC_ID = 100013863


   if OBJECT_ID ('Tempdb..#PatientHSCContacts') is not null
	drop table #PatientHSCContacts

select 
sel1.*,
np1.Description as StaffTitleDescription,
np2.Description as NamePrefixDescription,
dpr.Description as RoleDescription,
dor.Description as OrgRoleDescription

into #PatientHSCContacts

from
	(
		SELECT
		no.PatientNumber as CMC_ID,
		ps.ItemID as PatientSummary,

		pa.Provider,
		a.StaffEnterpriseID,
		a.Individual,
		a.StaffTitle,
		a.StaffForename,
		a.StaffMiddleName,
		a.StaffSurname,
		a.StaffODSCode,
		a.StaffLocalCMCId,
		a.StaffCreatedDate,
		a.StaffActive,
		a.StaffDescription,
		DeptName,
		OrganizationType,
		OrganizationTypeDescription,
		a.Organization,
		a.DeptSource,
		DeptEnterpriseID,
		a.DeptPDRegistryID,
		a.DeptLocalCMCId,
		a.LocalCMCOrgType,
		a.LocalCMCOrgTypeDescription,
		a.DeptODSCode,
		a.DeptOpenDate,
		a.DeptCloseDate,
		b.Role,
		b.OrgRole,
		b.Comment,
		b.FromTime,
		b.ToTime,
		isnull(b.SelectedProviderCareProviderType,ip.CareProviderType) as CareProviderType,
		isnull(b.SelectedProviderNamePrefix,ip.NamePrefix) as NamePrefix,
		isnull(b.SelectedProviderGivenName,ip.GivenName) as GivenName,
		isnull(b.SelectedProviderFamilyName,ip.FamilyName) as FamilyName,
		b.SelectedOrgName,
		b.SelectedOrgType,
		case when b.MainHealthcareContact = 1 then 'Y' else 'N' end as MainHealthcareContact,
		ROW_NUMBER() over (partition by ps.ItemId order by pa.Provider) as ProviderNo

		from ETL_PROD.dbo.CMC_PatientSummary ps
		join ETL_PROD.dbo.CMC_Patient_PatientNumbers po on po.Patient = ps.ItemID
		join ETL_PROD.dbo.CMC_PatientNumber no on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
		join ETL_PROD.dbo.CMC_PatientSummary_Providers pa on ps.itemid = pa.PatientSummary
		join ETL_PROD.dbo.CMC_DocumentProvider b on pa.Provider = b.Provider
		left join [ETL_Local_PROD].[dbo].[AT_StaffDeptContext] a on pa.Provider = a.ProviderOrgContext
		left join ETL_PROD.dbo.CMC_IndividualProvider ip on ip.ItemId = b.Provider

		--where no.PatientNumber = 100013863

	) sel1
-- Add title and role descriptions MS 19.2.16
left join ETL_PROD.dbo.Coded_NamePrefix np1 on StaffTitle = np1.code
left join ETL_PROD.dbo.Coded_NamePrefix np2 on NamePrefix = np2.code
left join ETL_PROD.dbo.Coded_DocumentProviderRole dpr on Role = dpr.code
left join ETL_PROD.dbo.Coded_DocumentOrganizationRole dor on OrgRole = dor.code

 
 


  if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_PatientRegistered_GP]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_PatientRegistered_GP]

	select 
	CMC_ID, 
	PatientSummary, 
	RegisteredGP, 
	CCG,
	-- Add new CCG ODS field for all, not just London MS 7.3.16
	case 
		when CCG = 'NHS SURREY DOWNS CCG' then NULL 
			else DeptODSCode 
	end as London_CCG_ODS,
	-- This field populated only if not Unknown/Cross-Border
	DeptODSCode as CCG_ODS,
	case
	  when CCG = 'NHS SURREY DOWNS CCG' then 'Surrey Downs'
	  when CCG like 'Cross Border%' then 'Cross Border'
	  when CCG like 'Unknown%' then 'Unknown'
		else 'London'
	end as CommissioningArea,
	SurgeryEId,
	GPEId,
	Surgery

	into [ETL_Local_PROD].[dbo].[AT_PatientRegistered_GP]

	from
	(select
	cmc_id,
	c.PatientSummary,
	Provider as RegisteredGP,
	case
	-- workaround for West London
	  when h.name6 = 'NHS WEST LONDON CCG' then h.name6
	  when h.name5 = 'NHS WEST LONDON CCG' then h.name5
	  when h.typedesc6 = 'CCG' then h.name6
	  when h.typedesc5 = 'CCG' then h.name5
	  when h.name6 like '%CCG' then 'Cross Border: ' + h.name6
	  when h.name5 like '%CCG' then 'Cross Border: ' + h.name5
	-- workaround for org to org links that have gone walkies
	  when m.dept_parent like '%CCG' then
		case m.dept_parent
		  when 'NHS WEST LONDON (K&C & QPP) CCG' then 'NHS WEST LONDON CCG'
		  else m.dept_parent end 
	  else 'Unknown/Practice not a Practice: ' + isnull(h.name6,'not given') end as CCG,
	ROW_NUMBER() over (partition by c.patientsummary
-- MS 3.11.16 proper sequencing
	order by
	case when h.typedesc6='CCG' then 0 when h.typedesc5='CCG' then 0 else 1 end,
	h.name6,h.name5) as prn,
	DeptEnterpriseId as SurgeryEId,
	StaffEnterpriseId as GPEId,
	DeptName as Surgery
	from #PatientHSCContacts c
	left join  [ETL_Local_PROD].[dbo].[AT_Dept_Heirarchy] h on c.DeptEnterpriseID = h.eid7
	left join Protocol.ODSReconciliationCandidates m on c.DeptLocalCMCId = m.dept_id
	where role = 'REG'
	-- take end date into account MS 21.3.16
	and (c.ToTime is null or cast(c.ToTime as date) > CAST(getdate() as DATE))
	and (c.FromTime is null or cast(c.FromTime as date) <= CAST(getdate() as DATE))
	) sel1
	-- Remove any possible duplicates MS 21.3.16
	left join (select *,ROW_NUMBER() over (PARTITION by deptname
	-- MS 3.11.16 proper sequencing
	order by deptenterpriseid) as drn from #PDDept) d
	  on sel1.CCG = d.DeptName
	where (drn=1 or drn is null) and prn=1

	 
	if OBJECT_ID ('Tempdb..#PatientCPRApprovers') is not null
	drop table #PatientCPRApprovers
	select CPR,
	apr.ApprovalTime,
	apr.ApproverName,ROW_NUMBER() OVER (partition by cpr order by approvaltime) as rn 
	into #PatientCPRApprovers
	from ETL_PROD.dbo.CMC_CPR_Approvers ap 
	left join ETL_PROD.dbo.CMC_CPRApprover apr on ap.Approver = apr.itemid

 
 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------




    if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_Patient_General]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_Patient_General]

	select 
	
	po.CMC_ID,
	NHS_Number,
	'Yes' as OnNewSystem,
	a.LastestVersion as VersionNumber,
	a.GenusId,
	a.FirstVersion as FirstVersionNumber,
	ps.itemid as PatientSummary,
	p.itemid as Patient,
	cp.ItemId as Care_Plan,
	 rc.runcomplete  as DataLoadDate,

	-- Genuine Available Care Plan Check
	-- 7 = Consent Withdrawn
	case when c.Type = '7' then 'Y' else 'N' end as IsSoftDeleted,
	--* 
	--pad.CombinedAddress as #MAIN_ADDRESS,

	-- Metadata
	cp.DateLastSaved,
	cp1.EnteredBy as OriginalEnteredBy,
	-- Simplified version
	isnull(cast(demo.Add_Date as date),convert(date,aup.ActionTime)) as Add_Date,
	cp1.LastApprovedBy as OriginalApprovedBy,
	
	'Completed' as OriginalAssessmentStatus,
	isnull(cast(demo.Date_original_Approval as date),convert(date,auz.ActionTime)) as Date_Original_Approval,
	isnull(la.AuditId,cp1.EnteredBy) as LatestEnteredBy,
	isnull(la.ActionTime,cast(demo.Date_Latest_Assessment as datetime)) as Date_Latest_Assessment,
	cp.LastApprovedBy as LatestApprovedBy,
	cp.LastApprovedTime as Date_Latest_Approval,

	
		-- Add title code MS 7.3.16
		NamePrefix as PatTitle,
		npc.Description as TITLE,
		n.GivenName as FORENAME,
		n.MiddleName,
		-- deal with duff apostrophes MS 11.2.17
		replace(n.FamilyName,'&apos;','''') as SURNAME,
		n.CCPreferredName as PreferredName,
		cast(p.DateOfBirth as date) as DoB,
		isnull(
		  DateDiff(year,p.DateOfBirth,getdate()) -
			case
			  when dbo.Date(year(getdate()),month(p.DateOfBirth),day(p.DateOfBirth)) <
				getdate() then 0
			  else 1 end,-1) as Age,
		p.Gender as GENDER,
		p.MaritalStatus as MARITALSTATUS,
		p.EthnicGroup as ETHNICITY,
		p.LivingCondDetails,
		p.Religion as RELIGION,
		p.PrimaryLanguage as PrimaryLanguage,
		p.PrimaryLangDetails,
		cast(p.DateOfDeath as date) as DoD,
		p.DeathLocation,
		p.DeathSourceInfo,
		p.DeathLocationOther,
		p.DeathVariance,
		p.DeathVarianceOther,
		case p.IsProtected when 0 then 'N' else 'Y' end as Restricted,
		p.PDSOverride,
		-- Currently no non-null values
		case
		  when pds.[fact of death] is not null and pds.[Fact of Death] = 'D' then
		 cast(pds.[date of death] as date) 
		  else null end as DoD_PDS,
		case [Fact of Death]
		  when 'D' then 'Deceased'
		  else 'Living' end as DeceasedPDS,
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
	(select CONVERT(date,[Latest Add Date Reconciled]) from PDSStatistics) as PDS_Reconciliation_Date,

	case when pscmain.Main_LastAddressflag = 1 then pscmain.CombinedAddress end as MAIN_ADDRESS,
	case when pscmain.Main_LastAddressflag = 1 then pscmain.PostalCode end as MAIN_POSTCODE,
	case when pscmain.Main_LastAddressflag = 1 then pscmain.CombinedAddress end as PRIMARY_ADDRESS,
	case when pscmain.Main_LastAddressflag = 1 then pscmain.PostalCode end as PRIMARY_POSTCODE,
	case when pscTemp.Crr_Temp_LastAddressflag = 1 then pscTemp.CombinedAddress end as CURRENT_ADDRESS,
	case when pscTemp.Crr_Temp_LastAddressflag = 1 then pscTemp.PostalCode end as CURRENT_POSTCODE,
	case when pscSecond.Second_LastAddressflag = 1 then pscSecond.CombinedAddress end as SECONDARY_ADDRESS,
	case when pscSecond.Second_LastAddressflag = 1 then pscSecond.PostalCode end as SECONDARY_POSTCODE,
	hct.ContactValue  as Home_Phone,
	mct.contactType as Mobile_Phone,
	wct.contactType as Work_Phone,
	ect.contactType as Email,
	oct.contactType as Other_Phone,

 
		-- GP Information
		gp.Surgery as GP_Practice,
		gp.CCG,
		gp.CommissioningArea,
		gp.London_CCG_ODS as London_CCG_ODS,
		-- Add more general CCG ODS code MS 7.3.16
		gp.CCG_ODS,


			-- Consent Information
		c.Clinician as ConsentedBy,
		 cast(c.DateObtained as date)  as ConsentedDate,
		c.Type as CONSENT,
		c.Comments as MC_DET,
		p.ReqCopy as REQ_COPY,
		cp.PlannedReviewer as PlannedReviewer,
		cast(cp.PlannedReviewTime as date)  as PlannedReviewDate,

		-- Contacts
		c.POADocLocation,

		-- Medical Background
		pr.Clinician as PrognosisBy,
		pr.FamilyAwareProgDetails as PA_FAMPRODDETAILS,
		pr.FamilyAwarePrognosis as PA_FAMPROD,
		pr.PatientAwareProgDetails as PA_PRODDETAILS,
		pr.PatientAwarePrognosis as PA_PROD,
		pr.TimeFrame,
		pr.TimeFrameUnits as ALT_PROGNOSIS,
		pr.Surprise as Surprise,
		cast(pr.UpdatedOn as date) as DATE_PROGNOSIS,
		mb.ADRTDetails,
		mb.ADRTExists as ADRTExists,
		mb.DisabilityDetails as COMM_DIFF_DETAIL,
		mb.FamilyAwareDiagDetails as C_AWAREDETAILS,
		mb.FamilyAwareDiagnosis as C_AWARE,
		mb.HaveDisability as COMMDIFF,
		mb.LevelOfTrtmnt as CEILTREAT,
		mb.LevelOfTrtmntDetails as CT_DET,
		mb.OtherSignifHx as SIGNIFICANT_MEDICAL,
		mb.PatientAwareDiagDetails as P_AWAREDETAILS,
		mb.PatientAwareDiagnosis as P_AWARE,
		mb.WHOPerf as WHPERF,
		cast(mb.WHOPerfTime as date) as WHP_DATE,
		-- Handle primary and secondary cancers properly MS 2.5.16
		dc.DiagnosisCategory as Classified_Diagnosis,
		dc.DiagnosisCode as DiagnosisCode,

		-- CPR
		cast(cpr.CPRReviewDate as date) as REVIEW_DATE,
		cpr.ChildInvolv,
		-- Currently no non-null values
		cpr.ChildParentConsult,
		-- Currently no non-null values
		cpr.Clinician as CPRBy,
		cpr.ClinicianAware as VALIDAD,
		cpr.AdditionalDetail as POSITION,
		cast(cpr.ClinicianTime as date) as DNARDATE1,
		cpr.CourtOrder,
		-- Currently no non-null values
		cpr.DNACPRFormLocation as ORDER_YES,
		cpr.DNACPRFormUploaded,
		-- Currently no non-null values
		cpr.Decision as CARDIO_YN,
		cast(cpr.DecisionTime as date) as CPRDECDATE,
		cpr.FamilyDiscussion as RESUS_FAMILY,
		cpr.FamilyDiscussionComments as RESUS_FAMDET,
		cast(cpr.FamilyDiscussionTime as date) as FamilyDiscussionTime,
		case cpr.HasBeenAgreed when 0 then 'N' else 'Y' end as HasBeenAgreed,
		cpr.JudgeCourt,
		-- Currently no non-null values
		cpr.JudgeCourtLocation,
		-- Currently no non-null values
		cpr.JudgeCourtTime,
		-- Currently no non-null values
		cpr.OtherTeamMemb as NAMEMEM,
		cpr.PatientDiscussion as RESUS_PATIENT,
		cpr.PatientDiscussionComments as RESUS_PATIENTDET,
		cast(cpr.PatientDiscussionTime as date) as PatientDiscussionTime,
		cpr.PtAbleToDecide as HAVECAP,
		cpr.WelfareAttourney as APPOINTWA,
		cpr.WhyCPRInapp as CLINPROB,
		ap1.ApproverName as DNARNAME1,
		ap2.ApproverName as DNARNAME2,
		ap3.ApproverName as DNARNAME3,
		cast(ap1.ApprovalTime as date)as DNARDATE2,
		cast(ap2.ApprovalTime as date) as DNARDATE3,
		cast(ap3.ApprovalTime as date) as DNARDATE4,

	 -- Social Background
	pkg.DS1500 as DS1500,
	pkg.Equipment as EQUIP,
	pkg.EquipmentNotes as EQUIP_DETAIL,
	pkg.FamilySupport as FAM_SUPPORT,
	pkg.FamilySupportNotes as FAM_SUPPORT_Y,
	pkg.HomecareHelp as HOMECARE,
	pkg.HomecareHelpNotes as HOMECARE_DET,
	pkg.PatientReceipt as CAREPLAN,
	pkg.PatientReceiptNotes as CAREPLAN_DETAIL,

	 -- Medications
	ms.Anticoags as Anticoags,
	ms.Insulin as Insulin,
	ms.MedListLocation,
	ms.Opioids as OPIOID,
	ms.OtherInfo as MED_OTH,
	ms.Steroids as Steroids,


	 -- Preferences
	pf.CulturalRelNeeds as CULTURAL,
	pf.FamilyAwarePref as FAMILY_AWAR,
	pf.OrganDonat as WISHES,
	pf.OrganDonatDet as WISHES_YES,
	pf.PatientWishes as PERCARE_PLAN,
	pf.PlaceCare as PPC,
	pf.PlaceCareDet as PPDDiscuss,
	pf.PlaceCareDet,
	-- unconfuse things
	pf.PlaceDeath1 as PPD1,
	pf.PlaceDeath1Det as PPCDiscuss,
	-- unconfuse things
	pf.PlaceDeath1Det,
	pf.PlaceDeath2 as PPD2,
	pf.PlaceDeath2Det

 into [ETL_Local_PROD].[dbo].[AT_Patient_General]

	from [ETL_Local_PROD].[dbo].[AT_CarePlanVersion] a
Left join ETL_PROD.dbo.CMC_CarePlan cp on cp.ItemId = a.LastCarePlan  
Left join ETL_PROD.dbo.CMC_CarePlan cp1 on cp1.ItemId = a.FirstCarePlan  
Left join ETL_PROD.dbo.CMC_PatientSummary ps on ps.ItemID =  a.LastPatientSummary  
Left join ETL_PROD.dbo.CMC_Patient p on p.ItemId = ps.Patient  
Left join ETL_PROD.dbo.CMC_Name n on n.itemid = p.Name  
Left join AT_CMCID po on po.Patient = ps.ItemID
Left join AT_NHSNumbers pn on pn.Patient = ps.ItemID



Left join [ETL_Local_PROD].[dbo].[AT_PatientRegistered_GP] gp on gp.patientsummary = ps.ItemId
left join ETL_PROD.dbo.CMC_CarePackage pkg on pkg.ItemId = ps.CarePackage
left join ETL_PROD.dbo.CMC_Consent c on c.ItemId = p.Consent
left join ETL_PROD.dbo.CMC_CPR cpr on ps.CPR = cpr.ItemId
left join ETL_PROD.dbo.CMC_MedicalBackground mb on ps.MedicalBackground = mb.ItemId
left join ETL_PROD.dbo.CMC_MedicationSummary ms on ms.ItemId = ps.MedicationSummary
left join ETL_PROD.dbo.CMC_PatientSummary_PatientPreferences psf on ps.ItemId = psf.PatientSummary
left join ETL_PROD.dbo.CMC_PatientPreference pf on psf.PatientPreference = pf.ItemId
left join ETL_PROD.dbo.CMC_Prognosis pr on ps.Prognosis = pr.ItemId

left join (select * from #PatientCPRApprovers where rn=1) ap1 on cpr.ItemId = ap1.CPR
left join (select * from #PatientCPRApprovers where rn=2) ap2 on cpr.ItemId = ap2.CPR
left join (select * from #PatientCPRApprovers where rn=3) ap3 on cpr.ItemId = ap3.CPR

--select * from AT_PatAddress where CMC_ID = 100007994
left join AT_PatAddress pscmain on pscmain.CMC_ID = a.GenusID and pscmain.Main_LastAddressFlag = 1
left join AT_PatAddress pscSecond on pscSecond.CMC_ID = a.GenusID and pscSecond.Second_LastAddressFlag = 1
left join AT_PatAddress pscTemp on pscTemp.CMC_ID = a.GenusID and pscTemp.Crr_Temp_LastAddressflag = 1
 --select * from [AT_CarePlanVersion] 

--select * from #PatTele where CMC_ID =  100013863
--left join #PatTele hct on hct.Patient = ps.Patient 
left join #PatTele hct on hct.Patient = ps.Patient and hct.contactType = 'HOME' 
left join #PatTele mct on mct.Patient = ps.Patient and mct.contactType = 'MOBILE' 
left join #PatTele wct on wct.Patient = ps.Patient and wct.contactType = 'WORK' 
left join #PatTele ect on ect.Patient = ps.Patient and ect.contactType = 'EMAIL'
left join #PatTele oct on oct.Patient = ps.Patient and oct.contactType = 'OTHER'

left join ETL_PROD.dbo.Coded_NamePrefix npc on npc.code = n.NamePrefix
--select * from #PatDiagnosis
left join AT_PatDiagnosis dc on dc.Patient = po.Patient  and dc.PatDiagRN = 1
 
left join (select top 1 * from ETL_PROD.dbo.CMC_RUN_COMPLETE where ItemId=1) rc on 1=1

--select * from #AuditPatient
 
left join [ETL_Local_PROD].[dbo].[AuditPatient_New]aup on aup.CMC_ID = po.CMC_ID and aup.ActionType = 'Create' and aup.ActionTypeOrder = 1
left join [ETL_Local_PROD].[dbo].[AuditPatient_New]auz on auz.CMC_ID = po.CMC_ID and auz.ActionType = 'Publish' and auz.ActionTypeOrder = 1
left join (
select cmc_id,add_date,Date_Original_Assessment,Date_Latest_Assessment,original_approval_date as date_original_approval from Protocol.OldSystemCarePlans
union all
select cmc_id,add_date,Date_Original_Assessment,Date_Latest_Assessment,original_approval_date as date_original_approval from Protocol.OldSystemCarePlansMigratedDuplicates
) demo on demo.CMC_ID = po.CMC_ID

left join [ETL_Local_PROD].[dbo].[AT_Accurate_Entered_by] la on la.patientsummary = ps.ItemId
left join Load.PDS on pds.[Local PID] = po.CMC_ID

--where a.GenusId = 100013863


--select * from #Version where  GenusId = 100013863
--select * from [ETL_Local_PROD].[dbo].[AuditPatient_New]where  CMC_ID = 100013863

 
 --select top 5000 * from [ETL_Local_PROD].[dbo].[AT_Patient_General] where cmc_ID = 100013863

 --select top 500 * from  ETL_PROD.dbo.CMC_CarePlan 
  --select 
  --a.*,b.* 
  --from [ETL_PROD].[dbo].[CMC_PatientSummary_CarePlan] a
  --left join ETL_PROD.dbo.CMC_CarePlan b on b.ItemId = a.CarePlan
  --where GenusId = 100013863
  --select top 5000 * from [ETL_Local_PROD].[dbo].[AT_Patient_General] where cmc_ID = 100013863
 --select top 5000 * from  [ETL_Local_PROD].[dbo].[AT_CarePlanVersion] 



--left join #PatAddress pad on ps.Patient = pad.Patient and pad.Main_LastAddressflag = 1
--left join #PatAddress sad on ps.Patient = sad.Patient and sad.Second_LastAddressflag = 1
--left join #PatAddress cad on ps.Patient = cad.Patient and cad.Crr_Temp_LastAddressflag = 1

 

