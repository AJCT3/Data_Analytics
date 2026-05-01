

  if OBJECT_ID ('Tempdb..#Emails') is not null
 drop table #Emails
select
 distinct
 
 case 
 when em.Address like '%nhs.net' then row_number() over (partition by DeptEnterpriseId order by Email) else null end as EmailOrder , 
 DeptEnterpriseId,
 em.Address as Email,
 convert(date,ds.StartDate) as StartDate,
 convert(date,ds.EndDate) as EndDate

 into #Emails

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

--select * from #Emails order by DeptEnterpriseId, EmailOrder


select 
*,
row_number() over (partition by DeptEnterpriseId order by Email) as rn 
from PDDeptEmails 
where Email like '%nhs.net'
 
select *,row_number() over (partition by DeptEnterpriseId order by CombinedAddress) as rn from PDDeptAddresses
select *,row_number() over (partition by DeptEnterpriseId order by Telephone) as rn from PDDeptPhones where left(Telephone,2) <> '07'
select *,row_number() over (partition by DeptEnterpriseId order by Telephone) as rn from PDDeptPhones where left(Telephone,2) = '07'


 if OBJECT_ID ('Tempdb..#AddressGP') is not null
 drop table #AddressGP
select 
case when d.AddressOrder = 1 then 1 else null end as CurrentAddressFlag,
d.*
into #AddressGP
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

FROM [ETL_PROD].[dbo].[CMC_Location] lo join PDDepartment d on lo.OrganizationEID = deptenterpriseid
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
 --select * from #AddressGP

 
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

 --select * from  #TelephoneGP



 if OBJECT_ID ('Tempdb..#Symptoms') is not null
 drop table #Symptoms
 select
 distinct 
 Symptom
 into #Symptoms
 FROM ETL_PROD.dbo.CMC_SymptomPlan

DECLARE @Columns VARCHAR(MAX) = ''

--Concatenate each country with a comma

--The QUOTENAME function adds square brackets around each value

SELECT @Columns += (QUOTENAME(Symptom) + ',')

FROM #Symptoms

--Remove the trailing comma

SET @Columns = LEFT(@Columns, LEN(@Columns) - 1)

--Check the result

--PRINT @Columns


 

SELECT
no.PatientNumber as CMC_ID,
pscp.CarePlan,
sp.Symptom,
--sp.NoteText,
ROW_NUMBER() over (partition by pscp.CarePlan order by sp.Symptom) as SymptomNo,
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
and  pscp.CarePlan = 'CarePlan||100000001||16'









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

SELECT @SQL += '
    , MAX(CASE WHEN SymptomNo = ' + convert(varchar,SymptomNo) + ' THEN Symptom END) AS [SymptomNo_' + convert(varchar,SymptomNo) + ']
    , MAX(CASE WHEN SymptomNo = ' + convert(varchar,SymptomNo) + ' THEN SymptomDescription END) AS [SymptomDescription_' + convert(varchar,SymptomNo) + ']'
FROM (SELECT DISTINCT  SymptomNo  AS SymptomNo FROM #Sample) AS T
ORDER BY   T.SymptomNo  ;

SET @SQL += ' into ##Symptoms FROM #Sample GROUP BY CMC_ID;';








EXECUTE (@SQL);

DROP TABLE #Sample;

select * from ##Symptoms 

drop table ##Symptoms




 