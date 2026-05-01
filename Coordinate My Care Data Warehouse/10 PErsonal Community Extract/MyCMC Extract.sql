
/**

Dear Andrew

Please can you arrange to send me a myCMC report every morning.  What I need for all patients who have a myCMC in draft is;
1.	Names of patients + NHS number
2.	Names of practice the patient belongs to
3.	Practice telephone number
4.	Practice email address

Each morning we will call and email the practice to tell them there is a plan in draft that needs approval.


**/
if OBJECT_ID ('Tempdb..#TelephoneGP') is not null
 drop table #TelephoneGP

SELECT 
distinct 
case when left(isnull(ph.FullNumber,ph.TelephoneNumber) ,2) <> '07' then row_number() over (partition by OrganizationEID order by case when tt.Description = 'Business Phone' then 1 else left(isnull(ph.FullNumber,ph.TelephoneNumber),5) end )else null end as No_07_PhoneOrder,
case when left(isnull(ph.FullNumber,ph.TelephoneNumber) ,2) = '07' then row_number() over (partition by OrganizationEID order by case when tt.Description = 'Business Phone' then 1 else left(isnull(ph.FullNumber,ph.TelephoneNumber),5) end )else null end as Yes_07_PhoneOrder,
OrganizationEID as DeptEnterpriseId, 
isnull(ph.FullNumber,ph.TelephoneNumber) as Telephone, 
tt.Description as TelephoneType,
convert(date, creationDateTime) as CreationDate,
convert(date,ds.StartDate) as StartDate,
convert(date,ds.EndDate) as EndDate

into #TelephoneGP

  FROM [ETL_PROD].[dbo].[CMC_Location] lo  
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

order by OrganizationEID

  
  	if OBJECT_ID ('Tempdb..#GPDetails') is not null
	drop table #GPDetails

--select * FROM [ETL_PROD].[dbo].[CMC_PersonalCommunityRequest]
select
 distinct
 lo.OrganizationEID,
 case 
 when em.Address like '%nhs.net' then row_number() over (partition by lo.OrganizationEID order by Email) else null end as EmailOrder , 
    row_number() over (partition by lo.OrganizationEID order by Email)  as EmailOrderaLL, 
 lo.OrganizationEID AS DeptEnterpriseId,
 em.Address as Email,
 convert(date,ds.StartDate) as StartDate,
 convert(date,ds.EndDate) as EndDate

 into #GPDetails

  FROM [ETL_PROD].[dbo].[CMC_Location] lo 
  join [ETL_Local_PROD].[dbo].[AT_PD_Dept] d on lo.OrganizationEID = deptenterpriseid
-- change CMC_Location_Contacts/CMC_ContactInfo_Emails to CMC_Location_Emails
  join ETL_PROD.dbo.CMC_Location_Emails cp on cp.Location = Lo.ItemId
  join ETL_PROD.dbo.CMC_Email em on cp.Email = em.ItemId
left join ETL_PROD.dbo.CMC_Location_DateSpan lods on lo.ItemId = lods.Location
left join ETL_PROD.dbo.CMC_DateSpan ds on lods.DateSpan = ds.ItemId
-- ensure ind->org info is omitted from this view MS 6.4.16
where 
--lo.IndividualEID is null
-- exclude deleted and expired/unstarted locations MS 25.8.16
--and 
lo.Deleted is null
-- exclude FLAGGING emails MS 17.2.17
and em.TypeCodedValue <> 'FLAGGING'
and (ds.StartDate is null or CAST(startdate as date) <= CAST(getdate() as DATE))
and (ds.EndDate is null or  CAST(enddate as date) > CAST(getdate() as DATE))

--select * FROM [ETL_Local_PROD].[dbo].[AT_AddressGP]

if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_MyCMC_CoreData]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_MyCMC_CoreData]

select
 distinct
CMC_ID,
PatientSummaryId,
Name_GivenName +' '+ Name_FamilyName as PatientName,
Number_PatientNumber as NHS_Number,
row_number() over (partition by Number_PatientNumber order by LastUpdated desc)  as OrderaLL, 
EmailAddress as PatientEmail,
a.postcode as PatientPostCode,
a.gender,
GPOrgRegId,
e.Name,
f.[Organisation Name],
f.[Address Line 1],
f.[Address Line 2],
f.Postcode,
h.Email as GPEmail,
i.Telephone,
f.CCG,
Complete,
CompleteTime,
LastUpdated,
SubmitTime,

InvalidReason,
IsValid
 
 into [ETL_Local_PROD].[dbo].[AT_MyCMC_CoreData]

FROM [ETL_PROD].[dbo].[CMC_PersonalCommunityRequest_Copy]a
 
--left join [ETL_Local_PROD].[dbo].[AT_AddressGP]c on c.DeptEnterpriseId = b.EnterpriseID
left join [ETL_PROD].[dbo].[CMC_Organization]d with (nolock) on d.PDRegistryID = a.GPOrgRegId
left join [ETL_PROD].[dbo].[CMC_OrganizationName]e with (nolock) on e.ItemID = d.name
left join [ETL_Local_PROD].[dbo].[AT_ODS_Data]f on f.[Organisation Code] = d.ODSCode
left join [ETL_Local_PROD].[dbo].[AT_Patient_General]g on g.NHS_Number = a.Number_PatientNumber
left join #GPDetails h on h.DeptEnterpriseID = d.EnterpriseID and h.EmailOrder = 1
left join #TelephoneGP i on i.DeptEnterpriseID = d.EnterpriseID and coalesce(No_07_PhoneORder,Yes_07_PhoneORder) = 1
where 
--CompleteTime is null
--and 
g.CMC_ID is null
and Complete = 0 
and IsValid = 1

--where a.PatientSummaryId = 100069340

--select * from [ETL_Local_PROD].[dbo].[AT_MyCMC_CoreDate] order by NHS_Number
 select * FROM [ETL_PROD].[dbo].[CMC_PersonalCommunityRequest] where Number_PatientNumber = '6124952742'
 
 CMC_ID	GP Practice Name	GP Telephone	GPEmail	NHS_Number	PatientName	LastUpdated	Number of Records
100001430				9999999999	Ben Bernanke	13/03/2020 14:15:00	1


select * from [ETL_Local_PROD].[dbo].[AT_Patient_General] where CMC_ID = 100022511

select * from [ETL_Local_PROD].[dbo].[AT_Emails]
 
select * from  [ETL_Local_PROD].[dbo].[AT_MyCMC_CoreData] order by NHS_Number , OrderaLL


select * from  [ETL_Local_PROD].[dbo].[AT_MyCMC_CoreData] where NHS_Number = '6496586152'
select * FROM [ETL_PROD].[dbo].[CMC_PersonalCommunityRequest_Copy] where Number_PatientNumber = '6496586152'

select * FROM [ETL_PROD].[dbo].[CMC_Organization] where PDRegistryID = '313950'

select * FROM [ETL_PROD].[dbo].[CMC_OrganizationName] where ItemId = '100068530O'


select * from #TelephoneGP where DeptEnterpriseID = 100068530





  select * FROM [ETL_Local_PROD].[dbo].[AT_Patient_General]where CMC_id = 100069340

select * from [ETL_PROD].[dbo].[CMC_PD_RegistryID_XRef]where RegistryID <> 'CMC' order by EnterpriseID


select * from  [ETL_PROD].[dbo].[CMC_PersonalCommunityRequest] order by Number_PatientNumber 
select top 10 * From [dbo].[CMC_Organization]
where PDRegistryID = '292281'



select * from  [ETL_Local_PROD].[dbo].[AT_MyCMC_CoreData] where NHS_Number = '4143314061'


select*

  FROM [ETL_PROD].[dbo].[CMC_Location] lo 
  join [ETL_Local_PROD].[dbo].[AT_PD_Dept] d on lo.OrganizationEID = deptenterpriseid
-- change CMC_Location_Contacts/CMC_ContactInfo_Emails to CMC_Location_Emails
  join ETL_PROD.dbo.CMC_Location_Emails cp on cp.Location = Lo.ItemId
  join ETL_PROD.dbo.CMC_Email em on cp.Email = em.ItemId
  left join ETL_PROD.dbo.CMC_Location_DateSpan lods on lo.ItemId = lods.Location
left join ETL_PROD.dbo.CMC_DateSpan ds on lods.DateSpan = ds.ItemId
-- ensure ind->org info is omitted from this view MS 6.4.16
 
-- exclude FLAGGING emails MS 17.2.17
--and em.TypeCodedValue <> 'FLAGGING'
--and (ds.StartDate is null or CAST(startdate as date) <= CAST(getdate() as DATE))
--and (ds.EndDate is null or  CAST(enddate as date) > CAST(getdate() as DATE))
  where d.DeptPDRegistryID = '297285'

  select * from #GPDetails where OrganizationEID = 100064436