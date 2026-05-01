/****** Script for SelectTopNRows command from SSMS  ******/

	if OBJECT_ID ('Tempdb..#GPDetails') is not null
	drop table #GPDetails

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

where lo.Deleted is null

-- exclude FLAGGING emails MS 17.2.17
and em.TypeCodedValue <> 'FLAGGING'
and (ds.StartDate is null or CAST(startdate as date) <= CAST(getdate() as DATE))
and (ds.EndDate is null or  CAST(enddate as date) > CAST(getdate() as DATE))



SELECT a.[CMC_ID]

      ,[NHS_Number]
 
      ,[GP_Practice]
      ,d.CCGLONGNAME as CCG
	  ,d.STP
     
	  ,c.Email
     
      
  FROM [ETL_Local_PROD].[dbo].[AT_Patient_General]a
  inner join [ETL_Local_PROD].[dbo].[AT_PatientRegistered_GP]b on b.[PatientSummary] = a.[PatientSummary]
  inner join [ETL_Local_PROD].[Reference].[STP]d on d.CCGLONGNAME = a.CCG
   

  left join #GPDetails c on c.DeptEnterpriseId = b.SurgeryEId and c.EmailOrder = 1

   where 
       --left(a.CCG, 3) = 'NHS'    
 --      and 
          coalesce(DOD,dod_pds) is NUll