/****** Script for SelectTopNRows command from SSMS  ******/

 


select
'Published' as ReportType,
a.CMC_ID,
Date_Original_Approval,
TeamType,
b.ccg,
b.[NHS Region]
FROM [ETL_Local_PROD].[dbo].[AT_Patient_General]a
left join
(
SELECT 
distinct
CMC_ID,
TEam,
coalesce(
		case 
		When TeamType in ('A&E','999 PROVIDER','111 PROVIDER','OUT OF HOURS') Then 'URGENT CARE'
		--when TeamType = 'PRIMARY CARE' then 'GP'
		when (charindex('mental',TeamType)>0 or charindex('Community',TeamType)>0)  then 'COMMUNITY PROVIDER'
		--when TeamType ='SECONDARY CARE' then 'Acute/Tertiary Services'
		when TeamType is null and charindex('CCG',[Parent Org]) > 0 and  charindex('GP Practices',[Team])> 0 then 'PRIMARY CARE'
		When TeamType = 'UNKNOWN' then 'Not recorded'
			else TeamType
			end,'Not Recorded'
		)  as  TeamType,
OrganisationCCG as ccg,
[NHS Region]
  FROM [ETL_Local_PROD].[dbo].[AT_CarePlanData]
  where   DerivedActionType = 'publish' and ActivityOrderNo = 1 
  )b on b.CMC_ID = a.CMC_ID
  WHere Date_Original_Approval >= dateadd(year,-2,getdate())
 

 union 

 select
 'Create' as ReportType,
CMC_ID,
actionTime,
coalesce(
		case 
		When TeamType in ('A&E','999 PROVIDER','111 PROVIDER','OUT OF HOURS') Then 'URGENT CARE'
		--when TeamType = 'PRIMARY CARE' then 'GP'
		when (charindex('mental',TeamType)>0 or charindex('Community',TeamType)>0)  then 'COMMUNITY PROVIDER'
		--when TeamType ='SECONDARY CARE' then 'Acute/Tertiary Services'
		when TeamType is null and charindex('CCG',[Parent Org]) > 0 and  charindex('GP Practices',[Team])> 0 then 'PRIMARY CARE'
		When TeamType = 'UNKNOWN' then 'Not recorded'
			else TeamType
			end,'Not Recorded'
		)  as  TeamType,
OrganisationCCG,
[NHS Region]
  FROM [ETL_Local_PROD].[dbo].[AT_CarePlanData]
  where   DerivedActionType = 'create' 
  and  convert(Date,actionTime) >= dateadd(year,-2,getdate())