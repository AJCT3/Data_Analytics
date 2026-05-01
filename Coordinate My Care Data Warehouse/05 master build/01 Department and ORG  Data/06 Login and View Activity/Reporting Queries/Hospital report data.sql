/****** Script for SelectTopNRows command from SSMS  ******/
SELECT [ReportType]
      ,[CMC_ID]
      ,convert(date,dateadd(d,-(day([Date_Original_Approval]-1)),[Date_Original_Approval]))  as MonthActivity
      ,[team]
	  ,b.[Parent Org]
      ,b.[TeamType]
      ,[ccg]
      ,[NHS Region]
  FROM [ETL_Local_PROD].[dbo].[AT_Created_And_Published_By_Team]a
  left join [ETL_Local_PROD].[dbo].[AT_Commissioner_Team_Org_Lookup]b on b.ActivityTeam = a.team	
  where team is not null
  and b.TeamType = 'SECONDARY CARE'
  and reportType = 'Published'
  and [Parent Org] = 'Barts Health NHS Trust'





  --Graph Mix

  SELECT  [ActionTime]
      --,[StaffProviderTypeDescription]
      --,[StaffFullName]
      --,[Team]
      ,[Parent Org]
      --,[TeamType]
      --,[OrganisationCCG]
      --,[STP]
      --,[Staff ORganisation Start Date]
      --,[Staff ORganisation End Date]
      ,[Report_ActionType]
      ,sum([Total]) as Total
  FROM [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table]
  where TeamType = 'SECONDARY CARE'
  and Report_ActionType in ('Publish','NON URGENT CARE','URGENT CARE')
  group by 
  [ActionTime],
  [Parent Org],
  [Report_ActionType]




  select *
    FROM [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table]
  where 
  TeamType = 'SECONDARY CARE'
  --and 
  --Report_ActionType = 'Revisions by Others' 
  --and ActionTime = '2020-05-01'

   --TEam Activity
   /**
   [Created]	[Own Revisions]	[Revisions  by Others]	[Own Views]	[Views by Others]	[Urgent Care Views]
`

   **/


  SELECT  *
  FROM [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table]
  where TeamType = 'SECONDARY CARE'
  and Report_ActionType in ('Publish','view','revise','Revisions by Others','NON URGENT CARE','URGENT CARE')
  group by 
  [ActionTime],
  [Team],
  [Parent Org],
  [Report_ActionType]



  select *
   FROM [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table]
  where  Report_ActionType in ( 'NON URGENT CARE','URGENT CARE')