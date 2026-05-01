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

  
  select *
    FROM [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table]
   where TeamType = 'SECONDARY CARE'

   --Year		Month	Created	Own Revisions	Revisions  by Others	Own Views	Views by Others	Urgent Care Views
   SELECT distinct  [Parent Org]
  FROM [AT_TEam_Activity_Hospital_Report_Data_Table] 
   where TeamType = 'SECONDARY CARE'
   --Parent Trust by Hospital Totals		
   select *
    FROM [AT_TEam_Activity_Hospital_Report_Data_Table] 
   where [Parent Org] = 'AIREDALE NHS FOUNDATION TRUST'
   and ActionTime >= '2020-04-01'
   and PatientCCG = 'NHS NOTTINGHAM CITY'
   order by PatientCCG


    select
 *
 from [ETL_Local_PROD].[dbo].[AT_CarePlanData] with (nolock)
 where [Parent Org]  = 'BARTS HEALTH NHS TRUST'
    and ActionTime >= '2020-04-01'
	and PatientCCG = 'NHS NOTTINGHAM CITY'
	and DerivedActionType = 'publish'


	select *  FROM [ETL_Local_PROD].[dbo].[AT_Patient_General]
  --where [Date_Original_Approval] >= '2020-07-01'
  where cmc_id = 100081342


     SELECT distinct  OrganisationCCG
  FROM [AT_TEam_Activity_Hospital_Report_Data_Table] 
   where TeamType = 'SECONDARY CARE'

        SELECT distinct  OrganisationCCG
  FROM [AT_TEam_Activity_Hospital_Report_Data_Table] a
  inner join [ETL_Local_PROD].[Reference].[STP]b on b.CCGLONG_TRUNC = a.OrganisationCCG
   where TeamType = 'SECONDARY CARE'
   and b.STP is not null


	  select 
	  ActionTime
      ,[Team]
      ,[Parent Org]
      ,[TeamType]
      ,[OrganisationCCG]
 
      , [REport_ActionType]
      ,sum(total) as [Total]
	  ,case  
	  when [REport_ActionType] = 'Publish'then 1 
	  when [REport_ActionType] = 'Own Revisions'then 2
	  when [REport_ActionType] = 'Revisions  by Others'then 3
	  when [REport_ActionType] = 'Own Views'then 4
	  when [REport_ActionType] = 'Views by Others'then 5 
	  when [REport_ActionType] = 'Urgent Care Views'then 6

	  End as TableSort
	   from [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Hospital_Report_Data_Table] 
	   where TeamType = 'SECONDARY CARE'
	   and  convert(Date,actiontime)>= @StartDate and convert(date,actiontime)<= @EndDate
 
--and  Total >0 
and  team  in (@RP_Team) 
and [Parent Org] in (@RP_Org ) 
	   --and REport_ActionType in ('Publish','Urgent Care Views', 'Views by Others')
	   group by
	   ActionTime
      ,[Team]
      ,[Parent Org]
      ,[TeamType]
      ,[OrganisationCCG]
   
      , [REport_ActionType] 
	  --,[Total]


	  
	  select 
	  ActionTime
      ,[Team]
      ,[Parent Org]
      ,[TeamType]
      ,[OrganisationCCG]
	   ,PatientCCG
      , [REport_ActionType]
      ,sum(total) as [Total]
	  ,case  
	  when [REport_ActionType] = 'Publish'then 1 
	  when [REport_ActionType] = 'Own Revisions'then 2
	  when [REport_ActionType] = 'Revisions  by Others'then 3
	  when [REport_ActionType] = 'Own Views'then 4
	  when [REport_ActionType] = 'Views by Others'then 5 
	  when [REport_ActionType] = 'Urgent Care Views'then 6

	  End as TableSort
	   from [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Hospital_Report_Data_Table] 
	   where TeamType = 'SECONDARY CARE'
	   and  convert(Date,actiontime)>= @StartDate and convert(date,actiontime)<= @EndDate
 
--and  Total >0 

and [Parent Org] in (@RP_Org ) 
	   --and REport_ActionType in ('Publish','Urgent Care Views', 'Views by Others')
	   group by
	   ActionTime
      ,[Team]
      ,[Parent Org]
      ,[TeamType]
      ,[OrganisationCCG]
		,PatientCCG
      , [REport_ActionType] 