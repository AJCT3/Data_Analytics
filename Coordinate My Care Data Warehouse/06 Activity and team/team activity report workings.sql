/****** Script for SelectTopNRows command from SSMS  ******/
SELECT *
  FROM [ETL_Local_PROD].[dbo].[AT_CarePlanData]
  where DerivedActionType = 'Publish' and ActivityOrderNo = 1
  and CMC_ID in (select distinct Cmc_id from [ETL_Local_PROD].[dbo].[AT_CarePlanData] where DerivedActionType = 'create')  


  SELECT *
  FROM [ETL_Local_PROD].[dbo].[AT_CarePlanViewData]
  where PatientCCG = 'NHS Havering' and convert(Date,ActionTime)>= '2019-05-01' and convert(Date,ActionTime) <= '2020-04-30'



  SELECT *
  FROM [ETL_Local_PROD].[dbo].[AT_CarePlanData]
  where charindex('barts',team)>0
  and convert(Date,ActionTime)>= '2019-04-01' 

  sum(case when [Access Type] like 'create%' then 1 else 0 end) as Created,
sum(case when [Access Type] like 'revise%'  then 1 else 0 end) as Updated,
sum(case when [Access Type] like 'view%' then 1 else 0 end) as Viewed,
sum(case when [Access Type] like 'print%' then 1 else 0 end) as Printed



	DECLARE 
		@EndDate date,  
		@StartDate Date
 

		
	set @StartDate = dateadd(year,-2,convert(datetime, cast(year(dateadd(month, -3, getdate())) as varchar(10)) + '-04-01', 120))
	set @EndDate =  convert(Date,dateadd(d,-(day(getdate())),getdate()))
	print @StartDate
	Print @EndDate

 

			if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table]') is not null
			drop table [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table]



	  SELECT *,
	  case
	  when charindex('audit',DerivedActionType)> 0 then 'Audit'
	  when DerivedActionType in ('clinic_event_added','clinical_event_updated','death_date_added','death_date_removed','deceased_updated','generate_dna_cpr','revise') then 'REvise'
	  when charindex('create',DerivedActionType)> 0 then 'Create'
	  when charindex('publish',DerivedActionType)> 0 then 'Publish'
	  when charindex('delete',DerivedActionType)> 0 then 'Delete'
	  when charindex('discard',DerivedActionType)> 0 then 'Discard'
	  when charindex('enroll',DerivedActionType)> 0 then 'Enroll'
	  when charindex('initiated',DerivedActionType)> 0 then 'Initiated'
	  when DerivedActionType in ('lr_claimed','lr_expired','patient_found','patient_list') then 'Search'
	  when charindex('print',DerivedActionType)> 0 then 'Print'
	  when charindex('proxy',DerivedActionType)> 0 then 'Proxy'
	  when charindex('request',DerivedActionType)> 0 then 'Request'
	  when charindex('subscription',DerivedActionType)> 0 then 'Subscription'
	  when charindex('view',DerivedActionType)> 0 then 'View'
	  else DerivedActionType
	  End as REport_ActionType
	  into [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table]
  FROM [ETL_Local_PROD].[dbo].[AT_CarePlanData]

  where convert(Date,actiontime)>= @StartDate and convert(Date,actiontime) < @EndDate


  	if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_OrgTeam_Reference]') is not null
			drop table [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_OrgTeam_Reference]
 

   SELECT distinct [Parent Org],Team
   into [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_OrgTeam_Reference]
  FROM [ETL_Local_PROD].[dbo].[AT_CarePlanData]

  where convert(Date,actiontime)>= @StartDate and convert(Date,actiontime) < @EndDate
  and TEam is not null
   