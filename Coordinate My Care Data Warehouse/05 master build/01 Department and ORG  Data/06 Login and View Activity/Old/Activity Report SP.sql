USE [ETL_Local_PROD]
GO
/****** Object:  StoredProcedure [dbo].[AT__Activity_Data]    Script Date: 17/06/2020 09:47:26 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




Alter PROCEDURE [dbo].[AT__Activity_Report_Data] 
-- Amended for PD Upgrade
AS
BEGIN

 







	--  Report tables addded May 2020 - we always need more tables in the poxy database!! --------------	
		

	DECLARE 
		@EndDate date,  
		@StartDate Date
 

		
	set @StartDate = dateadd(year,-2,convert(datetime, cast(year(dateadd(month, -3, getdate())) as varchar(10)) + '-04-01', 120))
	set @EndDate =  convert(Date,dateadd(d,-(day(getdate())),getdate()))
	print @StartDate
	Print @EndDate

 

 

	IF OBJECT_ID('tempdb..#Temp_CTE_Date') IS NOT NULL 
	dROP TABLE #Temp_CTE_Date



	;WITH cte AS 
	(
	SELECT   [ID]
	,ROW_NUMBER() OVER(ORDER by ID ASC) AS RowNumb
	,DEnse_RANK() OVER  ( ORDER BY [Week Of Year] ASC)  AS RowNumbWeek
	--,convert(date,dateadd(week,datediff(week,0,[Calendar Day]),0)) as WeekStart
	,case when [Week Day] = 1 then  DATEADD(DAY, 2 - DATEPART(WEEKDAY, dateadd(day,-1,[Calendar Day])), CAST(dateadd(day,-1,[Calendar Day]) AS DATE)) else DATEADD(DAY, 2 - DATEPART(WEEKDAY, [Calendar Day]), CAST([Calendar Day] AS DATE))end  as WeekStart
	,case when [Week Day] = 1 then [Calendar Day] else dateadd(day,6,DATEADD(DAY, 2 - DATEPART(WEEKDAY, [Calendar Day]), CAST([Calendar Day] AS DATE)))end  as  WeekEnd
	--,convert(date,case when [Week day] = 1 then dateadd(day,6,[Calendar Day]) else  dateadd(day,6,DATEADD(dd, -(DATEPART(dw, [Calendar Day])-1), [Calendar Day]))end)   as WeekEnd
      ,[Calendar Day] as MyDate
	  ,convert(varchar,[Calendar Day],103) as CalendarDayField
	  ,[Month Start Date]

    
  FROM [ETL_Local_PROD].[dbo].[DIM_Date]
  where [Calendar Day] >= @StartDate and [Calendar Day] < @EndDate
	)

	--populate temp table with results so they can be used below...
	SELECT *
	into #Temp_CTE_Date		
	FROM cte
	OPTION (MAXRECURSION 0)

	--select * from #Temp_CTE_Date order by WeekStart
	--select dateadd(d,-(day(getdate()-1)),getdate())





 
		if OBJECT_ID ('Tempdb..#PreReport') is not null
			drop table #PreReport




	  SELECT
	  [PatAuditID]
      ,[CMC_ID]
      ,[actionTime]
      ,[ActionType]
      ,[comment]
      ,[DerivedActionType]
      ,[ActivityOrderNo]
      ,[OverAllOrder]
      ,[STAFFEnterpriseID]
      ,[StaffUserId]
      ,[ODSCode]
      ,[StaffActiveDescription]
      ,coalesce([StaffProviderTypeDescription],'Not Recorded') as [StaffProviderTypeDescription]
	  ,[StaffFullName]
      --,dbo.ProperCase([StaffFullName]) as [StaffFullName]
      ,[StaffRegistryId]
      ,[PDRegistryID]
      ,[ItemId]
	  ,TEam
      --,dbo.ProperCase(convert(varchar(400),[Team])) as TEam
      ,[Parent Org]
      ,[TeamType]
      ,[PostCode]
      ,REPLACE([OrganisationCCG], ' CCG', '') as [OrganisationCCG]
      ,[OrgSTP]
      ,[NHS Region]
      ,[DeptODSCode]
      ,[EnterpriseID]
      ,[PatientCCG]
      ,[PatientSTP]
      ,[LoginReference]
      ,[CMC_OrItem]
      ,[CMC_Individual_ItemID]
      ,[CMC_Individual_PDRegistryID]
      ,[App]
	  ,case
	  when charindex('audit',DerivedActionType)> 0 then 'Audit'
	  when DerivedActionType in ('clinic_event_added','clinical_event_updated','death_date_added','death_date_removed','deceased_updated','generate_dna_cpr','revise') then 'Revise'
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

	  into #PreReport
	  --into [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table]


  FROM [ETL_Local_PROD].[dbo].[AT_CarePlanData]

  where convert(Date,actiontime)>= @StartDate and convert(Date,actiontime) < @EndDate

  and DerivedActionType <> 'save'

  

delete from #PreReport where DerivedActionType = 'publish' and ActivityOrderNo > 1

if OBJECT_ID ('Tempdb..#Publishers') is not null
			drop table #Publishers
			select
			CMC_ID,
			Team,
			[OrganisationCCG],
			[Parent Org]

			into #Publishers

			from #PreReport
			where DerivedActionType = 'publish'

  	if OBJECT_ID ('Tempdb..#ExternalViewers') is not null
			drop table #ExternalViewers

			select
			 convert(date,dateadd(d,-(day(actionTime-1)),actionTime))  as MonthActivity,
			  b.Team as PublishingTEam,
			 b.OrganisationCCG as PublsihingCCG,
			 a.TEam as ViewingTEam,
			
			 case 
		When TeamType in ('A&E','999 PROVIDER','111 PROVIDER','OUT OF HOURS') Then 'URGENT CARE'
		----when TeamType = 'PRIMARY CARE' then 'GP'
		--when (charindex('mental',TeamType)>0 or charindex('Community',TeamType)>0)  then 'COMMUNITY PROVIDER'
		----when TeamType ='SECONDARY CARE' then 'Acute/Tertiary Services'
		--when TeamType is null and charindex('CCG',a.[Parent Org]) > 0 and  charindex('GP Practices',a.[Team])> 0 then 'PRIMARY CARE'
		--When TeamType = 'UNKNOWN' then 'Not recorded'
			else 'NON URGENT CARE'
			end as DErivedTEamTypeViewing,
			 TeamType as ViewingTeamType,
			 count(*) as [Total Views]

			 into #ExternalViewers

			from #PreReport a 
			inner join #Publishers b on b.CMC_ID = a.CMC_ID
			where DerivedActionType = 'view'
			and b.Team <> a.Team
			group by
			convert(date,dateadd(d,-(day(actionTime-1)),actionTime)),
			a.[Parent Org],
			 a.TEam,
			 b.Team,
			 b.OrganisationCCG,
			 TeamType



			 --select * from #ExternalViewers where charindex('marsden',publishingteam)>0 order by publishingteam


  	if OBJECT_ID ('Tempdb..#PreReportUsers') is not null
			drop table #PreReportUsers
			select
			distinct 
			  [StaffFullName]
			  ,[StaffProviderTypeDescription]
 
			  ,TEam
			  --,dbo.ProperCase(convert(varchar(400),[Team])) as TEam
		 
			  ,[TeamType]
	 

			  into #PreReportUsers

			from #PreReport

			--select * from #PreReportUsers
  
		if OBJECT_ID ('Tempdb..#PreReportLogins') is not null
			drop table #PreReportLogins

			SELECT  
			--[UserRegistryID]
			--  ,[OrganizationRegistryID]
			  --,
			  [UserID]
			  ,[UserRole]
			  ,[ETL_Local_PROD].[dbo].[ProperCase]([UserFullName])as [UserFullName]
			  ,[OrganizationName]
			  --,[deptenterpriseid]
			  --,[StaffEnterpriseID]
			  ,[StaffORgStartDate]
			  ,[StaffORgEndDate]
			  ,a.[TeamType]
			  ,c.StaffProviderTypeDescription
			  ,a.[Parent Org]
			  ,b.[Month Start Date] as MonthActivity
			
			  ----,[LoginDate]
			  --,[SessionDuration]
			  --,[DerivedDevicetype]
			  ,a.[PostCode]
			  ,a.[OrganisationCCG]
			  ,[OrgSTP]
			  ,[NHS Region]
			  ,count(*) as Total
			  --,[Domain]
			  --,[App]
			  --,[ActionType]

			  into #PreReportLogins

		  FROM [ETL_Local_PROD].[dbo].[AT_Logins_Daily]a
		  left join [ETL_Local_PROD].[dbo].[DIM_Date]b on b.[Calendar Day] = convert(Date,LoginDate)
		  inner join #PreReportUsers c on c.StaffFullName = a.UserFullName
									and c.Team = a.OrganizationName
									 
		  group by
		   [UserID]
			  ,[UserRole]
			  ,[UserFullName]
			  ,[OrganizationName]
			  ,c.StaffProviderTypeDescription
			  ,[StaffORgStartDate]
			  ,[StaffORgEndDate]
			  ,a.[TeamType]
			  ,a.[Parent Org]
			  ,b.[Month Start Date]
			  --,[DerivedDevicetype]
			  ,a.[PostCode]
			  ,a.[OrganisationCCG]
			  ,[OrgSTP]
			  ,[NHS Region]




			  --select * from #PreReportLogins











		if OBJECT_ID ('Tempdb..#PreReport2') is not null
			drop table #PreReport2
			select
			d.*
			into #PreReport2
			from
			(
			select
			 convert(date,dateadd(d,-(day(b.actionTime-1)),b.actionTime))  as MonthActivity
			 ,b.[StaffProviderTypeDescription]
			 ,b.[StaffFullName]
			 ,b.[Team]
			  ,b.[Parent Org]
			  ,b.[TeamType]
			  ,b.[OrganisationCCG]
			  ,b.REport_ActionType
			  ,Count(*) as Total
			   
			   from [#PreReport]b
		
			   group by

	   				 convert(date,dateadd(d,-(day(b.actionTime-1)),b.actionTime)) 
				 ,b.[StaffProviderTypeDescription]
				 ,b.[StaffFullName]
				 ,b.[Team]
 
				  ,b.[Parent Org]
				  ,b.[TeamType]
				  ,b.[OrganisationCCG]
				  ,b.REport_ActionType



				  union 

				  select
			 MonthActivity
			 ,b.[StaffProviderTypeDescription]
			 ,b.[UserFullName]
			 ,b.[OrganizationName]
			  ,b.[Parent Org]
			  ,b.[TeamType]
			  ,REPLACE(b.[OrganisationCCG], ' CCG', '') as [OrganisationCCG]
			  ,'Daily Logins' as REport_ActionType
			  ,Count(*) as Total

				  from #PreReportLogins b
				 group by
				  MonthActivity
			 ,b.[StaffProviderTypeDescription]
			 ,b.[UserFullName]
			 ,b.[OrganizationName]
			  ,b.[Parent Org]
			  ,b.[TeamType]
			  ,b.[OrganisationCCG]

	  )d



	   update x
				set StaffFullName = [ETL_Local_PROD].[dbo].[ProperCase]([StaffFullName]),
				TEam = [ETL_Local_PROD].[dbo].[ProperCase](convert(varchar(400),[Team])) 

	   from #PreReport2 x




	   

  			if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table]') is not null
			drop table [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table]
   
    

	select  
	distinct
	a.[Month Start Date] as ActionTime
 
     , replace([StaffProviderTypeDescription],'General Medical Practitioner: No Organisation Sub-Type','GP') as [StaffProviderTypeDescription]
	 , [StaffFullName]
	 , [Team]
      , [Parent Org]
      , [TeamType]
	  , [OrganisationCCG]
	  ,CAST(NULL AS VARCHAR(400)) AS STP
	  ,cast(null as date) as [Staff ORganisation Start Date]
	  ,cast(null as date) as [Staff ORganisation End Date]
	  , Report_ActionType
	  ,null as Total

	  into [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table]
 
		from [#PreReport2]b
		cross join (select distinct [Month Start Date]from #Temp_CTE_Date  )a


			order by   a.[Month Start Date]

			  delete from  [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table] where [OrganisationCCG] is null





	
			
		--select * from [#PreReport2]b where b.TEam = 'Abbey Road MEdical Practice' and monthactivity = '2019-11-01'
		--	select 
		--* 
		--from  [#PreReport2]b 
		--where b.TEam = 'Abbey Road MEdical Practice' 
		--and monthactivity = '2019-11-01' 
		--and stafffullname = 'S Sen' 
		--and REport_ActionType = 'Search'
		--order by StaffFullName,REport_ActionType


		--select 
		--* 
		--from  [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table]b 
		--where b.TEam = 'Homerton University Hospital Care of The Elderly Dept' 
		----and actiontime = '2019-11-01' 
		--and stafffullname = 'Sana Mufti'
		----and REport_ActionType = 'Search'
		--order by StaffFullName,REport_ActionType
		--and convert(Date,actionTime)>= '2019-04-01' and  convert(Date,actionTime)<= '2020-05-31'
		--and convert(date,dateadd(d,-(day(actionTime-1)),actionTime)) = [Month Start Date]
 ----select top 500 * from [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table]
 --select distinct team from  [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table] where charindex('homerton',[Parent ORg])>0


update b
		set b.Total = coalesce(c.Total,0),
		b.[Staff ORganisation Start Date] = d.StaffORgStartDate,
		b.[Staff ORganisation End Date] = d.StaffORgEndDate,
		B.STP = E.STP

from [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table]b

left join #PreReport2 c on c.MonthActivity = ActionTime
					and c.[StaffProviderTypeDescription] = b.StaffProviderTypeDescription
					and c.StaffFullName = b.StaffFullName
					and c.TEam = b.Team
					and c.[Parent Org] = b.[Parent Org]
					and c.REport_ActionType = b.REport_ActionType
left join #PreReportLogins d on d.UserFullName = b.StaffFullName
							and d.OrganizationName = b.Team
LEFT JOIN [ETL_Local_PROD].[Reference].[STP]E ON E.CCGLONG_TRUNC = B.OrganisationCCG





  	if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_OrgTeam_Reference]') is not null
			drop table [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_OrgTeam_Reference]
 

   SELECT 
   distinct
   STP,
   [Parent Org],
    Team, 
   (select min(actiontime)from [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table] where team = a.Team and Total > 0 ) as TeamStartDate,
    (select max(actiontime)from [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table] where team = a.Team and Total > 0) as LastTeamAction

   into [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_OrgTeam_Reference]
  FROM [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table]a

 
 --select * from  [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_OrgTeam_Reference] where  TEam = 'Homerton University Hospital Care of The Elderly Dept' 

   alter table [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table] 
   alter column team nvarchar(400)
   
	  CREATE INDEX ActiTeam
ON [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table]  (actiontime, team);


End