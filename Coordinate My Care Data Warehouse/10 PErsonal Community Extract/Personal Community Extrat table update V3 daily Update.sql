USE [ETL_Local_PROD]
GO
/****** Object:  StoredProcedure [dbo].[AT_Loging_and_Activity_Data]    Script Date: 14/04/2020 10:11:49 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





alter PROCEDURE [dbo].[AT_MyCMC_Community_Extract] 
-- Amended for PD Upgrade
AS
BEGIN

		DECLARE 
		@EndDate date,  
		@StartDate Date,
		@WeekEnd date,
		@ReportDate date,
		@WeekStart date,
		@totalWeeks int,
		@WeekCounter int = 0,
		@SnapDateStart Date,
		@SnapDateEnd Date

		
	set @StartDate = (select min([report day]) from [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract] where [Report week] = (select max([Report week]) from [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract] ))
	--(select min(submitTime) from [ETL_PROD].[dbo].[CMC_PersonalCommunityRequest])
	--dateadd(week,datediff(week,0,
	--dateadd(week,datediff(week,0, convert(date, cast(year(dateadd(month, -3, getdate())) as varchar(10)) + '-04-01', 120)),0)
	--),0)
	set @EndDate =  convert(date, dateadd(week,datediff(week,0,getdate()),6))
	print @StartDate
	Print @EndDate
 
	--set @WeekEnd = EOMONTH(@MonthStart)
	 
 --select dateadd(week,datediff(week,0,getdate()),0)
 --select dateadd(d,-(day(dateadd(m,-1,getdate()-2))),dateadd(m,-1,getdate()-1)) 

	IF OBJECT_ID('tempdb..#Temp_CTE_Date') IS NOT NULL 
	dROP TABLE #Temp_CTE_Date



	;WITH cte AS 
	(
	SELECT   [ID]
	,ROW_NUMBER() OVER(ORDER by ID ASC) AS RowNumb
	,DEnse_RANK() OVER  ( ORDER BY [Week Of Year] ASC)  AS RowNumbWeek
	,convert(date,dateadd(week,datediff(week,0,[Calendar Day]),0)) as WeekStart
	,convert(date,dateadd(week,datediff(week,0,[Calendar Day]),6)) as WeekEnd
      ,[Calendar Day] as MyDate
      ,[Week Day]
      ,[Day of Month]
      ,[DaySuffix]
      ,[Day Of Week]
      ,[Day Of Year]
      ,[Week Of Year]
      ,[Week Of Month]
      ,[Month]
      ,[Month Name]
      ,[Month Start Date]
      ,[Month End Date]
      ,[Year Start Date]
      ,[Year End Date]
      ,[Quarter]
      ,[Quarter Name]
      ,[Year]
      ,[Financial Year]
      ,[Month Start Date Flag]
      ,[Month End Date Flag]
      ,[Fin Year Start Date Flag]
      ,[Fin Year End Date Flag]
      ,[Weekend Flag]
      ,[PK_DateKey]
  FROM [ETL_Local_PROD].[dbo].[DIM_Date]
  where [Calendar Day] >= @StartDate and [Calendar Day] < @EndDate
	)

	--populate temp table with results so they can be used below...
	SELECT *
	into #Temp_CTE_Date		
	FROM cte
	OPTION (MAXRECURSION 0)



	--select * from #Temp_CTE_Date

	set @totalWeeks = (select MAX(Rownumb) from #Temp_CTE_Date  ) 

	 
		--The first part gets all patients added to the waiting List within the month													
	WHILE @WeekCounter < @totalWeeks BEGIN
		SET @WeekCounter = @WeekCounter + 1
		set @ReportDate = (select MyDate from  #Temp_CTE_Date where RowNumb = @WeekCounter)
		set @SnapDateStart = (select WeekStart from  #Temp_CTE_Date where RowNumb = @WeekCounter)
		--set @SnapDateEnd = DATEADD(d, -1, DATEADD(m, DATEDIFF(m, 0, @SnapDateStart) + 1, 0))

		IF OBJECT_ID('tempdb..#Temp22') IS NOT NULL 
		dROP TABLE #Temp22
		select 
		e.[Financial Year],
		e.Quarter,
		@SnapDateStart as [Report Week],
		@ReportDate as [Report Day],
		d.*
		into
		#Temp22
		from
		(
		
  SELECT 'Initiated and Not Complete' as ReportName,
   COUNT(*) as Total
  FROM [ETL_PROD].[dbo].[CMC_PersonalCommunityRequest]
  where IsEdit=0
  and Complete=0
  and CAST(SubmitTime as Date)<= @ReportDate

  union

  SELECT 'Initiated and Complete' as ReportName,
  COUNT(*) as Total
  FROM [ETL_PROD].[dbo].[CMC_PersonalCommunityRequest]
  where IsEdit=0
  and Complete=1
and CAST(SubmitTime as Date)<= @ReportDate
  Union
  
  SELECT 'Edit and Not Complete' as ReportName,
  COUNT(*) as Total
  FROM [ETL_PROD].[dbo].[CMC_PersonalCommunityRequest]
  where IsEdit=1
  and Complete=0
  and CAST(SubmitTime as Date)<= @ReportDate
  Union
  
  
  SELECT 'Edit and Complete' as ReportName,
  COUNT(*) as Total
  FROM [ETL_PROD].[dbo].[CMC_PersonalCommunityRequest]
  where IsEdit=1
  and Complete=1
  and CAST(SubmitTime as Date)<= @ReportDate
  Union
  -- All accounts
  
    SELECT 'Account Activated'  as ReportName,
	COUNT(*) as Total
  FROM [ETL_PROD].[dbo].[PC_Acct_User]
  Where CAST([ActivationTime] as Date)<= @ReportDate
  Union
 --- Login

SELECT 'New Count Login Total' as ReportName,
COUNT(*) as Total
from ETL_PROD.dbo.PC_HSPortal_Logging_Log
where Name = 'session_start'
and CAST(ServerTimeLogged as Date)<= @ReportDate
		)d
		 
	  left join [ETL_Local_PROD].[dbo].[DIM_Date]e on e.[Calendar Day] = @ReportDate
	  
 --select * from [ETL_Local_PROD].[dbo].[DIM_Date]

 	insert into [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract] ([Financial Year],Quarter,[Report Week],[Report Day], ReportName,Total)
 select
 *
 from #Temp22 z

  where not exists (SELECT [Report Week] FROM [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]x WHERE x.[Report Week] = z.[Report Week] and x.[Report Day] = z.[Report Day]);
	 



	END


	 delete from [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract] where [Report Week] is null
	 update r 
	 
		set r.Total = null
	 
	 from [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract] r
	 where [Report Day] >= dateadd(day,-1,getdate())


 --select * FROM [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]where [Report Week] is null
 --select * into [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract_Back_Up] FROM [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]
 --truncate table [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract] 
-- ALTER TABLE [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract] 
--ADD [Report Day] date;

end