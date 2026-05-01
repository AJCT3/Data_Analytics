
		DECLARE 
		@EndDate date,  
		@StartDate Date,
		@WeekEnd date, 
		@WeekStart date,
		@totalWeeks int,
		@WeekCounter int = 0,
		@SnapDateStart Date,
		@SnapDateEnd Date


	set @StartDate = dateadd(week,datediff(week,0,convert(date, cast(year(dateadd(month, -3, getdate())) as varchar(10)) + '-04-01', 120)),0)
	set @EndDate =  convert(date, dateadd(week,datediff(week,0,getdate()),6))
	
 
	--set @WeekEnd = EOMONTH(@MonthStart)

 --select dateadd(week,datediff(week,0,getdate()),0)
 --select dateadd(d,-(day(dateadd(m,-1,getdate()-2))),dateadd(m,-1,getdate()-1)) 

	IF OBJECT_ID('tempdb..#Temp_CTE_Date') IS NOT NULL 
	dROP TABLE #Temp_CTE_Date



	;WITH cte AS 
	(
	SELECT 
	DATEPART(Day,@StartDate)as RowNumb,
	CASE WHEN DATEPART(Day,@StartDate) = 1 THEN @StartDate 
				ELSE DATEADD(week,DATEDIFF(week,0,@StartDate)+1,0) END AS myDate
	            
	UNION ALL
	SELECT
	RowNumb+1 as RowNumb,--this is a field to be used with the for loop below
	 DATEADD(week,1,myDate)

	FROM cte
	WHERE DATEADD(week,1,myDate) <=  @EndDate
	)

	--populate temp table with results so they can be used below...
	SELECT RowNumb,myDate
	into #Temp_CTE_Date		
	FROM cte
	OPTION (MAXRECURSION 0)



	--select * from #Temp_CTE_Date

	set @totalWeeks = (select MAX(Rownumb) from #Temp_CTE_Date  ) 

	 
		--The first part gets all patients added to the waiting List within the month													
	WHILE @WeekCounter < @totalWeeks BEGIN
		SET @WeekCounter = @WeekCounter + 1

		set @SnapDateStart = (select myDate from  #Temp_CTE_Date where RowNumb = @WeekCounter)
		--set @SnapDateEnd = DATEADD(d, -1, DATEADD(m, DATEDIFF(m, 0, @SnapDateStart) + 1, 0))

		IF OBJECT_ID('tempdb..#Temp22') IS NOT NULL 
		dROP TABLE #Temp22
		select 
		e.[Financial Year],
		e.Quarter,
		@SnapDateStart as [Report Week],
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
  and CAST(SubmitTime as Date)<= @SnapDateStart

  union

  SELECT 'Initiated and Complete' as ReportName,
  COUNT(*) as Total
  FROM [ETL_PROD].[dbo].[CMC_PersonalCommunityRequest]
  where IsEdit=0
  and Complete=1
and CAST(SubmitTime as Date)<= @SnapDateStart
  Union
  
  SELECT 'Edit and Not Complete' as ReportName,
  COUNT(*) as Total
  FROM [ETL_PROD].[dbo].[CMC_PersonalCommunityRequest]
  where IsEdit=1
  and Complete=0
  and CAST(SubmitTime as Date)<= @SnapDateStart
  Union
  
  
  SELECT 'Edit and Complete' as ReportName,
  COUNT(*) as Total
  FROM [ETL_PROD].[dbo].[CMC_PersonalCommunityRequest]
  where IsEdit=1
  and Complete=1
  and CAST(SubmitTime as Date)<= @SnapDateStart
  Union
  -- All accounts
  
    SELECT 'Account Activated'  as ReportName,
	COUNT(*) as Total
  FROM [ETL_PROD].[dbo].[PC_Acct_User]
  Where CAST([ActivationTime] as Date)<= @SnapDateStart
  Union
 --- Login

SELECT 'New Count Login Total' as ReportName,
COUNT(*) as Total
from ETL_PROD.dbo.PC_HSPortal_Logging_Log
where Name = 'session_start'
and CAST(ServerTimeLogged as Date)<= @SnapDateStart
		)d
		 
	  left join [ETL_Local_PROD].[dbo].[DIM_Date]e on e.[Calendar Day] = @SnapDateStart
	  
 --select * from [ETL_Local_PROD].[dbo].[DIM_Date]

 	insert into [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract] ([Financial Year],Quarter,[Report Week],ReportName,Total)
 select
 *
 from #Temp22 z

  where not exists (SELECT [Report Week] FROM [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]x WHERE x.[Report Week] = z.[Report Week]);
	 



	END


	 delete from [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract] where [Report Week] is null
 --select * FROM [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]where [Report Week] is null