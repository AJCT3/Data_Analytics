
	INSERT INTO [Informatics_Reporting].[etl].[ExtractHistory](StartDate,Order_Of_Change,LoadTime,EndDate)
SELECT GETDATE(), NULL, Null,Null



if OBJECT_ID('tempdb..#TempHistory') is Not Null
 drop table #TempHistory
  select
  ExtractHistoryId, 
  StartDate,
  RANK() OVER(PARTITION BY Convert(date,StartDate) ORDER BY [StartDate]) as Order_Of_Change,
  convert(time(0),FORMAT(StartDate, 'HH:mm') )   as LoadTime,
  EndDate
  into #TempHistory
  from [Informatics_Reporting].[etl].[ExtractHistory]

drop table [Informatics_Reporting].[etl].[ExtractHistory]

select
*
into [Informatics_Reporting].[etl].[ExtractHistory]
from #TempHistory
