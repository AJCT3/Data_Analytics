

declare @StartDate Date

--set @StartDate = convert(datetime, cast(year(dateadd(month, -3, getdate())) as varchar(10)) + '-04-01', 120)
set @StartDate = '2020-03-23'

select 
1 as RepOrder,
'myCMC Initiated, Submitted, Not Approved' as [ReportName],

(select max(a.[Report Day]) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.Total is not null) as [Report Day],
[Report Week],
(select max(a.Total) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.ReportName = x.ReportName and a.Total is not null) as Total
from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]x
where [report week] >= @StartDate

and [ReportName] = 'Initiated and Not Complete'
group by

[Report Week],ReportName

union all

select 
2 as RepOrder,
'myCMC Initiated, Submitted, Approved' as [ReportName],

(select max(a.[Report Day]) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.Total is not null) as [Report Day],
[Report Week],
(select max(a.Total) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.ReportName = x.ReportName and a.Total is not null) as Total
from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]x
where [report week] >= @StartDate

and [ReportName] = 'Initiated and Complete'
group by

[Report Week],ReportName

union all

 
 
select 
3 as RepOrder,
'Total Initiated' as [ReportName],

(select max(a.[Report Day]) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.Total is not null) as [Report Day],
[Report Week],
(
(select  max(a.Total) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.ReportName = 'Initiated and Not Complete' and a.Total is not null) 
+ (select  max(a.Total) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.ReportName = 'Initiated and Complete' and a.Total is not null) 
)
as Total
from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]x
where [report week] >= @StartDate
 
group by

[Report Week] 

union all

select 
4 as RepOrder,
'myCMC Patient Change Request, Submitted, Not Approved' as [ReportName],

(select max(a.[Report Day]) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.Total is not null) as [Report Day],
[Report Week],
(select max(a.Total) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.ReportName = x.ReportName and a.Total is not null) as Total
from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]x
where [report week] >= @StartDate

and [ReportName] = 'Edit and Not Complete'
group by

[Report Week],ReportName


union all

select 
5 as RepOrder,
'myCMC Patient Change Request, Submitted, Approved' as [ReportName],

(select max(a.[Report Day]) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.Total is not null) as [Report Day],
[Report Week],
(select max(a.Total) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.ReportName = x.ReportName and a.Total is not null) as Total
from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]x
where [report week] >= @StartDate

and [ReportName] = 'Edit and Complete'
group by

[Report Week],ReportName


union all

select 
6 as RepOrder,
'Total Edits' as [ReportName],

(select max(a.[Report Day]) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.Total is not null) as [Report Day],
[Report Week],
(
(select  max(a.Total) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.ReportName = 'Edit and Not Complete' and a.Total is not null) 
+ (select  max(a.Total) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.ReportName = 'Edit and Complete' and a.Total is not null) 
)
as Total
from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]x
where [report week] >= @StartDate
 
group by

[Report Week] 


union all

select 
7 as RepOrder,
'myCMC User Accounts Activated to Access Approved Care Plan' as [ReportName],

(select max(a.[Report Day]) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.Total is not null) as [Report Day],
[Report Week],
(select max(a.Total) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.ReportName = x.ReportName and a.Total is not null) as Total
from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]x
where [report week] >= @StartDate

and [ReportName] = 'Account Activated'
group by

[Report Week],ReportName
 


union all

select 
8 as RepOrder,
'Patient & Proxy myCMC Logins to Access Approved Care Plan' as [ReportName],

(select max(a.[Report Day]) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.Total is not null) as [Report Day],
[Report Week],
(select max(a.Total) from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a where a.[Report Week] = x.[Report Week] and a.ReportName = x.ReportName and a.Total is not null) as Total
from   [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]x
where [report week] >= @StartDate

and [ReportName] = 'New Count Login Total'
group by

[Report Week],ReportName



--select * from [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract] where [Report Week] = '2020-04-06'and [ReportName] = 'Initiated and Not Complete'