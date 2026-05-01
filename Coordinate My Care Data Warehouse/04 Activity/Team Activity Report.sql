

select
*from[ETL_Local_PROD].[Reference].[STP]  
where stp = 'North West London'



--TEam Activity Report

select * from reporting.disambiguatedactivityteams where charindex('Middlesex',team) > 0 order by team


select
[Year],
[Month],
[Activity Month Name],
sum(case when [Access Type] like 'create%' then 1 else 0 end) as Created,
sum(case when [Access Type] like 'revise%'  then 1 else 0 end) as Updated,
sum(case when [Access Type] like 'view%' then 1 else 0 end) as Viewed,
sum(case when [Access Type] like 'print%' then 1 else 0 end) as Printed
from Reporting.TeamAudit a
where ActivityEnterpriseId = @Team
group by [Year], [Month], [Activity Month Name]
order by [Year], [Month], [Activity Month Name]