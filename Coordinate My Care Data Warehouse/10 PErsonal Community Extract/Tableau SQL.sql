select 
a.* 
from    [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]a

inner join
			(
			select
			[Report Week],
			max([Report Day]) as LastDay
			from    [ETL_Local_PROD].[dbo].[AT_Perssonal_Community_Extract]
			where Total is not null
			group by [Report Week]	
			)b on b.[Report Week] = a.[Report Week]
				and b.LastDay = a.[Report Day]
order by [Report Week] ,reportName 

 