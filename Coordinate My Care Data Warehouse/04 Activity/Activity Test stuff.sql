
--Statistics

declare @FromLoginDate date,@LoginDate date,@Authentication NvARCHAR(400),@CMCApp NVARCHAR(10)

set @FromLoginDate = '2019-10-01'
SET @LoginDate = '2019-10-31'
sET @Authentication = '*' 
SET @CMCApp = '*' 

--select * from (
select
LoginDate,
LoginTime,
LogoutTime,
isnull(Team,'(Organisation not identifiable at login time)') as Team,
isnull(UserId,'(User Id not identifiable at login time)') + isnull(' (' + case StaffProviderTypeDescription when '' then NULL else StaffProviderTypeDescription end + ')','') as UserId,
case Domain when '%HS_EMIS' then 'EMIS' when '%HS_Smartcard' then 'Smartcard' when '%HS_PC' then 'Portal' else 'CMC' end as Authentication,
LoginId,
ActionType,
case
  when Domain <> '%HS_CC' then ''
  when (isMobile = 'TRUE'or isMobile = 1) or (isMobile is null and (OSFamily = 'Android' or DeviceFamily = 'iPhone')) then 'Mobile'
  when left(loginid,6) ='System' then 'Auto-Flagging'
  else 'Desktop' end as App
from (

 

select * from AuditPatientAuthentication
where ActionType in ('login','direct','view','create','revise','failed')
and LoginDate between cast(@FromLoginDate as date) and cast(@LoginDate as date)
--and case Domain when '%HS_EMIS' then 'EMIS' when '%HS_PC' then 'Portal' when '%HS_Smartcard' then 'Smartcard' else 'CMC' end = @Authentication

) ap
left join PDDepartment on OrganizationRegistryId = DeptPDRegistryId
left join reporting.DisambiguatedActivityTeams on ActivityDepartmentId = DeptEnterpriseId
left join Staff on UserRegistryId = StaffRegistryId
left join Load.Devices d on ap.UserAgent = d.UserAgent

where team = 'LONDON AMBULANCE SERVICE NHS TRUST, RRU'

--) s1
--where
-- @Authentication <> 'CMC' or @CMCApp = '*' or App = @CMCApp

--order by 
--case actiontype when 'failed' then 0 when 'login' then 1 when 'direct' then 2 when 'create' then 3 when 'revise' then 4 else 5 end, logindate,authentication,team,userid,loginid


--Availability


select Service, Team, Match, COUNT(*) as NumberOfCalls,
sum(case when Match = 'Y' then 1 else 0 end) as MatchNumber from dbo.availabilitylog
where service=@Authentication and cast(EventDateTime as date) between cast(@FromLoginDate as date) and cast(@LoginDate as date)
group by service,team,match




select top 50 * from [ETL_Local_PROD].[dbo].[AT_AccessDataDetail] where charindex('LONDON AMBULANCE SERVICE',name)>0

select  * from [ETL_Local_PROD].[dbo].[AT_AccessDataDetail] where cmc_ID = 100053758 order by ActivityDate

select
Distinct  
[User Name],
convert(Date,ActivityDate) as ActivityDate,
--cmc_id
Count(distinct cmc_id) as [Total Views]
from [ETL_Local_PROD].[dbo].[AT_AccessDataDetail] 
Where TeamType = 'Ambulance Trust' 
and ActivityDate >= '2019-07-01' and ActivityDate <= '2019-07-31'  
and [Access Type] = 'custom-view'   
 --and [User Name] = 'Chelsey Pike'  
group by

[User Name],
convert(Date,ActivityDate)


Select
*

from [ETL_Local_PROD].[dbo].[AT_AccessDataDetail] 
Where TeamType = 'Ambulance Trust' 
and ActivityDate >= '2019-07-01' and ActivityDate <= '2019-07-31'  
--and [Access Type] = 'custom-view'   
 --and [User Name] = 'David Watts'  
 and cmc_id = 100048503
 order by ActivityDate



 select * from [ETL_Local_PROD].[dbo].[AuditPatient_New]where  CMC_ID = 100048503 and convert(date,ActionTime) = '2019-07-03' order by OverAllOrder
