declare 
@FromLoginDate date = '2020-03-01',
@LoginDate date = '2020-04-30',
@CMCApp varchar(30) = '*'



select * from (
select
LoginDate,
LoginTime,
LogoutTime,
UserRegistryID,
isnull(Team,'(Organisation not identifiable at login time)') as Team,
isnull(UserId,'(User Id not identifiable at login time)') + isnull(' (' + case StaffProviderTypeDescription when '' then NULL else StaffProviderTypeDescription end + ')','') as UserId,
case Domain when '%HS_EMIS' then 'EMIS' when '%HS_Smartcard' then 'Smartcard' when '%HS_PC' then 'Portal' else 'CMC' end as Authentication,
LoginId,
ActionType,
case
  when Domain <> '%HS_CC' then ''
  when isMobile = 'TRUE' or (isMobile is null and (OSFamily = 'Android' or DeviceFamily = 'iPhone')) then 'Mobile'
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
) s1
--where @Authentication <> 'CMC' or @CMCApp = '*' or App = @CMCApp
where UserRegistryID = 575202
order by 
case actiontype when 'failed' then 0 when 'login' then 1 when 'direct' then 2 when 'create' then 3 when 'revise' then 4 else 5 end, logindate,authentication,team,userid,loginid



select Service, Team, Match, COUNT(*) as NumberOfCalls,
sum(case when Match = 'Y' then 1 else 0 end) as MatchNumber select top 10 * from dbo.availabilitylog
where  cast(EventDateTime as date) between cast(@FromLoginDate as date) and cast(@LoginDate as date)
group by service,team,match

--select max(eventdateTime) from AT_AvailabilityLog