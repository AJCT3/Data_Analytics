
----Statistics

select * from (
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
  when isMobile = 'TRUE' or (isMobile is null and (OSFamily = 'Android' or DeviceFamily = 'iPhone')) then 'Mobile'
  when left(loginid,6) ='System' then 'Auto-Flagging'
  else 'Desktop' end as App
from (
select * from AuditPatientAuthentication
where ActionType in ('login','direct','view','create','revise','failed')
and LoginDate between '2019-04-01' and '2019-11-30'
--and case Domain when '%HS_EMIS' then 'EMIS' when '%HS_PC' then 'Portal' when '%HS_Smartcard' then 'Smartcard' else 'CMC' end = @Authentication
) ap
left join PDDepartment on OrganizationRegistryId = DeptPDRegistryId
left join reporting.DisambiguatedActivityTeams on ActivityDepartmentId = DeptEnterpriseId
left join Staff on UserRegistryId = StaffRegistryId
left join Load.Devices d on ap.UserAgent = d.UserAgent
) s1
--where @Authentication <> 'CMC' or @CMCApp = '*' or App = @CMCApp
where charindex('LONDON AMBULANCE SERVICE',Team )>0
order by 
case actiontype when 'failed' then 0 when 'login' then 1 when 'direct' then 2 when 'create' then 3 when 'revise' then 4 else 5 end, logindate,authentication,team,userid,loginid


--AuthenticationChoice
select distinct domain,
case domain
when '%HS_EMIS' then 'EMIS'
when '%HS_Smartcard' then 'Smartcard'
when '%HS_PC' then 'Portal'
else 'CMC' end as AuthenticationChoice from auditauthentication order by domain


--Availability
select Service, Team, Match, COUNT(*) as NumberOfCalls,
sum(case when Match = 'Y' then 1 else 0 end) as MatchNumber from dbo.availabilitylog
--where service=@Authentication and cast(EventDateTime as date) between cast(@FromLoginDate as date) and cast(@LoginDate as date)
group by service,team,match





--CMC App
select '*' as App
union all
select distinct
case
   when OSFamily = 'Android' or DeviceFamily = 'iPhone' then 'Mobile'
   when left(loginid,6) ='System' then 'Auto-Flagging'
   else 'Desktop' end as App
from AuditAuthentication aa
left join load.devices ld on aa.useragent = ld.useragent
where  @Authentication='CMC'









select top 100 * from PDDepartment
select top 100000 * from Ind2Org
 

 select * from Protocol.ODSReconciliationCandidates

 select top 5 * from [ETL_Local_PROD].[dbo].[AT_Commissioner_Report_Worksheet_4]

 select top 5 * from [ODSDataCurrent].[odsdata].[Cache-SearchODS]




 
SELECT  *    FROM [ODSDataCurrent].[odsdata].[Cache-SearchODS] where (charindex('Sutton',[Team or GP] )>0   and charindex('community',[Team or GP] )>0  )
--and [GP CCG] = 'LONDON GOVERNMENT OFFICE REGION' 
order by ParentTeam,[Team or GP]
  --select distinct type from [ODSDataCurrent].[odsdata].[Cache-SearchODS]
  SELECT  *    FROM [ODSDataCurrent].[odsdata].[Cache-SearchODS] where type = 'GP' and [GP CCG] = 'LONDON GOVERNMENT OFFICE REGION'




