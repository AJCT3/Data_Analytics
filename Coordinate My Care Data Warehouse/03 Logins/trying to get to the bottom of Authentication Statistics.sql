USE [ETL_Local_PROD]
GO
 



--ALTER view [dbo].[Cache-AuditPatientAuthentication] as


with patientaudit as
(select * from AuditPatient where LoginReference is not null
union all
select CMC_ID,FromPatientSummary,ToPatientSummary,Audit,ActionTime,ActionType,StaffRegistryId,DeptPDRegistryId,Role,aa.loginitemid as loginreference
from AuditAuthentication aa join AuditPatient ap
on ap.StaffRegistryId=aa.UserRegistryId and ap.DeptPDRegistryId=aa.OrganizationRegistryId and ap.LoginReference is null and ap.ActionTime between aa.LoginTime and aa.LogoutTime)

select UserId, UserRegistryID, OrganizationRegistryID, LoginTime, LogoutTime, UserAgent, Browser, BrowserVersion, isMobile, BrowserMode,
CspSessionId, Domain, LoginId, cast(LoginTime as date) as LoginDate,
isnull(Audit,'NoAudit'+LoginItemId) as Audit, LoginItemId, replace(ActionType,'custom-','') as ActionType from
(select
aa.[UserId],
aa.[UserRegistryId],
aa.[OrganizationRegistryId],
aa.[UserAgent],
aa.[Browser],
aa.[BrowserMode],
aa.[BrowserVersion],
-- add isMobile flag MS 21.1.17
isMobile,
aa.[CspSessionId],
aa.[Domain],
aa.[LoginId],
aa.AuthenticationTime as LoginTime,
aa.[LogoutTime],
ap.Audit,
aa.LoginItemId,
ActionType,
ROW_NUMBER() over (partition by audit,domain order by logintime) as rn
from AuditAuthentication aa join patientaudit ap
-- use direct audit lookup MS 16.3.17
on ap.LoginReference = aa.LoginItemId 
union all
select
a.[UserId],
a.[UserRegistryId],
case when Success=1 or domain <> '%HS_EMIS' then a.[OrganizationRegistryId]
  else case when charindex('^',a.loginid)=0 then NULL else LEFT(a.loginid,charindex('^',loginid)-1) end
  end as OrganizationRegistryId,
a.[UserAgent],
a.[Browser],
a.[BrowserMode],
a.[BrowserVersion],
isMobile,
a.[CspSessionId],
a.[Domain],
a.[LoginId],
a.AuthenticationTime as LoginTime,
a.[LogoutTime],
a.[LoginAuditId],
a.LoginItemId,
case
-- 16.3.17 MS success/failure now goes in here
-- need to look up EMIS organisation on failures [also wherever there is a single ind/org / no activity log entry, EMIS / auto-flagging]
  when Success = 0 then 'failed'
-- this works for now, we'll have to think of something later .....
  when domain = '%HS_EMIS' and (browser is null or loginstatus = 'direct') then 'direct'
  when domain = '%HS_Smartcard' and userid is not null then 'direct'
  else 'login' end as ActionType,1 as rn
-- 30.6.16 MS new direct handling
  from AuditAuthentication a
  join AuditIdentifyDirect d on a.CspSessionId = d.cspsessionid
) sel1
where rn=1

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

 --ALTER view [dbo].[Cache-AuditAuthentication] as

select loginstaffUserId as UserId,LoginstaffRegistryId as UserRegistryId,LoginDeptPDRegistryID as OrganizationRegistryId,
UserAgent,Browser,BrowserMode,BrowserVersion,isMobile,CspSessionId,Domain,LoginId,LoginTime,
isnull(case
  when loginstaffUserId is null then LogoutTime
  when LogoutTime is null then
    (select top 1 next.logouttime
      from auditloginsequence next
      where next.loginstaffuserid=ls.loginstaffuserid
        and next.logindate=ls.logindate
        and next.rn>ls.rn
        and next.LogoutTime is not null
        order by next.rn)
  else LogoutTime end,dateadd(second,-1,dateadd(day,1,cast(logindate as datetime)))) as LogoutTime,
AuditItemId as LoginAuditId, LoginItemId, Success, Reason, AuthenticationTime
from auditloginsequence ls



----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


--ALTER view [dbo].[Cache-AuditLoginSequence] as


select *,case when loginstaffuserid is null then 1 else ROW_NUMBER() over (partition by loginstaffuserid,logindate order by logintime) end as rn from [AuditAuthenticationRaw]



----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--ALTER view [dbo].[Cache-AuditAuthenticationRaw] as



with cleanup as
(select
isnull(LoginStaffUserId,ISNULL(ActivityLogUserId,StaffUserId)) as LoginStaffUserId,
isnull(LoginStaffRegistryId,ISNULL(ActivityLogUserRegistryID,a.StaffRegistryId)) as LoginStaffRegistryId,
isnull(LoginDeptPDRegistryId,ISNULL(ActivityLogOrganizationRegistryID,DeptPDRegistryId)) as LoginDeptPDRegistryId,
UserAgent,Browser,BrowserMode,BrowserVersion,isMobile,
isnull(CspSessionId,ActivityLogSessionId) as CSPSessionId,
Domain,UserLoginId as LoginId,Success,Reason,
LastLogin as AuthenticationTime,
cast(lastlogin as date) as AuthenticationDate,
ActivityLoginTime as LoginTime,
cast(ActivityLoginTime AS date) as LoginDate,
ActivityLogoutTime as LogoutTime,
Audit as AuditItemId,ActivityLogItemId,ItemId as LoginItemId
from 
AuditAuthenticationRaw2 a left join Staff s on a.staffregistryid = s.StaffRegistryId)

select
cast(LoginStaffUserId as varchar(255)) as LoginStaffUserId,loginstaffregistryid,logindeptpdregistryid,
UserAgent,Browser,BrowserMode,BrowserVersion,
isMobile,CspSessionId,Domain,LoginId,Success,Reason,
MIN(authenticationtime) as authenticationTime,
MIN(authenticationdate) as authenticationdate,
Min(LoginTime) as LoginTime,
MIN(LoginDate) as LoginDate,
MAX(LogoutTime) as LogoutTime,
min(AuditItemId) as AuditItemId, MIN(ActivityLogItemId) as ActivityLogItemId, MIN(LoginItemId) as LoginItemId
from cleanup
group by
LoginStaffUserId,loginstaffregistryid,logindeptpdregistryid,
UserAgent,Browser,BrowserMode,BrowserVersion,
isMobile,CspSessionId,Domain,LoginId,Success,Reason

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--ALTER view [dbo].[Cache-AuditAuthenticationRaw2] as



select distinct * from AARaw2LoginAdded
join AARaw2AuditPatientLoginReference au
on au.LoginReference = itemid
union all
select distinct l.*,a.* from AARaw2LoginAdded l
left join AARaw2AuditPatientLoginReference au on au.LoginReference = itemid
left join AARaw2LoginAudit a on a.loginreference=l.itemid
where au.Audit is null 




--ALTER view [dbo].[Cache-AARaw2LoginAdded] as

select * from AARaw2Domain union all select * from AARaw2DomainTwo union all select * from AARaw2DomainThree


--ALTER view [dbo].[Cache-AARaw2Domain] as

select r1.*,l.StaffUserId as LoginStaffUserId, l.LoginStaffRegistryId,l.LoginDeptPDRegistryId from [AARaw2Staff] r1 left join DomainLogins l
on r1.UserLoginId = l.loginid
and r1.Domain = l.Source
where r1.Domain <> '%HS_CC'
and (r1.Domain <> '%HS_Smartcard' or r1.UserLoginId <> 'smartcard')



--ALTER view [dbo].[Cache-AARaw2DomainTwo] as

select r1.*,l.StaffUserId as LoginStaffUserId, l.LoginStaffRegistryId,l.LoginDeptPDRegistryId from AARaw2Staff r1 left join DomainLogins l
on r1.UserLoginId = l.staffuserid
and r1.Domain = l.Source
where r1.Domain = '%HS_CC'



--ALTER view [dbo].[Cache-AARaw2Staff] as

select r.*,StaffUserId as ActivityLogUserId from AuditAuthenticationRaw1 r left join Staff s on r.ActivityLogUserRegistryId=s.StaffRegistryId


--ALTER view [dbo].[Cache-AuditAuthenticationRaw1] as


-- MS 13.3.17 ensure sessions with multi different logins are handled correctly

with activitylog as (select
a.SessionID as ActivityLogSessionId,
a.Browser,a.BrowserMode,a.BrowserVersion,a.UserAgent,
-- add isMobile flag MS 21.1.17
isMobile,
a.LoginTime as ActivityLoginTime,a.LogoutTime as ActivityLogoutTime,
a.UserRegistryId as ActivityLogUserRegistryId,
a.OrganizationRegistryID as ActivityLogOrganizationRegistryId,
LoginRowId,ItemId as ActivityLogItemId
from ETL_PROD.dbo.CMC_ActivityLog a),
matched as
(select la.* from etl_prod.dbo.cmc_auditlogin la
join activitylog al on al.loginrowid is not null and al.loginrowid = la.itemid)
select la.*,al.* from matched la
join activitylog al on al.loginrowid is not null and al.loginrowid = la.itemid
union all
select la.*,al.* from (select * from etl_prod.dbo.cmc_auditlogin except select * from matched) la
left join AuditLoginDeDup ld on la.ItemId = ld.AuditLoginItemId
left join activitylog al on al.ActivityLogItemId = ld.ActivityLogItemId


--ALTER view [dbo].[Cache-AuditLoginDeDup] as 
-- Cross-reference Activity Log to make sure each of its entries points to the right AuditLogin record. Omit anything with a proper login reference.
with ActivityLog as (select * from etl_prod.dbo.CMC_ActivityLog where LoginRowId is null),
Matches as
(select a.ItemId as ActivityLogItemId, l.ItemId as AuditLoginItemId, UserID, UserLoginId, LastLogin, logintime, sessionid,
ROW_NUMBER() over (partition by a.itemid order by abs(datediff(second,lastlogin,logintime))) as rn
from ActivityLog a
join DomainLogins d on a.UserRegistryID = d.LoginStaffRegistryId
join etl_prod.dbo.CMC_AuditLogin l 
on d.LoginId = l.UserLoginId and a.SessionID = l.cspsessionid
where LastLogin <= logintime)
select ActivityLogItemId,AuditLoginItemId from Matches where rn=1



--ALTER view [dbo].[Cache-DomainLogins] as


with singles as
(select io.StaffuserID,iodeptpdregistryid as DeptPDRegistryId from
(select * from (select staffenterpriseid,staffuserid,COUNT(*) as num from staffdepartmentall group by staffenterpriseid,staffuserid) sel1 where num=1) sel2
join staffdepartmentall io on sel2.StaffEnterpriseID=io.StaffEnterpriseID)
select Source,StaffUserId,LoginId,LoginStaffRegistryId,LoginDeptPDRegistryId from
(select '%HS_EMIS' as Source,se.StaffUserId,emis as LoginId,EMISStaffRegistryId as LoginStaffRegistryId,
EMISDeptPDRegistryId as LoginDeptPDRegistryId, 
ROW_NUMBER() over (partition by emis order by emis) as lrn
from StaffEMIS se
union all
select '%HS_Smartcard' as Source,se.StaffUserId,smartcard as LoginId,SmartcardStaffRegistryId as LoginStaffRegistryId,
s.DeptPDRegistryID as LoginDeptPDRegistryId,
ROW_NUMBER() over (partition by smartcard order by smartcard) as lrn
from StaffSmartcards se left join singles s on se.staffuserid=s.staffuserid
union all
select '%HS_CC' as Source,se.StaffUserId,LoginId,LoginIdStaffRegistryId,
s.DeptPDRegistryID as LoginDeptPDRegistryId,
ROW_NUMBER() over (partition by loginid order by loginid) as lrn
from StaffLoginIds se left join singles s on se.staffuserid=s.staffuserid
union all
select distinct '%HS_CC' as Source, StaffUserId, StaffUserId, IOStaffRegistryId as LoginStaffRegistryId,IODeptPDRegistryId as LoginDeptPDRegistryId, 1 as lrn from StaffDepartmentAll where StaffUserId like 'System%'
) sel1
where lrn=1

-- add dept MS 14.3.17
--ALTER view [dbo].[Cache-DomainLogins] as


with singles as
(select io.StaffuserID,iodeptpdregistryid as DeptPDRegistryId from
(select * from (select staffenterpriseid,staffuserid,COUNT(*) as num from staffdepartmentall group by staffenterpriseid,staffuserid) sel1 where num=1) sel2
join staffdepartmentall io on sel2.StaffEnterpriseID=io.StaffEnterpriseID)
select Source,StaffUserId,LoginId,LoginStaffRegistryId,LoginDeptPDRegistryId from
(select '%HS_EMIS' as Source,se.StaffUserId,emis as LoginId,EMISStaffRegistryId as LoginStaffRegistryId,
EMISDeptPDRegistryId as LoginDeptPDRegistryId, 
ROW_NUMBER() over (partition by emis order by emis) as lrn
from StaffEMIS se
union all
select '%HS_Smartcard' as Source,se.StaffUserId,smartcard as LoginId,SmartcardStaffRegistryId as LoginStaffRegistryId,
s.DeptPDRegistryID as LoginDeptPDRegistryId,
ROW_NUMBER() over (partition by smartcard order by smartcard) as lrn
from StaffSmartcards se left join singles s on se.staffuserid=s.staffuserid
union all
select '%HS_CC' as Source,se.StaffUserId,LoginId,LoginIdStaffRegistryId,
s.DeptPDRegistryID as LoginDeptPDRegistryId,
ROW_NUMBER() over (partition by loginid order by loginid) as lrn
from StaffLoginIds se left join singles s on se.staffuserid=s.staffuserid
union all
select distinct '%HS_CC' as Source, StaffUserId, StaffUserId, IOStaffRegistryId as LoginStaffRegistryId,IODeptPDRegistryId as LoginDeptPDRegistryId, 1 as lrn from StaffDepartmentAll where StaffUserId like 'System%'
) sel1
where lrn=1


----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------