USE [ETL_Local_PROD]
GO

/****** Object:  View [dbo].[Cache-AARaw2AuditPatientLoginReference]    Script Date: 12/11/2019 11:35:59 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



 



 --Cache-AARaw2AuditPatientLoginReference]

select 
a.LoginReference,
a.StaffRegistryId,
a.DeptPDRegistryId, 
MIN([PatAuditID]) as Audit,
MIN(ActionTime) as ActionTime, 
'Login' as ActionType 
from [ETL_Local_PROD].[dbo].[AuditPatient_New] a
where LoginReference is not null
group by a.LoginReference,a.StaffRegistryId,a.DeptPDRegistryId;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

 --[dbo].[Cache-AuditLoginDeDup] 

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




--[dbo].[Cache-AuditAuthenticationRaw1]  
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




--[dbo].[Cache-AARaw2Staff]  

select r.*,StaffUserId as ActivityLogUserId from AuditAuthenticationRaw1 r left join Staff s on r.ActivityLogUserRegistryId=s.StaffRegistryId




--[dbo].[Cache-AARaw2Domain] 

select r1.*,l.StaffUserId as LoginStaffUserId, l.LoginStaffRegistryId,l.LoginDeptPDRegistryId from [AARaw2Staff] r1 left join DomainLogins l
on r1.UserLoginId = l.loginid
and r1.Domain = l.Source
where r1.Domain <> '%HS_CC'
and (r1.Domain <> '%HS_Smartcard' or r1.UserLoginId <> 'smartcard')






--[dbo].[Cache-AARaw2LoginAdded] 

select * from AARaw2Domain union all select * from AARaw2DomainTwo union all select * from AARaw2DomainThree

 
--[dbo].[Cache-AuditAuthenticationRaw2] 

select 
distinct 
* 
from AARaw2LoginAdded
join AARaw2AuditPatientLoginReference au
on au.LoginReference = itemid
union all
select distinct l.*,a.* from AARaw2LoginAdded l
left join AARaw2AuditPatientLoginReference au on au.LoginReference = itemid
left join AARaw2LoginAudit a on a.loginreference=l.itemid
where au.Audit is null 


--select top 500 * 
--from auditpatient 


