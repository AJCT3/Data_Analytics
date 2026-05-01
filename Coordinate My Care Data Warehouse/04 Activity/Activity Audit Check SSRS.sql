USE [ETL_Local_PROD]
GO
 
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




GO



		  select top 5* from [ETL_PROD].[dbo].[CMC_AuditData] where ActionTime >= '2019-12-01' and actiontype = 'view'
		  select top 5* from [ETL_PROD].[dbo].[CMC_ActivityLog] where LoginTime  >= '2019-12-01'
		  select top 5* from [ETL_PROD].[dbo].[CMC_AuditLogin]
		  
		  
		    select top 5* from [ETL_PROD].[dbo].[CMC_ActivityLog] where LoginRowId = 1305382
		    select * from [ETL_PROD].[dbo].[CMC_AuditData] where LoginRowId = 1305382





