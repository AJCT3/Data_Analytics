USE [ETL_Local_PROD]
GO

--- Update the pointers
--- For incremental builds
--- If FULL build is required set pointers to ZERO

delete from auditdata_DOC_count
INSERT INTO auditdata_DOC_count 
Select TOP 1 CAST(SUBSTRING(ItemId,4,Len(ItemId)) as numeric) FROM ETL_PROD.dbo.CMC_AuditData
Where SUBSTRING(ItemId,1,3)='Doc' 
ORDER BY CAST(SUBSTRING(ItemId,4,Len(ItemId)) as numeric) DESC

delete from auditdata_REG_count
INSERT INTO auditdata_REG_count 
Select TOP 1 CAST(SUBSTRING(ItemId,9,Len(ItemId)) as numeric) FROM ETL_PROD.dbo.CMC_AuditData
Where SUBSTRING(ItemId,1,3)='Reg' 
ORDER BY CAST(SUBSTRING(ItemId,9,Len(ItemId)) as numeric) DESC



  --[Cache-ServiceSearch_Incremental] 
select *, 
rtrim(REPLACE(Actor,'EMISAvail','')) as ODS
from ServiceSearch1
where Actor like 'EMISAvail%'
and CAST(SUBSTRING(ItemId,9,Len(ItemId)) as numeric)>(select regcount from auditdata_REG_count)


--[Cache-ServiceSearch] 
select *, 
rtrim(REPLACE(Actor,'EMISAvail','')) as ODS
from ServiceSearch1
where Actor like 'EMISAvail%'


--[Cache-ServiceSearch1_Incremental] 
select ItemId,ActionTime,MRNs,Actor,REGISTRYRoles
-- add stripped actor for performance MS 21.5.16
-- rtrim(REPLACE(Actor,'EMISAvail','')) as ODS
from etl_PROD.dbo.cmc_auditdata
where ActionType = 'SearchPatient'
and REGISTRYRoles like '%HSCC_Service_Search%'
and CAST(SUBSTRING(ItemId,9,Len(ItemId)) as numeric)>(select regcount from auditdata_REG_count)



--[Cache-ServiceSearch1] 
select ItemId,ActionTime,MRNs,Actor,REGISTRYRoles
-- add stripped actor for performance MS 21.5.16
-- rtrim(REPLACE(Actor,'EMISAvail','')) as ODS
from etl_PROD.dbo.cmc_auditdata
where ActionType = 'SearchPatient'
and REGISTRYRoles like '%HSCC_Service_Search%'

 
-------------------------------------------------------------------------------------------------oOo------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------oOo--oOo--oOo----------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------oOo--oOo--oOo--oOo--oOo-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------oOo--oOo--oOo--oOo--oOo--oOo--oOo---------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------oOo--oOo--oOo--oOo--oOo--oOo--oOo--oOo--oOo----------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------oOo--oOo--oOo--oOo--oOo--oOo--oOo--------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------oOo--oOo--oOo--oOo--oOo-----------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------oOo--oOo--oOo----------------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------oOo---------------------------------------------------------------------------------------------------------------------------------------------------------------

--[Cache-AvailabilityLog]
select ItemId as id,
cast(actiontime as datetime) as EventDateTime,
cast(actiontime as date) as EventDate,
case when MRNs='0' then 'N' else 'Y' end as match,
'EMIS' as Service, DeptEnterpriseID, Team
from ServiceSearch a
-- logic here heeds to take close dates into account and also to follow Gareth's instructions re filtering
join (select deptodscode,deptenterpriseid,ROW_NUMBER() over (PARTITION by deptodscode order by deptodscode) as rn from PDDepartment) d on a.ODS = DeptODSCode and d.rn=1
-- enforce uniqueness MS 29.5.16
join (select *,
row_number() over (partition by activitydepartmentid order by team) rn 
from Reporting.DisambiguatedActivityTeams) at on at.ActivityDepartmentID = d.DeptEnterpriseID and at.rn=1




--[Cache-AvailabilityLogGeneric] 
select ItemId as id,
cast(actiontime as datetime) as EventDateTime,
cast(actiontime as date) as EventDate,
case when MRNs='0' then 'N' else 'Y' end as match,
'Generic' as Service, DeptEnterpriseID, Team
from ServiceSearchGeneric a
-- logic here heeds to take close dates into account and also to follow Gareth's instructions re filtering
join (select deptodscode,deptenterpriseid,ROW_NUMBER() over (PARTITION by deptodscode order by deptodscode) as rn from PDDepartment) d on a.ODS = DeptODSCode and d.rn=1
-- enforce uniqueness MS 29.5.16
join (select *,
row_number() over (partition by activitydepartmentid order by team) rn 
from Reporting.DisambiguatedActivityTeams) at on at.ActivityDepartmentID = d.DeptEnterpriseID and at.rn=1