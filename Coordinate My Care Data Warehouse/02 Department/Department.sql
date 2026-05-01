USE [ETL_Local_PROD]
GO

/****** Object:  View [dbo].[Cache-Department]    Script Date: 11/10/2019 14:05:32 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


 if OBJECT_ID ('Tempdb..#CacheOrg') is not null
 drop table #CacheOrg
select ItemId as Organisation, 
cast(case when Source is null then 'CC' else  Source end as varchar(25)) as DeptSource, 
EnterpriseID as DeptEnterpriseId,
PDRegistryID as DeptPDRegistryId,
Name,
cast(LocalCMCId as varchar(255)) as DeptLocalCMCID, 
cast(ODSCode as varchar(255)) as DeptODSCode, 
OpenDate as DeptOpenDate,
CloseDate as DeptCloseDate,
LocalCMCOrgType,
RegistryID 
into #CacheOrg
from ETL_PROD.dbo.cmc_organization with (nolock)
where ETL_PROD.dbo.cmc_organization.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
  and ETL_PROD.dbo.cmc_organization.ItemId not like 'PS|%|%|%|%|%|LCA'
 
 --select * from #CacheOrg
 



  if OBJECT_ID ('Tempdb..#OrganizationType') is not null
 drop table #OrganizationType
select 
a.*,
b.*
into #OrganizationType
from ETL_PROD.dbo.CMC_Organization_OrganizationTypeCodes a
left join ETL_PROD.dbo.Coded_OrgType b on b.Code = a.OrganizationType

--select * from #OrganizationType

  if OBJECT_ID ('Tempdb..#OrganizationName') is not null
 drop table #OrganizationName

select   * into #OrganizationName from  ETL_PROD.dbo.CMC_OrganizationName where name is not null


  if OBJECT_ID ('Tempdb..#Department') is not null
 drop table #Department

select 
cast(null as varchar(max))as  DeptName,
cast(null as varchar(255)) as OrganizationType,
cast(null as varchar(max))as  OrganizationTypeDescription,
Name,
o.Organisation,
o.DeptSource,
o.DeptEnterpriseID,
o.DeptPDRegistryID,
o.RegistryID,
o.DeptLocalCMCId,
o.LocalCMCOrgType,
cast(null as varchar(max))as  LocalCMCOrgTypeDescription,
o.DeptODSCode,
o.DeptOpenDate,
o.DeptCloseDate

into #Department
from #CacheOrg o



Update r

		set r.DeptEnterpriseID = coalesce(r.DeptEnterpriseID,o.DeptEnterpriseID),
			r.DeptPDRegistryId = coalesce(r.DeptPDRegistryId,o.DeptPDRegistryId),
			--r.Name = coalesce(r.Name,o.Name),
			r.DeptLocalCMCID = coalesce(r.DeptLocalCMCID,o.DeptLocalCMCID),
			r.DeptODSCode = coalesce(r.DeptODSCode,o.DeptODSCode),
			r.DeptOpenDate = coalesce(r.DeptOpenDate,o.DeptOpenDate),
			r.DeptCloseDate = coalesce(r.DeptCloseDate,o.DeptCloseDate),
			r.LocalCMCOrgType = coalesce(r.LocalCMCOrgType,o.LocalCMCOrgType)
from #Department r
left join #CacheOrg o on o.DeptPDRegistryId = r.RegistryID 
where r.DeptSource is null


Update x
		set x.DeptName = n.Name,
			x.OrganizationType = t.OrganizationType,
				x.OrganizationTypeDescription = t.Description  

from #Department x
left join #OrganizationType  t on t.Organization = x.Organisation
 
left join ETL_PROD.dbo.Coded_LocalCMCOrgType ol on ol.code = x.LocalCMCOrgType
left join #OrganizationName n on n.ItemId = x.Name

where n.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
AND n.ItemId not like 'PS|%|%|%|%|%|LCA'

 
 drop table #CacheOrg
 drop table #OrganizationType
 drop table #OrganizationName
 
 --select * from #Department where DeptSource = 'PD'
----USE [ETL_PROD]
----GO

--SELECT
--  			*
--  		FROM
--  			INFORMATION_SCHEMA.TABLES
--  		WHERE
--  			 charindex('Organization',TABLE_NAME )>0
--GO


