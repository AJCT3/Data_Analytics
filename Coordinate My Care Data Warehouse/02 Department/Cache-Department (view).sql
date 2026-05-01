USE [ETL_Local_PROD]
GO

/****** Object:  View [dbo].[Cache-Department]    Script Date: 21/10/2019 10:09:34 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE view [dbo].[Cache-Department] as
select 
n.Name as DeptName,
t.OrganizationType,
tl.Description as OrganizationTypeDescription,
o.Organization,
o.DeptSource,
o.DeptEnterpriseID,
o.DeptPDRegistryID,
o.DeptLocalCMCId,
o.LocalCMCOrgType,
ol.Description as LocalCMCOrgTypeDescription,
o.DeptODSCode,
o.DeptOpenDate,
o.DeptCloseDate
from
(select raw.ItemId as Organization,
-- shorten fields so we can create indexes MS 16.2.16
cast(case when raw.Source is null then 'CC' else raw.Source end as varchar(25)) as DeptSource,
ISNULL(pd.ItemId,raw.ItemId) as PDItemId,
ISNULL(pd.enterpriseid,raw.enterpriseid) as DeptEnterpriseId,
ISNULL(pd.PDRegistryID,raw.PDRegistryID) as DeptPDRegistryId,
ISNULL(pd.Name,raw.Name) as Name,
cast(ISNULL(pd.LocalCMCID,raw.LocalCMCID) as varchar(255)) as DeptLocalCMCID,
ISNULL(pd.ODSCode,raw.ODSCode) as DeptODSCode,
ISNULL(pd.OpenDate,raw.OpenDate) as DeptOpenDate,
ISNULL(pd.CloseDate,raw.CloseDate) as DeptCloseDate,
ISNULL(pd.LocalCMCOrgType,raw.LocalCMCOrgType) as LocalCMCOrgType
-- use cached version of CMC_Organization, with indexes, for performance MS 7.8.16
from CacheOrganization raw
left join CacheOrganization pd
on raw.registryid = pd.pdregistryid and raw.Source is null) o
left join SingleOrganizationType t on o.PDItemId = t.Organization
left join ETL_PROD.dbo.Coded_OrgType tl on tl.Code = t.OrganizationType
left join ETL_PROD.dbo.Coded_LocalCMCOrgType ol on ol.code = o.LocalCMCOrgType
left join ETL_PROD.dbo.CMC_OrganizationName n on n.ItemId = o.Name
-- Exclude Last Clinical Approver rows for 15.1 release
where n.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
AND n.ItemId not like 'PS|%|%|%|%|%|LCA'
GO


