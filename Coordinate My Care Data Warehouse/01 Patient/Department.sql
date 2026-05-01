USE [ETL_Local_PROD]
GO

 if OBJECT_ID ('Tempdb..#CacheOrg') is not null
	drop table #CacheOrg
select a.ItemId as Organization, 
cast(  a.Source as varchar(25)) as DeptSource,
EnterpriseID as DeptEnterpriseId,
PDRegistryID as DeptPDRegistryId,
c.Name as DeptName ,
c.ItemId as OrgItem,
b.OrganizationType,
tl.Description as OrganizationTypeDescription,
cast(LocalCMCId as varchar(255)) as DeptLocalCMCID,
cast(ODSCode as varchar(255)) as DeptODSCode,
OpenDate as DeptOpenDate,
CloseDate as DeptCloseDate,
LocalCMCOrgType,
ol.Description as LocalCMCOrgTypeDescription,
RegistryID 

into #CacheOrg

from ETL_PROD.dbo.cmc_organization a with (nolock) 
left join(
			select 
			Organization,
			MIN(organizationtype) as OrganizationType 
			from ETL_PROD.dbo.CMC_Organization_OrganizationTypeCodes --There is no way of knowing which one is valid!!!!!! Query with Zhong
			group by Organization)
			b on b.Organization = a.ItemId
Left join ETL_PROD.dbo.CMC_OrganizationName c on c.ItemId = a.Name
left join ETL_PROD.dbo.Coded_OrgType tl on tl.Code = b.OrganizationType
left join ETL_PROD.dbo.Coded_LocalCMCOrgType ol on ol.code = a.LocalCMCOrgType
where a.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
  and a.ItemId not like 'PS|%|%|%|%|%|LCA'
  and a.Source  = 'PD'

 --  if OBJECT_ID ('Tempdb..#PreOrg') is not null
	--drop table #PreOrg

 -- select raw.ItemId as Organization,
	--	-- shorten fields so we can create indexes MS 16.2.16
	--	--cast( coalesce(Source,'CC') as varchar(25)) as DeptSource,
	--	cast(  Source as varchar(25)) as DeptSource,
	--	ItemId as PDItemId,
	--	enterpriseid as DeptEnterpriseId,
	--	PDRegistryID as DeptPDRegistryId,
	--	OrganizationName as Name,
	--	cast(LocalCMCID as varchar(255)) as DeptLocalCMCID,
	--	ODSCode as DeptODSCode,
	--	OpenDate as DeptOpenDate,
	--	CloseDate as DeptCloseDate,
	--	LocalCMCOrgType as LocalCMCOrgType,
	--	organizationType as OrganizationType,
	--	OrgItem as OrgItem

	--	into #PreOrg
	--	-- use cached version of CMC_Organization, with indexes, for performance MS 7.8.16
	--	from #CacheOrg raw


		--update f

		--		set f.DeptSource = coalesce(f.DeptSource,'CC'),
		--			f.PDItemId = coalesce(f.pditemid,pd.ItemId),
		--			f.DeptEnterpriseId = coalesce(f.DeptEnterpriseId,pd.enterpriseid),
		--			f.DeptPDRegistryId = coalesce(f.DeptPDRegistryId,pd.PDRegistryID),
		--			f.Name = coalesce(f.name,pd.OrganizationName),
		--			f.DeptLocalCMCID = coalesce(f.DeptLocalCMCID,pd.LocalCMCID),
		--			f.DeptODSCode = coalesce(f.DeptODSCode,pd.ODSCode),
		--			f.DeptOpenDate = coalesce(f.DeptOpenDate,pd.OpenDate),
		--			f.DeptCloseDate = coalesce(f.DeptCloseDate,pd.CloseDate),
		--			f.LocalCMCOrgType = coalesce(f.LocalCMCOrgType,pd.LocalCMCOrgType),
		--			f.OrganizationType = coalesce(f.OrganizationType,pd.OrganizationType),
		--			f.OrgItem = coalesce(f.OrgItem,pd.OrgItem)



		--from #PreOrg f 
		--left join #CacheOrg pd
		--on f.DeptPDRegistryId = pd.pdregistryid and f.DeptSource is null
		--where (f.DeptPDRegistryId is not null and pd.pdregistryid is not null)
 --select * from #PreOrg where DeptSource is null and DeptPDRegistryId is not null

--  if OBJECT_ID ('Tempdb..#Department') is not null
--	drop table #Department
--select 
--o.Name as DeptName,
--o.OrganizationType,
--tl.Description as OrganizationTypeDescription,
--o.Organization,
--o.DeptSource,
--o.DeptEnterpriseID,
--o.DeptPDRegistryID,
--o.DeptLocalCMCId,
--o.LocalCMCOrgType,
--ol.Description as LocalCMCOrgTypeDescription,
--o.DeptODSCode,
--o.DeptOpenDate,
--o.DeptCloseDate

--into #Department

--from #CacheOrg o
----left join SingleOrganizationType t on o.PDItemId = t.Organization
--left join ETL_PROD.dbo.Coded_OrgType tl on tl.Code = o.OrganizationType
--left join ETL_PROD.dbo.Coded_LocalCMCOrgType ol on ol.code = o.LocalCMCOrgType
----left join ETL_PROD.dbo.CMC_OrganizationName n on n.ItemId = o.Name
---- Exclude Last Clinical Approver rows for 15.1 release
--where o.OrgItem not like 'CarePlan|%|%|%|%|%|LCA'
--AND o.OrgItem not like 'PS|%|%|%|%|%|LCA'

--and o.DeptSource = 'PD'

--select * from #Department

  if OBJECT_ID ('Tempdb..#PDOrgToOrg') is not null
	drop table #PDOrgToOrg
select
o.ChildOrganizationEID,
-- Names for documentation only
dc.DeptName as Child,
dc.DeptODSCode as ChildODS,
dc.DeptLocalCMCId as ChildLocalCMCId,
dc.OrganizationType as ChildOrgType,
dc.LocalCMCOrgType as ChildCMCOrgType,
dc.OrganizationTypeDescription as ChildOrgTypeDescription,
dc.LocalCMCOrgTypeDescription as ChildCMCOrgTypeDescription,
o.ParentOrganizationEID,
dp.DeptName as Parent,
dp.DeptODSCode as ParentODS,
dp.DeptLocalCMCId as ParentLocalCMCId,
dp.OrganizationType as ParentOrgType,
dp.LocalCMCOrgType as ParentCMCOrgType,
dp.OrganizationTypeDescription as ParentOrgTypeDescription,
dp.LocalCMCOrgTypeDescription as ParentCMCOrgTypeDescription,
TypeCodedValue as Org2OrgType,
t.Description as Org2OrgTypeDescription,
-- MS 30.1.17 improve start dates to cover those left blank
case
when d.startdate='1900-01-01' then cast(CreationDateTime as date)
when d.startdate is null then cast(CreationDateTime as date)
else d.startdate end as StartDate,
d.EndDate,
-- MS 30.1.17 add CreationDateTime
o.CreationDateTime

into #PDOrgToOrg

from ETL_PROD.dbo.CMC_OrgToOrg o
join #CacheOrg dc on dc.DeptEnterpriseID = o.ChildOrganizationEID
join #CacheOrg dp on dp.DeptEnterpriseID = o.ParentOrganizationEID
left join ETL_PROD.dbo.Coded_OrgOrgRelationshipType t on TypeCodedValue = t.code
left join
  (select orgtoorg,StartDate,EndDate,
          ROW_NUMBER() over (partition by OrgToOrg order by startdate desc) as rn
  from ETL_PROD.dbo.CMC_OrgToOrg_DateSpan od
  left join ETL_PROD.dbo.CMC_DateSpan ds on od.DateSpan = ds.ItemId) d 
  on o.ItemId = d.OrgToOrg and d.rn=1




   if OBJECT_ID ('Tempdb..#Department') is not null
	drop table #Department
 
SELECT     
d1.DeptName AS name7, 
d1.LocalCMCOrgType AS type7, 
d1.LocalCMCOrgTypeDescription AS typedesc7, 
d1.DeptODSCode AS ods7,
d1.DeptLocalCMCId AS cmcdeptid7, 
d1.DeptEnterpriseID AS eid7, 
d1.DeptCloseDate AS close7, 
d1.OrganizationType AS odstype7, 
d1.OrganizationTypeDescription AS odstypedescription7, 
d2.DeptName AS name6, 
d2.LocalCMCOrgType AS type6, 
d2.LocalCMCOrgTypeDescription AS typedesc6,
d2.DeptODSCode AS ods6, 
d2.DeptLocalCMCId AS cmcdeptid6, 
d2.DeptEnterpriseID AS eid6, 
d2.DeptCloseDate AS close6, 
d2.OrganizationType AS odstype6,
d2.OrganizationTypeDescription AS odstypedescription6, 
d3.DeptName AS name5, 
d3.LocalCMCOrgType AS type5, 
d3.LocalCMCOrgTypeDescription AS typedesc5,
d3.DeptODSCode AS ods5, 
d3.DeptLocalCMCId AS cmcdeptid5, 
d3.DeptEnterpriseID AS eid5, 
d3.DeptCloseDate AS close5, 
d3.OrganizationType AS odstype5,
d3.OrganizationTypeDescription AS odstypedescription5, 
d4.DeptName AS name4, 
d4.LocalCMCOrgType AS type4, 
d4.LocalCMCOrgTypeDescription AS typedesc4,
d4.DeptODSCode AS ods4, 
d4.DeptLocalCMCId AS cmcdeptid4, 
d4.DeptEnterpriseID AS eid4, 
d4.DeptCloseDate AS close4, 
d4.OrganizationType AS odstype4,
d4.OrganizationTypeDescription AS odstypedescription4, 
d5.DeptName AS name3, 
d5.LocalCMCOrgType AS type3, 
d5.LocalCMCOrgTypeDescription AS typedesc3,
d5.DeptODSCode AS ods3, 
d5.DeptLocalCMCId AS cmcdeptid3, 
d5.DeptEnterpriseID AS eid3, 
d5.DeptCloseDate AS close3, 
d5.OrganizationType AS odstype3,
d5.OrganizationTypeDescription AS odstypedescription3, 
d6.DeptName AS name2, 
d6.LocalCMCOrgType AS type2, 
d6.LocalCMCOrgTypeDescription AS typedesc2,
d6.DeptODSCode AS ods2, 
d6.DeptLocalCMCId AS cmcdeptid2, 
d6.DeptEnterpriseID AS eid2, 
d6.DeptCloseDate AS close2, 
d6.OrganizationType AS odstype2,
d6.OrganizationTypeDescription AS odstypedescription2, 
d7.DeptName AS name1, 
d7.LocalCMCOrgType AS type1, 
d7.LocalCMCOrgTypeDescription AS typedesc1,
 d7.DeptODSCode AS ods1, 
 d7.DeptLocalCMCId AS cmcdeptid1, 
 d7.DeptEnterpriseID AS eid1, 
 d7.DeptCloseDate AS close1, 
 d7.OrganizationType AS odstype1,
 d7.OrganizationTypeDescription AS odstypedescription1

 into #Department

FROM         #CacheOrg AS d1 WITH (nolock) LEFT OUTER JOIN
                      dbo.PDOrgToOrg AS o1 WITH (nolock) ON d1.DeptEnterpriseID = o1.ChildOrganizationEID AND (o1.EndDate IS NULL OR
                      CAST(o1.EndDate AS date) > '2015-05-01') AND (o1.StartDate IS NULL OR
                      CAST(o1.StartDate AS date) <= CAST(GETDATE() AS DATE)) LEFT OUTER JOIN
                     #CacheOrg AS d2 WITH (nolock) ON d2.DeptEnterpriseID = o1.ParentOrganizationEID LEFT OUTER JOIN
                      dbo.PDOrgToOrg AS o2 WITH (nolock) ON o1.ParentOrganizationEID = o2.ChildOrganizationEID AND (o2.EndDate IS NULL OR
                      CAST(o2.EndDate AS date) > '2015-05-01') AND (o2.StartDate IS NULL OR
                      CAST(o2.StartDate AS date) <= CAST(GETDATE() AS DATE)) LEFT OUTER JOIN
                      #CacheOrg AS d3 WITH (nolock) ON d3.DeptEnterpriseID = o2.ParentOrganizationEID LEFT OUTER JOIN
                      dbo.PDOrgToOrg AS o3 WITH (nolock) ON o2.ParentOrganizationEID = o3.ChildOrganizationEID AND (o3.EndDate IS NULL OR
                      CAST(o3.EndDate AS date) > '2015-05-01') AND (o3.StartDate IS NULL OR
                      CAST(o3.StartDate AS date) <= CAST(GETDATE() AS DATE)) LEFT OUTER JOIN
                      #CacheOrg AS d4 WITH (nolock) ON d4.DeptEnterpriseID = o3.ParentOrganizationEID LEFT OUTER JOIN
                      dbo.PDOrgToOrg AS o4 WITH (nolock) ON o3.ParentOrganizationEID = o4.ChildOrganizationEID AND (o4.EndDate IS NULL OR
                      CAST(o4.EndDate AS date) > '2015-05-01') AND (o4.StartDate IS NULL OR
                      CAST(o4.StartDate AS date) <= CAST(GETDATE() AS DATE)) LEFT OUTER JOIN
                      #CacheOrg AS d5 WITH (nolock) ON d5.DeptEnterpriseID = o4.ParentOrganizationEID LEFT OUTER JOIN
                      dbo.PDOrgToOrg AS o5 WITH (nolock) ON o4.ParentOrganizationEID = o5.ChildOrganizationEID AND (o5.EndDate IS NULL OR
                      CAST(o5.EndDate AS date) > '2015-05-01') AND (o5.StartDate IS NULL OR
                      CAST(o5.StartDate AS date) <= CAST(GETDATE() AS DATE)) LEFT OUTER JOIN
                      #CacheOrg AS d6 WITH (nolock) ON d6.DeptEnterpriseID = o5.ParentOrganizationEID LEFT OUTER JOIN
                      dbo.PDOrgToOrg AS o6 WITH (nolock) ON o5.ParentOrganizationEID = o6.ChildOrganizationEID AND (o6.EndDate IS NULL OR
                      CAST(o6.EndDate AS date) > '2015-05-01') AND (o6.StartDate IS NULL OR
                      CAST(o6.StartDate AS date) <= CAST(GETDATE() AS DATE)) LEFT OUTER JOIN
                      #CacheOrg AS d7 WITH (nolock) ON d7.DeptEnterpriseID = o6.ParentOrganizationEID
 


 

