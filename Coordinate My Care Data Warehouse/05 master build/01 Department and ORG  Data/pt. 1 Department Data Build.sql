

 
 use ETL_Local_PROD
 
GO
/****** Object:  StoredProcedure [dbo].[NewCache]    Script Date: 07/01/2020 13:12:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






Alter PROCEDURE [dbo].[AT_MASTERBUILD_ORG_DATA] 
-- Amended for PD Upgrade
AS
BEGIN




if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_CarePlanVersion]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_CarePlanVersion]


  select 
 a.[GenusId],
 b.VersionNumber as FirstVersion,
 b.PatientSummary as FirstPatientSummary,
 b.CarePlan as FirstCarePlan,
 c.VersionNumber as LastestVersion,
 c.PatientSummary as LastPatientSummary,
 c.CarePlan as LastCarePlan
  
  Into [ETL_Local_PROD].[dbo].[AT_CarePlanVersion]
  
  from  [ETL_PROD].[dbo].[CMC_PatientSummary_CarePlan] a
  inner join
			(
			SELECT [GenusId]
      ,[VersionNumber]
      ,[PatientSummary]
      ,[CarePlan]
  FROM (select *,
  ROW_NUMBER() over (PARTITION by GenusID order by VersionNumber) as rn
  from [ETL_PROD].[dbo].[CMC_PatientSummary_CarePlan]) sel1
  where rn=1

			)b on b.GenusId = a.GenusId
			and b.VersionNumber = a.VersionNumber
  left join
			(
			SELECT [GenusId]
      ,[VersionNumber]
      ,[PatientSummary]
      ,[CarePlan]
  FROM (select *,
  ROW_NUMBER() over (PARTITION by GenusID order by VersionNumber desc) as rn
  from [ETL_PROD].[dbo].[CMC_PatientSummary_CarePlan]) sel1
  where rn=1

			)c on c.GenusId = a.GenusId 


 

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 

truncate table [ETL_Local_PROD].[dbo].[AT_CacheOrg]

insert into [ETL_Local_PROD].[dbo].[AT_CacheOrg]

select ItemId, 
cast(Source as varchar(255)) as Source, 
EnterpriseID,
PDRegistryID,
Name,
cast(LocalCMCId as varchar(255)) as LocalCMCId, 
cast(ODSCode as varchar(255)) as ODSCode, 
OpenDate,
CloseDate,
LocalCMCOrgType,
RegistryID 

--into [ETL_Local_PROD].[dbo].[AT_CacheOrg]

from ETL_PROD.dbo.cmc_organization with (nolock)
where ETL_PROD.dbo.cmc_organization.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
  and ETL_PROD.dbo.cmc_organization.ItemId not like 'PS|%|%|%|%|%|LCA'

 
  

if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_SingleORgType]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_SingleORgType]
select 
Organization,
MIN(organizationtype) as OrganizationType 
into [ETL_Local_PROD].[dbo].[AT_SingleORgType]
from ETL_PROD.dbo.CMC_Organization_OrganizationTypeCodes 
group by Organization




	if OBJECT_ID ('Tempdb..#CacheO') is not null
	drop table #CacheO

	select 
	raw.ItemId as Organization,
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

	into #CacheO

	from [ETL_Local_PROD].[dbo].[AT_CacheOrg] raw
	left join [ETL_Local_PROD].[dbo].[AT_CacheOrg] pd on raw.registryid = pd.pdregistryid and raw.Source is null
														and raw.RegistryID is not null and pd.PDRegistryID is not null
 


truncate table [ETL_Local_PROD].[dbo].[AT_Dept]

insert  into [ETL_Local_PROD].[dbo].[AT_Dept]
  select 
    ROW_NUMBER() over ( order by o.Organization,o.DeptEnterpriseID,o.DeptPDRegistryID)  as DeptKey,
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

 

from #CacheO o
left join [ETL_Local_PROD].[dbo].[AT_SingleORgType] t on o.PDItemId = t.Organization
left join ETL_PROD.dbo.Coded_OrgType tl on tl.Code = t.OrganizationType
left join ETL_PROD.dbo.Coded_LocalCMCOrgType ol on ol.code = o.LocalCMCOrgType
left join ETL_PROD.dbo.CMC_OrganizationName n on n.ItemId = o.Name
-- Exclude Last Clinical Approver rows for 15.1 release
where n.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
AND n.ItemId not like 'PS|%|%|%|%|%|LCA'
 
------------------------------------------------------------------------------------------------- 
if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_PD_Dept]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_PD_Dept]
	select 
	*
	--into [ETL_Local_PROD].[dbo].[AT_PD_Dept]
	into [ETL_Local_PROD].[dbo].[AT_PD_Dept]
	from [ETL_Local_PROD].[dbo].[AT_Dept]
	Where DeptSource = 'PD'

 

if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_PDToOrg]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_PDToOrg]
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
dc.DeptSource,
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

into [ETL_Local_PROD].[dbo].[AT_PDToOrg]

from ETL_PROD.dbo.CMC_OrgToOrg o
join [ETL_Local_PROD].[dbo].[AT_PD_Dept] dc on dc.DeptEnterpriseID = o.ChildOrganizationEID  
join [ETL_Local_PROD].[dbo].[AT_PD_Dept] dp on dp.DeptEnterpriseID = o.ParentOrganizationEID  
left join ETL_PROD.dbo.Coded_OrgOrgRelationshipType t on TypeCodedValue = t.code
left join
		(
		select 
		orgtoorg,
		StartDate,
		EndDate,
        ROW_NUMBER() over (partition by OrgToOrg order by startdate desc) as rn
		from ETL_PROD.dbo.CMC_OrgToOrg_DateSpan od
		left join ETL_PROD.dbo.CMC_DateSpan ds on od.DateSpan = ds.ItemId
		) d  on o.ItemId = d.OrgToOrg and d.rn=1


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_Staff]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_Staff]
-- Amended for PD upgrade
select 
i.EnterpriseID as StaffEnterpriseID,
i.ItemID as Individual,
n.NamePrefix as StaffTitle,
t.Description as StaffTitleDescription,
n.GivenName as StaffForename, n.MiddleName as StaffMiddleName, n.FamilyName as StaffSurname,
i.ODSCode as StaffODSCode,
i.LocalCMCId as StaffLocalCMCId,
--CreatedDate changed to CreationDateTime MS 2.9.16
cast(i.CreationDateTime as date) as StaffCreatedDate,
-- correct status MS 20.5.17
i.StatusCode as StaffActive,
tu.Description as StaffActiveDescription,
c.Description as StaffDescription,
pt.ProviderType as StaffProviderType,
tt.Description as StaffProviderTypeDescription,
i.PDRegistryId as StaffRegistryId,
c.UserID as StaffUserId,
c.ItemId as StaffUserClinician

into [ETL_Local_PROD].[dbo].[AT_Staff]

from ETL_PROD.dbo.CMC_Individual i
left join ETL_PROD.dbo.CMC_Name n on i.Name = n.ItemId
left join ETL_PROD.dbo.CMC_UserIdentifier u on i.PDRegistryID = u.Extension
and AssigningAuthorityName = 'HSREGISTRY'
left join ETL_PROD.dbo.CMC_UserClinician c on u.UserClinician = c.itemid
left join ETL_PROD.dbo.Coded_NamePrefix t on n.NamePrefix = t.Code
-- handle multiple provider types MS 2.7.16
left join (select *,ROW_NUMBER() over (PARTITION by individual order by individual) as rn from ETL_PROD.dbo.CMC_Individual_ProviderTypes) pt on i.ItemId = pt.Individual and pt.rn=1
left join ETL_PROD.dbo.Coded_IndType tt on pt.ProviderType = tt.Code
-- correct status lookup MS 20.5.17
left join ETL_PROD.dbo.Coded_IndStatus tu on i.StatusCode = tu.Code


   if OBJECT_ID ('Tempdb..#PDOrgToOrg') is not null ---Query Logic here
	drop table #PDOrgToOrg
	select 
	* 
	into #PDOrgToOrg
	from [ETL_Local_PROD].[dbo].[AT_PDToOrg]
-- 'Member' in 141001 MS 28.9.16
	where Org2OrgType is null
	or Org2OrgType = 'Member'





if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_Dept_Heirarchy]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_Dept_Heirarchy]
 
SELECT    d1.DeptName AS name7, d1.LocalCMCOrgType AS type7, d1.LocalCMCOrgTypeDescription AS typedesc7, d1.DeptODSCode AS ods7, 
                      d1.DeptLocalCMCId AS cmcdeptid7, d1.DeptEnterpriseID AS eid7, d1.DeptCloseDate AS close7, d1.OrganizationType AS odstype7, 
                      d1.OrganizationTypeDescription AS odstypedescription7, d2.DeptName AS name6, d2.LocalCMCOrgType AS type6, d2.LocalCMCOrgTypeDescription AS typedesc6, 
                      d2.DeptODSCode AS ods6, d2.DeptLocalCMCId AS cmcdeptid6, d2.DeptEnterpriseID AS eid6, d2.DeptCloseDate AS close6, d2.OrganizationType AS odstype6, 
                      d2.OrganizationTypeDescription AS odstypedescription6, d3.DeptName AS name5, d3.LocalCMCOrgType AS type5, d3.LocalCMCOrgTypeDescription AS typedesc5, 
                      d3.DeptODSCode AS ods5, d3.DeptLocalCMCId AS cmcdeptid5, d3.DeptEnterpriseID AS eid5, d3.DeptCloseDate AS close5, d3.OrganizationType AS odstype5, 
                      d3.OrganizationTypeDescription AS odstypedescription5, d4.DeptName AS name4, d4.LocalCMCOrgType AS type4, d4.LocalCMCOrgTypeDescription AS typedesc4, 
                      d4.DeptODSCode AS ods4, d4.DeptLocalCMCId AS cmcdeptid4, d4.DeptEnterpriseID AS eid4, d4.DeptCloseDate AS close4, d4.OrganizationType AS odstype4, 
                      d4.OrganizationTypeDescription AS odstypedescription4, d5.DeptName AS name3, d5.LocalCMCOrgType AS type3, d5.LocalCMCOrgTypeDescription AS typedesc3, 
                      d5.DeptODSCode AS ods3, d5.DeptLocalCMCId AS cmcdeptid3, d5.DeptEnterpriseID AS eid3, d5.DeptCloseDate AS close3, d5.OrganizationType AS odstype3, 
                      d5.OrganizationTypeDescription AS odstypedescription3, d6.DeptName AS name2, d6.LocalCMCOrgType AS type2, d6.LocalCMCOrgTypeDescription AS typedesc2, 
                      d6.DeptODSCode AS ods2, d6.DeptLocalCMCId AS cmcdeptid2, d6.DeptEnterpriseID AS eid2, d6.DeptCloseDate AS close2, d6.OrganizationType AS odstype2, 
                      d6.OrganizationTypeDescription AS odstypedescription2, d7.DeptName AS name1, d7.LocalCMCOrgType AS type1, d7.LocalCMCOrgTypeDescription AS typedesc1, 
                      d7.DeptODSCode AS ods1, d7.DeptLocalCMCId AS cmcdeptid1, d7.DeptEnterpriseID AS eid1, d7.DeptCloseDate AS close1, d7.OrganizationType AS odstype1, 
                      d7.OrganizationTypeDescription AS odstypedescription1

					  into [ETL_Local_PROD].[dbo].[AT_Dept_Heirarchy]

FROM         [ETL_Local_PROD].[dbo].[AT_PD_Dept] AS d1 WITH (nolock) 
					LEFT OUTER JOIN  #PDOrgToOrg AS o1 WITH (nolock) ON d1.DeptEnterpriseID = o1.ChildOrganizationEID 
																								AND (o1.EndDate IS NULL OR  CAST(o1.EndDate AS date) > '2015-05-01') 
																								AND (o1.StartDate IS NULL OR  CAST(o1.StartDate AS date) <= CAST(GETDATE() AS DATE)) 
					  LEFT OUTER JOIN  [ETL_Local_PROD].[dbo].[AT_PD_Dept] AS d2 WITH (nolock) ON d2.DeptEnterpriseID = o1.ParentOrganizationEID 
																							 
					  LEFT OUTER JOIN  #PDOrgToOrg AS o2 WITH (nolock) ON o1.ParentOrganizationEID = o2.ChildOrganizationEID 
																								AND (o2.EndDate IS NULL OR CAST(o2.EndDate AS date) > '2015-05-01') 
																								AND (o2.StartDate IS NULL OR CAST(o2.StartDate AS date) <= CAST(GETDATE() AS DATE)) 
					  LEFT OUTER JOIN  [ETL_Local_PROD].[dbo].[AT_PD_Dept] AS d3 WITH (nolock) ON d3.DeptEnterpriseID = o2.ParentOrganizationEID 
																							 
					  LEFT OUTER JOIN #PDOrgToOrg AS o3 WITH (nolock) ON o2.ParentOrganizationEID = o3.ChildOrganizationEID 
																								AND (o3.EndDate IS NULL OR CAST(o3.EndDate AS date) > '2015-05-01') 
																								AND (o3.StartDate IS NULL OR CAST(o3.StartDate AS date) <= CAST(GETDATE() AS DATE)) 
					  LEFT OUTER JOIN [ETL_Local_PROD].[dbo].[AT_PD_Dept] AS d4 WITH (nolock) ON d4.DeptEnterpriseID = o3.ParentOrganizationEID 
																							 
					  LEFT OUTER JOIN #PDOrgToOrg AS o4 WITH (nolock) ON o3.ParentOrganizationEID = o4.ChildOrganizationEID 
																								AND (o4.EndDate IS NULL OR CAST(o4.EndDate AS date) > '2015-05-01') 
																								AND (o4.StartDate IS NULL OR  CAST(o4.StartDate AS date) <= CAST(GETDATE() AS DATE)) 
					  LEFT OUTER JOIN [ETL_Local_PROD].[dbo].[AT_PD_Dept] AS d5 WITH (nolock) ON d5.DeptEnterpriseID = o4.ParentOrganizationEID 
																							 
					  LEFT OUTER JOIN #PDOrgToOrg AS o5 WITH (nolock) ON o4.ParentOrganizationEID = o5.ChildOrganizationEID 
																								AND (o5.EndDate IS NULL OR CAST(o5.EndDate AS date) > '2015-05-01') 
																								AND (o5.StartDate IS NULL OR  CAST(o5.StartDate AS date) <= CAST(GETDATE() AS DATE)) 
					  LEFT OUTER JOIN [ETL_Local_PROD].[dbo].[AT_PD_Dept] AS d6 WITH (nolock) ON d6.DeptEnterpriseID = o5.ParentOrganizationEID 
																						 
					  LEFT OUTER JOIN #PDOrgToOrg AS o6 WITH (nolock) ON o5.ParentOrganizationEID = o6.ChildOrganizationEID 
																								AND (o6.EndDate IS NULL OR CAST(o6.EndDate AS date) > '2015-05-01') 
																								AND (o6.StartDate IS NULL OR  CAST(o6.StartDate AS date) <= CAST(GETDATE() AS DATE)) 
					  LEFT OUTER JOIN [ETL_Local_PROD].[dbo].[AT_PD_Dept] AS d7 WITH (nolock) ON d7.DeptEnterpriseID = o6.ParentOrganizationEID 
																							 
					 
	 
 

  if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_IndToOrg]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_IndToOrg]

 SELECT
IndividualEId as StaffEnterpriseID,
OrganizationEId as DeptEnterpriseID,
TypeCodedValue as Ind2OrgType,
ic.Description as Ind2OrgTypeDescription,
case
when i.startdate='1900-01-01' then cast(CreationDateTime as date)
when i.startdate is null then cast(CreationDateTime as date)
else i.startdate end as StartDate,
i.EndDate,
LocalIndCMCRole as CMCRole,
cr.Description as CMCRoleDescription,
-- Add IndToOrg itemid for matching MS 22.1.17
io.ItemId as IndToOrg,
-- Add CreationDateTime MS 22.1.17
io.CreationDateTime

into [ETL_Local_PROD].[dbo].[AT_IndToOrg]

from ETL_PROD.dbo.CMC_IndToOrg io
left join ETL_PROD.dbo.Coded_IndOrgRelationshipType ic on io.TypeCodedValue = ic.Code
left join ETL_PROD.dbo.Coded_LocalIndCMCRole cr on io.LocalIndCMCRole = cr.Code
left join
  (select indtoorg,StartDate,EndDate,
          ROW_NUMBER() over (partition by indToOrg order by startdate desc) as rn
  from ETL_PROD.dbo.CMC_IndToOrg_DateSpan id
  left join ETL_PROD.dbo.CMC_DateSpan ds on id.DateSpan = ds.ItemId) i 
  on io.ItemId = i.IndToOrg and i.rn=1


   end