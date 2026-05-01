 
 use ETL_Local_PROD


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



 --select * FROM  [ETL_Local_PROD].[dbo].[AT_Dept_Heirarchy]
-- select
--REPLACE(ad.GenusId,'PatientSummary||','') as CMC_ID,
--REPLACE(FromVersionId,'PatientSummary','PS') as FromPatientSummary,
--REPLACE(ToVersionId,'PatientSummary','PS') as ToPatientSummary,
--ItemId as Audit,
--ActionTime,
--ActionType
--from (select * from ETL_PROD.dbo.CMC_AuditData ad where Actor = 'System' and ActionType not in ('Login','Logout')) ad
--where RecordName = 'PatientSummary'

-----------------------------------------------------------------------------------------------------------------------------------------------------------------




-----------------------------------------------------------------------------------------------------------------------------------------------------------------




 
	 if OBJECT_ID ('Tempdb..#AuditPatient') is not null
	drop table #AuditPatient
	select
	cast(ltrim(rtrim(RIGHT(GenusId,CHARINDEX('||',REVERSE(GenusId))-1)))as Nvarchar(75)) as GenusID,
	cast(REPLACE(ad.GenusId,'PatientSummary||','')as Nvarchar(75)) as CMC_ID,
	cast(REPLACE(FromVersionId,'PatientSummary','PS')as Nvarchar(75))  as FromPatientSummary,
	cast(REPLACE(ToVersionId,'PatientSummary','PS')as Nvarchar(75))  as ToPatientSummary,
	ItemId as PatAuditID,
	ActionTime,
	ActionType,
	(select splitdata from (select splitdata,ROW_NUMBER() over (order by offset) as rn from [ETL_Local_PROD].[dbo].[SplitString](Actor,'|')) sel1 where rn=1) as StaffRegistryId,
	(select splitdata from (select splitdata,ROW_NUMBER() over (order by offset) as rn from [ETL_Local_PROD].[dbo].[SplitString](Actor,'|')) sel1 where rn=3) as DeptPDRegistryId,
	cast((select splitdata from (select splitdata,ROW_NUMBER() over (order by offset) as rn from [ETL_Local_PROD].[dbo].[SplitString](Actor,'|')) sel1 where rn=5)as Nvarchar(100))  as Role,
	-- Add CMC_AuditLogin linkage info MS 4.7.16
	LoginRowId as LoginReference
	into #AuditPatient
 
	from ETL_PROD.dbo.CMC_AuditData ad 
 
	Where 
	 not exists (select PatAuditID from [ETL_Local_PROD].[dbo].[AuditPatient_New] z where ad.ItemId = z.PatAuditID)
	and  (Actor <> 'System' and ActionType not in ('Login','Logout'))
	and RecordName = 'PatientSummary'
	--and ltrim(rtrim(RIGHT(GenusId,CHARINDEX('||',REVERSE(GenusId))-1))) in (select distinct GenusId from [ETL_Local_PROD].[dbo].[AT_CarePlanVersion])
 
	--select top 50 * from #AuditPatient

--	ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_New] 
--DROP COLUMN AuditDataType;



	insert into [ETL_Local_PROD].[dbo].[AuditPatient_New] 

	SELECT
	GenusID,
	CMC_ID,
	FromPatientSummary,
	ToPatientSummary,
	PatAuditID,
	ActionTime,
	ActionType,
	StaffRegistryId,
	DeptPDRegistryId,
	Role,
	LoginReference,
	null as OverAllOrder,
	null as ActionTypeOrder
  FROM #AuditPatient x

  Where not exists (select PatAuditID from [ETL_Local_PROD].[dbo].[AuditPatient_New] z where x.PatAuditID = z.PatAuditID)

 

 update r
		set r.OverAllOrder = b.OverAllOrder,
			r.ActionTypeOrder = b.ActionTypeOrder

 from  [ETL_Local_PROD].[dbo].[AuditPatient_New] r
 left join
 (
 select
 PatAuditID,
 CMC_ID,
 ActionTime,
 ActionType,
ROW_NUMBER() over (PARTITION by CMC_ID  order by ActionTime,PatAuditID)  as OverAllOrder,
ROW_NUMBER() over (PARTITION by CMC_ID,ActionType order by ActionTime,PatAuditID) as ActionTypeOrder
 from  [ETL_Local_PROD].[dbo].[AuditPatient_New] 
 )as b on b.PatAuditID = r.PatAuditID







 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AuditPatient_CarePlan_New]') is not null
 drop table [ETL_Local_PROD].[dbo].[AuditPatient_CarePlan_New]
 select
REPLACE(ad.GenusId,'CarePlan||','') as CMC_ID,
FromVersionId as FromCarePlan,
ToVersionId as ToCarePlan,
ItemId as CPAuditID,
null as OverAllOrder,
null as ActionTypeOrder,
ActionTime,
ActionType,
(select splitdata from (select splitdata,ROW_NUMBER() over (order by offset) as rn from [ETL_Local_PROD].[dbo].[SplitString](Actor,'|')) sel1 where rn=1) as StaffRegistryId,
(select splitdata from (select splitdata,ROW_NUMBER() over (order by offset) as rn from [ETL_Local_PROD].[dbo].[SplitString](Actor,'|')) sel1 where rn=3) as DeptPDRegistryId,
(select splitdata from (select splitdata,ROW_NUMBER() over (order by offset) as rn from [ETL_Local_PROD].[dbo].[SplitString](Actor,'|')) sel1 where rn=5) as Role

into [ETL_Local_PROD].[dbo].[AuditPatient_CarePlan_New]

from  ETL_PROD.dbo.CMC_AuditData ad where Actor <> 'System' and ActionType not in ('Login','Logout') and RecordName = 'CarePlan' 


--select top 500 * from [ETL_Local_PROD].[dbo].[AuditPatient_New] order by CMC_ID, overallOrder
--select top 500 * from [ETL_Local_PROD].[dbo].[AuditPatient_CarePlan_New] order by CMC_ID, overallOrder
 update r
		set r.OverAllOrder = b.OverAllOrder,
			r.ActionTypeOrder = b.ActionTypeOrder

 from  [AuditPatient_CarePlan_New] r
 left join
 (
 select
 CPAuditID,
 CMC_ID,
 ActionTime,
 ActionType,
ROW_NUMBER() over (PARTITION by CMC_ID  order by ActionTime,CPAuditID)  as OverAllOrder,
ROW_NUMBER() over (PARTITION by CMC_ID,ActionType order by ActionTime,CPAuditID) as ActionTypeOrder
 from  [ETL_Local_PROD].[dbo].[AuditPatient_CarePlan_New]
 )as b on b.CPAuditID = r.CPAuditID


--view [Reporting].[Cache-AccessDataDetail]

  if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_AccessDataDetail]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_AccessDataDetail] 
	select
	PatAuditID as ADAuditID,
	OverAllOrder,
	ActionTypeOrder,
	ap.cmc_id,
	ActionTime as ActivityDate,
	ActionType as [Access Type],
	s.StaffEnterpriseId,
	ISNULL(s.StaffForename + ' ','') + ISNULL(s.StaffSurname,'') as [User Name],
	d1.DeptEnterpriseId as ActivityEnterpriseId,
	d1.DeptName as name,
	d1.LocalCMCOrgTypeDescription as TeamType,
	s.StaffActiveDescription as Active_Status,
	s.StaffProviderTypeDescription as UserType,
	io.CMCRoleDescription,
	ap.Role,
	-- MS 11.3.16 add version information
	ap.FromPatientSummary, ap.ToPatientSummary

	into [ETL_Local_PROD].[dbo].[AT_AccessDataDetail] 

	from [ETL_Local_PROD].[dbo].[AuditPatient_New]  ap
	join [ETL_Local_PROD].[dbo].[AT_Staff] s on s.StaffRegistryId = ap.StaffRegistryId
	join [ETL_Local_PROD].[dbo].[AT_PD_Dept] d1 on d1.DeptPDRegistryId = ap.DeptPDRegistryId
	join [ETL_Local_PROD].[dbo].[AT_IndToOrg] io on io.StaffEnterpriseId = s.StaffEnterpriseId and io.DeptEnterpriseId = d1.DeptEnterpriseId


 

 
 
  /** table Work...


  ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_CarePlan_New]
   ADD CONSTRAINT PK_AudCarePlan PRIMARY KEY CLUSTERED (CPAuditID);


     CREATE INDEX CMCID_OverAllOrder
ON [ETL_Local_PROD].[dbo].[AT_AccessDataDetail]  (CMC_ID, OverAllOrder);



  ALTER INDEX PK_AuditTransactionOrder_ItemID ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  DISABLE
  go
  ALTER INDEX PK_AuditTransactionOrder_ItemID ON [ETL_Local_PROD].[dbo].[AuditPatient_New] REBUILD
  go

  select top 5000 * from [ETL_Local_PROD].[dbo].[AuditPatient_New] 
  SELECT MAX(LEN(Role)) FROM [ETL_Local_PROD].[dbo].[AuditPatient_New] 

  ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_CarePlan_New]
  ADD 
		AuditDataType Nvarchar(50);
      OverAllOrder int, 
	 ActionTypeOrder int;
	

  ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_New] 
  ALTER COLUMN Role NVARCHAR(100) NOT NULL;

    ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_New] 
  ALTER COLUMN ToPatientSummary NVARCHAR(75) ;

      ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_CarePlan_New]
  ALTER COLUMN [ToCarePlan] NVARCHAR(75) ;

  CREATE INDEX CMCID_ActionType
ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  (CMC_ID, ActionType);

  CREATE INDEX Role_ActionType
ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  (Role, ActionType);

  CREATE INDEX CareRecord_Date
ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  (CMC_ID, ActionTime);

  CREATE INDEX CarePlan_PatAuditIndex
ON [AuditPatient_CarePlan_New]  (CMC_ID, CPAuditID);


  ALTER TABLE [Sales].[SalesOrderDetail] ADD  CONSTRAINT [PK_SalesOrderDetail_SalesOrderID_SalesOrderDetailID] PRIMARY KEY CLUSTERED 
(
    [SalesOrderID] ASC,
    [SalesOrderDetailID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

**/







   if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_CMCID]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_CMCID]
  select 
Patient, 
cast(n.PatientNumber as bigint) as CMC_ID 
into [ETL_Local_PROD].[dbo].[AT_CMCID]
from ETL_PROD.dbo.CMC_Patient_PatientNumbers pn
join ETL_PROD.dbo.CMC_PatientNumber n on n.ItemId = pn.PatientNumber
where AssigningAuthority = 'CMC'

 



if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_NHSNumbers]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_NHSNumbers]
select 
Patient, 
n.PatientNumber as NHS_Number 
into [ETL_Local_PROD].[dbo].[AT_NHSNumbers]
from ETL_PROD.dbo.CMC_Patient_PatientNumbers pn
join ETL_PROD.dbo.CMC_PatientNumber n on n.ItemId = pn.PatientNumber
where AssigningAuthority = 'NHS'







	if OBJECT_ID ('Tempdb..#PatTele') is not null
	drop table #PatTele
		SELECT
		*
		INTO #PatTele
		FROM
		(
		  SELECT
		no.PatientNumber as CMC_ID,
		ps.Patient,
		ContactType,ContactValue,
		ROW_NUMBER() over (partition by no.PatientNumber,ContactType order by pa.PATIENT DESC) as ContactNo,
		ct.Description as ContactTypeDescription
		from ETL_PROD.dbo.CMC_PatientSummary ps
		join ETL_PROD.dbo.CMC_Patient p on ps.Patient = p.ItemId
		join ETL_PROD.dbo.CMC_Patient_PatientNumbers po on po.Patient = ps.ItemID
		join ETL_PROD.dbo.CMC_PatientNumber no on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
		join ETL_PROD.dbo.CMC_Patient_ContactInfo pa on p.itemid = pa.Patient
		join ETL_PROD.dbo.CMC_ContactInfo a on a.ItemID = pa.ContactInfo 
		left join ETL_PROD.dbo.Coded_ContactType ct on a.ContactType = ct.code

		--where no.PatientNumber = 100013863
		)D WHERE ContactNo = 1

		--select * from #PatTele  where cmc_id = 100007061
		--select * from [ETL_Local_PROD].[dbo].[AT_Patient_General] where cmc_id = 100007061
		--select * from (select cmc_id,Count(*) as Total from [ETL_Local_PROD].[dbo].[AT_Patient_General] group by CMC_ID )d where d.Total >1 



--Multiple lines per patient  this is eliminated by the current CMC record (derived from #Versions (each cmc care record for each pateint has a different patient number (ps.Patient)
	if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_PatAddress]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_PatAddress]

	SELECT
	Address,
	Null as Main_LastAddressflag,
	Null as Second_LastAddressflag,
	Null as Crr_Temp_LastAddressflag,
	no.PatientNumber as CMC_ID,
	ROW_NUMBER() over (partition by no.PatientNumber order by ps.Patient) as PatientNumber,
 
	ps.Patient,
	nm.GivenName + ' ' + Nm.FamilyName as PatName,
	a.StreetLine, 
	a.Line2, 
	a.City, 
	a.County, 
	a.PostalCode,
	a.CCAddressUse, 
	a.CCDwellingType,
	a.CCFromTime, 
	a.CCToTime,
	a.CCLivingConditions,
	a.CCKeySafeDetails, 
	a.CCResidenceNotes,
	ISNULL(a.StreetLine,'') +
	  case 
		when a.StreetLine is not null and rtrim(a.StreetLine) <> '' and a.Line2 is not null and rtrim(a.Line2) <> ''then ', ' 
			else '' 
			end + ISNULL(a.Line2,'') + 
		case 
			when a.City is not null and rtrim(a.City) <> '' then ', ' 
			else '' end +ISNULL(a.City,'') + 
		case 
			when a.County is not null and rtrim(a.County) <> '' then ', ' 
			else '' end +ISNULL(a.County,'') 
	as CombinedAddress,
 
	ROW_NUMBER() over (partition by ps.Patient order by Address,a.CCFromTime) as AddressNo,
	au.Description as CCAddressUseDescription,
	dt.Description as CCDwellingTypeDescription,
	-- MS 19.2.16 add living conditions description
	lc.Description as CCLivingConditionsDescription

	into [ETL_Local_PROD].[dbo].[AT_PatAddress]

	from ETL_PROD.dbo.CMC_PatientSummary ps
	join ETL_PROD.dbo.CMC_Patient p on ps.Patient = p.ItemId
	join ETL_PROD.dbo.CMC_Patient_PatientNumbers po on po.Patient = ps.ItemID
	join ETL_PROD.dbo.CMC_PatientNumber no on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
	join ETL_PROD.dbo.CMC_Patient_Addresses pa on p.itemid = pa.Patient
	join ETL_PROD.dbo.CMC_Address a on pa.Address = a.ItemID
	left join ETL_PROD.dbo.Coded_AddressUse au on a.CCAddressUse = au.code
	left join ETL_PROD.dbo.Coded_DwellingType dt on a.CCDwellingType = dt.code
	left join ETL_PROD.dbo.Coded_LivingConditions lc on a.CCLivingConditions = lc.code
	left join [ETL_PROD].[dbo].[CMC_Name]Nm on nm.ItemId = p.ItemId
	--WHERE no.PatientNumber IN (
	--							100015671, --This returns two rows for the same person but it seems one is generates for each care plan
	--							100015678
	--							)
 
	 ORDER BY no.PatientNumber, a.StreetLine



	 --select top 500 * from ETL_PROD.dbo.CMC_Patient where name in ( 'PS||100007994||1','PS||100007994||3')






	 update r
			set r.Main_LastAddressflag = case  when s.LastAddress is not null then 1 else null end,
				r.Second_LastAddressflag = case  when t.LastAddress is not null then 1 else null end,
				r.Crr_Temp_LastAddressflag = case  when u.LastAddress is not null then 1 else null end

	 from [ETL_Local_PROD].[dbo].[AT_PatAddress] r
	 left join 
				(
				select
				CMC_ID,
				max(PatientNumber) as LastAddress

				from [ETL_Local_PROD].[dbo].[AT_PatAddress]
				where CCAddressUse = 'MAIN'
			
				group by CMC_ID
				)s on s.CMC_ID = r.CMC_ID 
				and s.LastAddress = r.PatientNumber
	 left join 
				(
				select
				CMC_ID,
				max(PatientNumber) as LastAddress

				from [ETL_Local_PROD].[dbo].[AT_PatAddress]
				where CCAddressUse = 'SECO'
				group by CMC_ID
				)t on t.CMC_ID = r.CMC_ID 
				and t.LastAddress = r.PatientNumber
	 left join 
				(
				select
				CMC_ID,
				max(PatientNumber) as LastAddress

				from [ETL_Local_PROD].[dbo].[AT_PatAddress]
				where CCAddressUse in ('CURR','TEMP')
				group by CMC_ID
				)u on u.CMC_ID = r.CMC_ID 
				and u.LastAddress = r.PatientNumber




	 --select * from #PatAddress where Patient = 'PS||100011841||4'
	--select *  from [ETL_Local_PROD].[dbo].[AT_PatAddress] where cmc_id = 100007994
	--			where 
	--			--CCAddressUse = 'MAIN'
	--			--and 
	--			cmc_id = 100007994

 





   if OBJECT_ID ('Tempdb..#PatLPA') is not null
 drop table #PatLPA

 SELECT
no.PatientNumber as CMC_ID,PatientSummary,
Name,FromTime,Comments,
SupportContact,
ROW_NUMBER() over (partition by patientsummary order by SupportContact) as LPANo

into #PatLPA

from ETL_PROD.dbo.CMC_PatientSummary ps
join ETL_PROD.dbo.CMC_Patient_PatientNumbers po on po.Patient = ps.ItemID
join ETL_PROD.dbo.CMC_PatientSummary_SupportContacts psc on ps.ItemId = psc.PatientSummary
join ETL_PROD.dbo.CMC_SupportContact sc on psc.SupportContact = sc.itemid
join ETL_PROD.dbo.CMC_PatientNumber no on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
where SupportContactType = 'POA'

--select * from #PatLPA where cmc_ID = 100013863

--  if OBJECT_ID ('Tempdb..#PatTele') is not null
-- drop table #PatTele
-- SELECT
--CMC_ID, p.SupportContact, LPANo,
--i.ContactType, i.ContactValue,
--ROW_NUMBER() over (partition by cmc_id,LPANo order by ContactInfo) as ContactInfoNo

--into #PatTele

--from #PatLPA p
--join ETL_PROD.dbo.CMC_SupportContact_ContactInfo sci on p.SupportContact = sci.SupportContact
--join ETL_PROD.dbo.CMC_ContactInfo i on sci.ContactInfo = i.ItemID
 --select * from #PatLPA
 --select * from #PatTele
  --select * from #PatTele where cmc_ID = 100013863
  --100013863




 


--SELECT * FROM ETL_PROD.dbo.CMC_ContactInfo WHERE ItemID =   100013863

--SELECT 
--* 
--FROM ETL_PROD.dbo.CMC_Patient P
--LEFT JOIN ETL_PROD.dbo.CMC_Patient_ContactInfo pa ON p.itemid = pa.Patient
--join ETL_PROD.dbo.CMC_ContactInfo a on a.ItemID = pa.ContactInfo 

 


--#PDDept


 
if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_AddressGP]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_AddressGP]
select 
case when d.AddressOrder = 1 then 1 else null end as CurrentAddressFlag,
d.*
into [ETL_Local_PROD].[dbo].[AT_AddressGP]
from
(
SELECT 
distinct 
convert(date,a.creationDateTime) as CreationDate,
convert(date,ds.StartDate) as StartDate,
convert(date,ds.EndDate) as EndDate,
row_number() over (partition by DeptEnterpriseId order by coalesce(a.creationDateTime,convert(date,ds.StartDate))desc) as AddressOrder,
DeptEnterpriseId,
a.StreetLine, 
a.Line2, 
a.City, 
a.County, 
a.PostalCode,
ISNULL(a.StreetLine,'') +
  case when a.StreetLine is not null and rtrim(a.StreetLine) <> '' 
       and a.Line2 is not null and rtrim(a.Line2) <> ''
       then ', ' else '' end + 
ISNULL(a.Line2,'') + case when a.City is not null and rtrim(a.City) <> '' then ', ' else '' end +
ISNULL(a.City,'') + case when a.County is not null and rtrim(a.County) <> '' then ', ' else '' end +
ISNULL(a.County,'') as CombinedAddress

FROM [ETL_PROD].[dbo].[CMC_Location] lo join [ETL_Local_PROD].[dbo].[AT_PD_Dept] d on lo.OrganizationEID = deptenterpriseid
join ETL_PROD.dbo.CMC_Address a on lo.AddressEID = a.PDEnterpriseID
left join etl_PROD.dbo.CMC_Location_DateSpan lods on lo.ItemId = lods.Location
left join etl_PROD.dbo.CMC_DateSpan ds on lods.DateSpan = ds.ItemId
-- ensure ind->org info is omitted from this view MS 6.4.16
where lo.IndividualEID is null
-- exclude deleted and expired/unstarted locations MS 25.8.16
and lo.Deleted is null
and (ds.StartDate is null or CAST(startdate as date) <= CAST(getdate() as DATE))
and (ds.EndDate is null or  CAST(enddate as date) > CAST(getdate() as DATE))

 
) d 



if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_Emails]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_Emails]
select
 distinct
 
 case 
 when em.Address like '%nhs.net' then row_number() over (partition by DeptEnterpriseId order by Email) else null end as EmailOrder , 
 DeptEnterpriseId,
 em.Address as Email,
 convert(date,ds.StartDate) as StartDate,
 convert(date,ds.EndDate) as EndDate

 into [ETL_Local_PROD].[dbo].[AT_Emails]

  FROM [ETL_PROD].[dbo].[CMC_Location] lo 
  join [ETL_Local_PROD].[dbo].[AT_PD_Dept] d on lo.OrganizationEID = deptenterpriseid
-- change CMC_Location_Contacts/CMC_ContactInfo_Emails to CMC_Location_Emails
  join ETL_PROD.dbo.CMC_Location_Emails cp on cp.Location = Lo.ItemId
  join ETL_PROD.dbo.CMC_Email em on cp.Email = em.ItemId
left join ETL_PROD.dbo.CMC_Location_DateSpan lods on lo.ItemId = lods.Location
left join ETL_PROD.dbo.CMC_DateSpan ds on lods.DateSpan = ds.ItemId
-- ensure ind->org info is omitted from this view MS 6.4.16
where lo.IndividualEID is null
-- exclude deleted and expired/unstarted locations MS 25.8.16
and lo.Deleted is null
-- exclude FLAGGING emails MS 17.2.17
and em.TypeCodedValue <> 'FLAGGING'
and (ds.StartDate is null or CAST(startdate as date) <= CAST(getdate() as DATE))
and (ds.EndDate is null or  CAST(enddate as date) > CAST(getdate() as DATE))







 if OBJECT_ID ('Tempdb..#TelephoneGP') is not null
 drop table #TelephoneGP

SELECT 
distinct 
case when left(isnull(ph.FullNumber,ph.TelephoneNumber) ,2) <> '07' then row_number() over (partition by DeptEnterpriseId order by case when tt.Description = 'Business Phone' then 1 else left(isnull(ph.FullNumber,ph.TelephoneNumber),5) end )else null end as No_07_PhoneOrder,
case when left(isnull(ph.FullNumber,ph.TelephoneNumber) ,2) = '07' then row_number() over (partition by DeptEnterpriseId order by case when tt.Description = 'Business Phone' then 1 else left(isnull(ph.FullNumber,ph.TelephoneNumber),5) end )else null end as Yes_07_PhoneOrder,
DeptEnterpriseId, 
isnull(ph.FullNumber,ph.TelephoneNumber) as Telephone, 
tt.Description as TelephoneType,
convert(date, creationDateTime) as CreationDate,
convert(date,ds.StartDate) as StartDate,
convert(date,ds.EndDate) as EndDate

into #TelephoneGP

  FROM [ETL_PROD].[dbo].[CMC_Location] lo join PDDepartment d on lo.OrganizationEID = deptenterpriseid
-- change CMC_Location_Contacts/CMC_ContactInfo_Phones to CMC_Location_Phones  
  join ETL_PROD.dbo.CMC_Location_Phones cp on cp.Location = Lo.ItemId
  join ETL_PROD.dbo.CMC_Telecom ph on cp.Phone = ph.ItemId
left join ETL_PROD.dbo.CMC_Location_DateSpan lods on lo.ItemId = lods.Location
left join ETL_PROD.dbo.CMC_DateSpan ds on lods.DateSpan = ds.ItemId
left join etl_prod.dbo.Coded_TelecomType tt on tt.Code = ph.TypeCodedValue
-- ensure ind->org info is omitted from this view MS 6.4.16
where lo.IndividualEID is null
-- exclude deleted and expired/unstarted locations MS 25.8.16
and lo.Deleted is null
and (ds.StartDate is null or CAST(ds.startdate as date) <= CAST(getdate() as DATE))
and (ds.EndDate is null or  CAST(ds.enddate as date) > CAST(getdate() as DATE))

order by DeptEnterpriseId


--A dynamic column table for the symptoms

 if OBJECT_ID ('Tempdb..#Sample') is not null
 drop table  #Sample

CREATE TABLE #Sample
(
    CMC_ID varchar(max)
    , CarePlan varchar(36)
    , Symptom varchar(255)
    , SymptomNo  Int
	,SymptomDescription varchar(max)
);

DECLARE @SQL VARCHAR(MAX) = 'SELECT CMC_ID';

INSERT INTO #Sample  
SELECT
no.PatientNumber as CMC_ID,
pscp.CarePlan,
sp.Symptom,
--sp.NoteText,
convert(int,ROW_NUMBER() over (partition by pscp.CarePlan order by sp.Symptom)) as SymptomNo,
sc.Description as SymptomDescription
from ETL_PROD.dbo.CMC_PatientSummary ps
join ETL_PROD.dbo.CMC_Patient p on ps.Patient = p.ItemId
join ETL_PROD.dbo.CMC_Patient_PatientNumbers po on po.Patient = ps.ItemID
join ETL_PROD.dbo.CMC_PatientNumber no on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
join ETL_PROD.dbo.CMC_PatientSummary_CarePlan pscp on pscp.PatientSummary = ps.ItemId
join ETL_PROD.dbo.CMC_CarePlan_SymptomPlans cps on pscp.CarePlan = cps.CarePlan
join ETL_PROD.dbo.CMC_SymptomPlan sp on cps.SymptomPlan = sp.ItemId
left join ETL_PROD.dbo.Coded_Symptom sc on sp.Symptom = sc.code
where sp.NoteText is not null
--and  no.PatientNumber = '100022416'

;
if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_Symptoms]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_Symptoms]
SELECT @SQL += '
    , MAX(CASE WHEN SymptomNo = ' + convert(varchar,SymptomNo) + ' THEN Symptom END) AS [SymptomNo_' + convert(varchar,SymptomNo) + ']
    , MAX(CASE WHEN SymptomNo = ' + convert(varchar,SymptomNo) + ' THEN SymptomDescription END) AS [SymptomDescription_' + convert(varchar,SymptomNo) + ']'
FROM (SELECT DISTINCT  SymptomNo  AS SymptomNo FROM #Sample) AS T
ORDER BY   T.SymptomNo  ;

SET @SQL += ' into [ETL_Local_PROD].[dbo].[AT_Symptoms] FROM #Sample GROUP BY CMC_ID;';


EXECUTE (@SQL);

DROP TABLE #Sample;

--select * from ##Symptoms

--drop table ##Symptoms

if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_PatDiagnosis]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_PatDiagnosis]
 
	SELECT
	no.PatientNumber as CMC_ID,
	ps.Patient,
	DiagnosisCode,
	MainDiagnosis,
	OnsetTime,
	ToTime,
	FamilyAware,
	PatientAware,
	Comments,
	DiagnosisCategory,
	ROW_NUMBER() over (partition by patientsummary order by Diagnosis) as DiagnosisNo,
	ROW_NUMBER() over (partition by ps.Patient order by case when MainDiagnosis =1 then 1 else 0 end desc,case when dcc.Description <>'Migrated' then dia.ItemId else 'Q' end) as PatDiagRN,--For Final patient Table
	-- Add descriptions MS 18.2.16
	dic.Description as DiagnosisDescription,
	dcc.Description as DiagnosisCategoryDescription,
	-- Add diagnosis item id MS 12.9.16
	dia.ItemId as DiagnosisItemId 

	into [ETL_Local_PROD].[dbo].[AT_PatDiagnosis]

	from ETL_PROD.dbo.CMC_PatientSummary ps
	join ETL_PROD.dbo.CMC_Patient_PatientNumbers po on po.Patient = ps.ItemID
	join ETL_PROD.dbo.CMC_PatientSummary_Diagnoses pdia on ps.ItemId = pdia.PatientSummary
	join ETL_PROD.dbo.CMC_Diagnosis dia on pdia.Diagnosis = dia.ItemId
	join ETL_PROD.dbo.CMC_PatientNumber no on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
	join ETL_PROD.dbo.Coded_Diagnosis_DiagnosisCode dic on dia.DiagnosisCode = dic.Code
	join ETL_PROD.dbo.Coded_DiagnosisCategory dcc on dia.DiagnosisCategory = dcc.Code


---Patient alerts also has multiple rowsa - one for every cmc record created by patient
 if OBJECT_ID ('Tempdb..#PatAlert') is not null
 drop table #PatAlert
SELECT
no.PatientNumber as CMC_ID,
ps.Patient,
a.Alert,AlertType,FromTime,ToTime,Comments,
ROW_NUMBER() over (partition by patientsummary order by pa.Alert) as AlertNo,
ac.Description as AlertDescription,
at.Description as AlertTypeDescription

into #PatAlert

from ETL_PROD.dbo.CMC_PatientSummary ps
join ETL_PROD.dbo.CMC_Patient_PatientNumbers po on po.Patient = ps.ItemID
join ETL_PROD.dbo.CMC_PatientSummary_Alerts pa on ps.ItemId = pa.PatientSummary
join ETL_PROD.dbo.CMC_Alert a on pa.Alert = a.ItemId
join ETL_PROD.dbo.CMC_PatientNumber no on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
left join etl_PROD.dbo.Coded_AlertCode ac on a.alert = ac.code
left join etl_PROD.dbo.Coded_AlertType at on a.alerttype = at.code




 if OBJECT_ID ('Tempdb..#PatAlias') is not null
	drop table #PatAlias

	SELECT 
	sel1.*,
	ROW_NUMBER() over (partition by Patient order by Surname,Forename,MiddleName,Title) as AliasNo
	into #PatAlias
	from 
	(
		select 
		distinct
		no.PatientNumber as CMC_ID,
		ps.Patient,
		a.NamePrefix as Title,
		np.Description as TitleDescription,
		a.GivenName as Forename,
		a.MiddleName,
		a.FamilyName as Surname
		from ETL_PROD.dbo.CMC_PatientSummary ps
		join ETL_PROD.dbo.CMC_Patient p on ps.Patient = p.ItemId
		join ETL_PROD.dbo.CMC_Patient_PatientNumbers po on po.Patient = ps.ItemID
		join ETL_PROD.dbo.CMC_PatientNumber no on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
		join ETL_PROD.dbo.CMC_Patient_Aliases pa on p.itemid = pa.Patient
		join ETL_PROD.dbo.CMC_Name a on pa.Alias = a.ItemID
		left join ETL_PROD.dbo.Coded_NamePrefix np on a.NamePrefix = np.Code
	) sel1

	--select * from #PatAlias
	 
  if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_StaffDeptContext]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_StaffDeptContext]
 
select 
poc.ItemId as ProviderOrgContext, 
poc.Organization as POC_Org,
s.*, 
d.*

 into [ETL_Local_PROD].[dbo].[AT_StaffDeptContext]

from ETL_PROD.dbo.CMC_ProviderOrgContext poc
left join ETL_PROD.dbo.CMC_IndividualProvider ip on poc.Provider = ip.ItemId
left join ETL_PROD.dbo.CMC_Individual i on i.PDRegistryID = ip.RegistryID
left join [ETL_Local_PROD].[dbo].[AT_Staff] s on s.Individual = i.ItemID
left join [ETL_Local_PROD].[dbo].[AT_Dept]  d on d.Organization = poc.Organization  
-- Exclude LastClinicalApprover rows introduced in 15.1 release 
where ip.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
AND ip.ItemId not like 'PS|%|%|%|%|%|LCA'
AND poc.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
AND poc.ItemId not like 'PS|%|%|%|%|%|LCA'

 
 










   if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_Accurate_Entered_by]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_Accurate_Entered_by]
	SELECT [CMC_ID]
      ,PatAuditID as AuditId
      ,ToPatientSummary as [PatientSummary]
      ,[ActionTime]
      ,[ActionType]
       ,cast(null as Varchar(max)) as [DeptName]
      ,cast(null as Varchar(255)) as [OrganizationType]
      ,cast(null as Varchar(max)) as [OrganizationTypeDescription]
      ,cast(null as Varchar(36)) as [Organization]
      ,cast(null as Varchar(25)) as [DeptSource]
      ,null as [DeptEnterpriseID]
      ,[DeptPDRegistryID]
      ,cast(null as Varchar(255)) as [DeptLocalCMCId]
      ,cast(null as Varchar(255)) as [LocalCMCOrgType]
      ,cast(null as Varchar(max)) as [LocalCMCOrgTypeDescription]
      ,cast(null as Varchar(255)) as [DeptODSCode]
      ,cast(null as date) as [DeptOpenDate]
      ,cast(null as date) as [DeptCloseDate]
      ,null as [StaffEnterpriseID]
      ,cast(null as Varchar(36)) as [Individual]
      ,cast(null as Varchar(max)) as [StaffTitle]
      ,cast(null as Varchar(max)) as [StaffTitleDescription]
      ,cast(null as Varchar(max)) as [StaffForename]
      ,cast(null as Varchar(max)) as [StaffMiddleName]
      ,cast(null as Varchar(max)) as [StaffSurname]
      ,cast(null as Varchar(max)) as [StaffODSCode]
      ,cast(null as Varchar(max)) as [StaffLocalCMCId]
      ,cast(null as  date) as [StaffCreatedDate]
      ,cast(null as Varchar(255)) as [StaffActive]
      ,cast(null as Varchar(max)) as [StaffActiveDescription]
      ,cast(null as Varchar(max)) as [StaffDescription]
      ,cast(null as Varchar(255)) as [StaffProviderType]
      ,cast(null as Varchar(max)) as [StaffProviderTypeDescription]
      ,cast(null as Varchar(255)) as [StaffRegistryId]
      ,cast(null as Varchar(max)) as [StaffUserId]
      ,cast(null as Varchar(36)) as [StaffUserClinician]

	  into [ETL_Local_PROD].[dbo].[AT_Accurate_Entered_by]

	FROM [ETL_Local_PROD].[dbo].[AuditPatient_New]ap
  --left join [ETL_Local_PROD].[dbo].[AT_Staff] s on ap.StaffRegistryId = s.StaffRegistryId
  --left join [ETL_Local_PROD].[dbo].[AT_Dept] d on ap.DeptPDRegistryId = d.DeptPDRegistryId
	where ActionType in ('create','revise')



	update r
			
			set r.DeptName = d.DeptName,
				r.[OrganizationType] = d.OrganizationType,
				r.[OrganizationTypeDescription] = d.[OrganizationTypeDescription],
				r.[Organization] = d.[Organization],
				r.[DeptSource] = d.[DeptSource],
				r.[DeptEnterpriseID] = d.[DeptEnterpriseID],
				r.[DeptLocalCMCId] = d.[DeptLocalCMCId],
				r.[LocalCMCOrgType] = d.[LocalCMCOrgType],
				r.[LocalCMCOrgTypeDescription] = d.[LocalCMCOrgTypeDescription],
				r.[DeptODSCode] = d.[DeptODSCode],
				r.[DeptOpenDate] = d.[DeptOpenDate],
				r.[DeptCloseDate] = d.[DeptCloseDate]
			

	from [ETL_Local_PROD].[dbo].[AT_Accurate_Entered_by]r
	--left join [ETL_Local_PROD].[dbo].[AT_Staff] s on s.StaffRegistryId = r.StaffRegistryId
    left join [ETL_Local_PROD].[dbo].[AT_PD_Dept] d on d.DeptPDRegistryId = r.DeptPDRegistryId 




	update r
			
			set r.[StaffEnterpriseID] = s.[StaffEnterpriseID],
				r.[Individual] = s.[Individual],
				r.[StaffTitle] = s.[StaffTitle],
				r.[StaffTitleDescription] = s.[StaffTitleDescription],
				r.[StaffForename] = s.[StaffForename],
				r.[StaffMiddleName] = s.[StaffMiddleName],
				r.[StaffSurname] = s.[StaffSurname],
				r.[StaffODSCode] = s.[StaffODSCode],
				r.[StaffLocalCMCId] = s.[StaffLocalCMCId],
				r.[StaffCreatedDate] = s.[StaffCreatedDate],
				r.[StaffActive] = s.[StaffActive],
				r.[StaffActiveDescription] = s.[StaffActiveDescription],
				r.[StaffDescription] = s.[StaffDescription],
				r.[StaffProviderType] = s.[StaffProviderType],
				r.[StaffProviderTypeDescription] = s.[StaffProviderTypeDescription],
				r.[StaffRegistryId] = s.[StaffRegistryId],
				r.[StaffUserId] = s.[StaffUserId],
				r.[StaffUserClinician] = s.[StaffUserClinician]


	from [ETL_Local_PROD].[dbo].[AT_Accurate_Entered_by]r
	left join [ETL_Local_PROD].[dbo].[AT_Staff] s on s.StaffRegistryId = r.StaffRegistryId
   





  --select top 10000 *  from [ETL_Local_PROD].[dbo].[AT_Accurate_Entered_by]

--select top 10 * from [ETL_Local_PROD].[dbo].[AT_Staff] 
--select top 10 * from [ETL_Local_PROD].[dbo].[AT_Dept]
--select count(*)FROM [ETL_Local_PROD].[dbo].[AuditPatient_New] where ActionType in ('create','revise')
   /** table Work...


  ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_New]
   ADD CONSTRAINT PK_AuditTransactionOrder_ItemID PRIMARY KEY CLUSTERED (PatAuditID);

  ALTER INDEX PK_AuditTransactionOrder_ItemID ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  DISABLE
  go
  ALTER INDEX PK_AuditTransactionOrder_ItemID ON [ETL_Local_PROD].[dbo].[AuditPatient_New] REBUILD
  go

  select top 5000 * from [ETL_Local_PROD].[dbo].[AuditPatient_New] 
  SELECT MAX(LEN(Role)) FROM [ETL_Local_PROD].[dbo].[AuditPatient_New] 

  ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_New] 
  ADD 
      OverAllOrder int, 
	 ActionTypeOrder int;
	

  ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_New] 
  ALTER COLUMN Role NVARCHAR(100) NOT NULL;

    ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_New] 
  ALTER COLUMN ToPatientSummary NVARCHAR(75) ;

      ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_New] 
  ALTER COLUMN [GenusId] NVARCHAR(75) ;

  CREATE INDEX CMCID_ActionType
ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  (CMC_ID, ActionType);

  CREATE INDEX Role_ActionType
ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  (Role, ActionType);

  CREATE INDEX CareRecord_Date
ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  (CMC_ID, ActionTime);

  CREATE INDEX CareRecord_PatAuditIndex
ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  (CMC_ID, PatAuditID);


  ALTER TABLE [Sales].[SalesOrderDetail] ADD  CONSTRAINT [PK_SalesOrderDetail_SalesOrderID_SalesOrderDetailID] PRIMARY KEY CLUSTERED 
(
    [SalesOrderID] ASC,
    [SalesOrderDetailID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

**/

  --select * from #PatientHSCContacts where CMC_ID = 100013863


   if OBJECT_ID ('Tempdb..#PatientHSCContacts') is not null
	drop table #PatientHSCContacts

select 
sel1.*,
np1.Description as StaffTitleDescription,
np2.Description as NamePrefixDescription,
dpr.Description as RoleDescription,
dor.Description as OrgRoleDescription

into #PatientHSCContacts

from
	(
		SELECT
		no.PatientNumber as CMC_ID,
		ps.ItemID as PatientSummary,

		pa.Provider,
		a.StaffEnterpriseID,
		a.Individual,
		a.StaffTitle,
		a.StaffForename,
		a.StaffMiddleName,
		a.StaffSurname,
		a.StaffODSCode,
		a.StaffLocalCMCId,
		a.StaffCreatedDate,
		a.StaffActive,
		a.StaffDescription,
		DeptName,
		OrganizationType,
		OrganizationTypeDescription,
		a.Organization,
		a.DeptSource,
		DeptEnterpriseID,
		a.DeptPDRegistryID,
		a.DeptLocalCMCId,
		a.LocalCMCOrgType,
		a.LocalCMCOrgTypeDescription,
		a.DeptODSCode,
		a.DeptOpenDate,
		a.DeptCloseDate,
		b.Role,
		b.OrgRole,
		b.Comment,
		b.FromTime,
		b.ToTime,
		isnull(b.SelectedProviderCareProviderType,ip.CareProviderType) as CareProviderType,
		isnull(b.SelectedProviderNamePrefix,ip.NamePrefix) as NamePrefix,
		isnull(b.SelectedProviderGivenName,ip.GivenName) as GivenName,
		isnull(b.SelectedProviderFamilyName,ip.FamilyName) as FamilyName,
		b.SelectedOrgName,
		b.SelectedOrgType,
		case when b.MainHealthcareContact = 1 then 'Y' else 'N' end as MainHealthcareContact,
		ROW_NUMBER() over (partition by ps.ItemId order by pa.Provider) as ProviderNo

		from ETL_PROD.dbo.CMC_PatientSummary ps
		join ETL_PROD.dbo.CMC_Patient_PatientNumbers po on po.Patient = ps.ItemID
		join ETL_PROD.dbo.CMC_PatientNumber no on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
		join ETL_PROD.dbo.CMC_PatientSummary_Providers pa on ps.itemid = pa.PatientSummary
		join ETL_PROD.dbo.CMC_DocumentProvider b on pa.Provider = b.Provider
		left join [ETL_Local_PROD].[dbo].[AT_StaffDeptContext] a on pa.Provider = a.ProviderOrgContext
		left join ETL_PROD.dbo.CMC_IndividualProvider ip on ip.ItemId = b.Provider

		--where no.PatientNumber = 100013863

	) sel1
-- Add title and role descriptions MS 19.2.16
left join ETL_PROD.dbo.Coded_NamePrefix np1 on StaffTitle = np1.code
left join ETL_PROD.dbo.Coded_NamePrefix np2 on NamePrefix = np2.code
left join ETL_PROD.dbo.Coded_DocumentProviderRole dpr on Role = dpr.code
left join ETL_PROD.dbo.Coded_DocumentOrganizationRole dor on OrgRole = dor.code

 
 


  if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_PatientRegistered_GP]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_PatientRegistered_GP]

	select 
	CMC_ID, 
	PatientSummary, 
	RegisteredGP, 
	CCG,
	-- Add new CCG ODS field for all, not just London MS 7.3.16
	case 
		when CCG = 'NHS SURREY DOWNS CCG' then NULL 
			else DeptODSCode 
	end as London_CCG_ODS,
	-- This field populated only if not Unknown/Cross-Border
	DeptODSCode as CCG_ODS,
	case
	  when CCG = 'NHS SURREY DOWNS CCG' then 'Surrey Downs'
	  when CCG like 'Cross Border%' then 'Cross Border'
	  when CCG like 'Unknown%' then 'Unknown'
		else 'London'
	end as CommissioningArea,
	SurgeryEId,
	GPEId,
	Surgery

	into [ETL_Local_PROD].[dbo].[AT_PatientRegistered_GP]

	from
	(select
	cmc_id,
	c.PatientSummary,
	Provider as RegisteredGP,
	case
	-- workaround for West London
	  when h.name6 = 'NHS WEST LONDON CCG' then h.name6
	  when h.name5 = 'NHS WEST LONDON CCG' then h.name5
	  when h.typedesc6 = 'CCG' then h.name6
	  when h.typedesc5 = 'CCG' then h.name5
	  when h.name6 like '%CCG' then 'Cross Border: ' + h.name6
	  when h.name5 like '%CCG' then 'Cross Border: ' + h.name5
	-- workaround for org to org links that have gone walkies
	  when m.dept_parent like '%CCG' then
		case m.dept_parent
		  when 'NHS WEST LONDON (K&C & QPP) CCG' then 'NHS WEST LONDON CCG'
		  else m.dept_parent end 
	  else 'Unknown/Practice not a Practice: ' + isnull(h.name6,'not given') end as CCG,
	ROW_NUMBER() over (partition by c.patientsummary
-- MS 3.11.16 proper sequencing
	order by
	case when h.typedesc6='CCG' then 0 when h.typedesc5='CCG' then 0 else 1 end,
	h.name6,h.name5) as prn,
	DeptEnterpriseId as SurgeryEId,
	StaffEnterpriseId as GPEId,
	DeptName as Surgery
	from #PatientHSCContacts c
	left join  [ETL_Local_PROD].[dbo].[AT_Dept_Heirarchy] h on c.DeptEnterpriseID = h.eid7
	left join Protocol.ODSReconciliationCandidates m on c.DeptLocalCMCId = m.dept_id
	where role = 'REG'
	-- take end date into account MS 21.3.16
	and (c.ToTime is null or cast(c.ToTime as date) > CAST(getdate() as DATE))
	and (c.FromTime is null or cast(c.FromTime as date) <= CAST(getdate() as DATE))
	) sel1
	-- Remove any possible duplicates MS 21.3.16
	left join (select *,ROW_NUMBER() over (PARTITION by deptname
	-- MS 3.11.16 proper sequencing
	order by deptenterpriseid) as drn from [ETL_Local_PROD].[dbo].[AT_PD_Dept]) d
	  on sel1.CCG = d.DeptName
	where (drn=1 or drn is null) and prn=1

	 
	if OBJECT_ID ('Tempdb..#PatientCPRApprovers') is not null
	drop table #PatientCPRApprovers
	select CPR,
	apr.ApprovalTime,
	apr.ApproverName,ROW_NUMBER() OVER (partition by cpr order by approvaltime) as rn 
	into #PatientCPRApprovers
	from ETL_PROD.dbo.CMC_CPR_Approvers ap 
	left join ETL_PROD.dbo.CMC_CPRApprover apr on ap.Approver = apr.itemid

 
 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 
----------------------
	 if OBJECT_ID ('Tempdb..#IndOrgEmails') is not null
	 drop table #IndOrgEmails
		SELECT 
		distinct 
		StaffEnterpriseId,
		DeptEnterpriseId,
		em.Address as Email,
		-- Add email location MS 5.2.17
		lo.EnterpriseID as EmailLocation,
		-- Add isPrimary flag MS 19.3.17
		em.isPrimary
		into #IndOrgEmails
		  FROM [ETL_PROD].[dbo].[CMC_Location] lo join Ind2Org io on lo.OrganizationEID = io.deptenterpriseid and lo.IndividualEID = io.StaffEnterpriseID
		-- change CMC_Location_Contacts/CMC_ContactInfo_Emails to CMC_Location_Emails  
		  join ETL_PROD.dbo.CMC_Location_Emails cp on cp.Location = Lo.ItemId
		  join ETL_PROD.dbo.CMC_Email em on cp.Email = em.ItemId
		-- Active ind2orgs only MS 21.2.17
		  where (StartDate is null or CAST(startdate as date) <= CAST(getdate() as date))
		  and (endDate is null or CAST(enddate as date) > CAST(getdate() as date))

-------------------------
--ALTER view [dbo].[PDDeptEmails] as
-- Amended for PD Upgrade
	if OBJECT_ID ('Tempdb..#PDDeptEmails') is not null
	drop table #PDDeptEmails
	SELECT 
	distinct DeptEnterpriseId,
	em.Address as Email

	into #PDDeptEmails

	FROM [ETL_PROD].[dbo].[CMC_Location] lo join PDDepartment d on lo.OrganizationEID = deptenterpriseid
	-- change CMC_Location_Contacts/CMC_ContactInfo_Emails to CMC_Location_Emails
	  join ETL_PROD.dbo.CMC_Location_Emails cp on cp.Location = Lo.ItemId
	  join ETL_PROD.dbo.CMC_Email em on cp.Email = em.ItemId
	left join ETL_PROD.dbo.CMC_Location_DateSpan lods on lo.ItemId = lods.Location
	left join ETL_PROD.dbo.CMC_DateSpan ds on lods.DateSpan = ds.ItemId
	-- ensure ind->org info is omitted from this view MS 6.4.16
	where lo.IndividualEID is null
	-- exclude deleted and expired/unstarted locations MS 25.8.16
	and lo.Deleted is null
	-- exclude FLAGGING emails MS 17.2.17
	and em.TypeCodedValue <> 'FLAGGING'
	and (ds.StartDate is null or CAST(startdate as date) <= CAST(getdate() as DATE))
	and (ds.EndDate is null or  CAST(enddate as date) > CAST(getdate() as DATE))


 


--ALTER view [dbo].[Cache-AuditApprovals] as
-- Amended for PD Upgrade

--ALTER view [dbo].[Cache-WorkbaseDQInfo] as


	if OBJECT_ID ('Tempdb..#WorkbaseDQInfo') is not null
	drop table #WorkbaseDQInfo
		select 
		[DeptKey]
      ,[DeptName]
      ,[OrganizationType]
      ,[OrganizationTypeDescription]
      ,[Organization]
      ,[DeptSource]
      ,[DeptEnterpriseID]
      ,[DeptPDRegistryID]
      ,[DeptLocalCMCId]
      ,[LocalCMCOrgType]
      ,[LocalCMCOrgTypeDescription]
      ,[DeptODSCode]
      ,[DeptOpenDate]
      ,[DeptCloseDate]

		,WorkbaseEmail
		,WorkbaseDeptEId 

		into #WorkbaseDQInfo

		from [ETL_Local_PROD].[dbo].[AT_PD_Dept] d
		left join
				  (
				  select 
				  Email as WorkbaseEmail, 
				  DeptEnterpriseId as WorkbaseDeptEid,
					 ROW_NUMBER() over (PARTITION by DeptEnterpriseId order by DeptEnterpriseId) as rn
				   from #PDDeptEmails
				   ) we on d.DeptEnterpriseID = WorkbaseDeptEId and we.rn=1


--ALTER view [dbo].[Cache-AssessorDQInfo] as
-- Amended for PD Upgrade
-- Add organisation MS 3.11.16
-- Correct de-dup handling MS 19.2.17
	--if OBJECT_ID ('Tempdb..#AssessorDQInfo') is not null
	--drop table #AssessorDQInfo
	  --if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_AssessorDQInfo]') is not null
	truncate table [ETL_Local_PROD].[dbo].[AT_AssessorDQInfo]
	insert into [ETL_Local_PROD].[dbo].[AT_AssessorDQInfo]

		select 
		distinct 
	[Individual]+cast([DeptKey] as varchar(400)) as DQKey
	,s.[StaffEnterpriseID]
      ,[Individual]
      ,[StaffTitle]
      ,[StaffTitleDescription]
      ,[StaffForename]
      ,[StaffMiddleName]
      ,[StaffSurname]
      ,[StaffODSCode]
      ,[StaffLocalCMCId]
      ,[StaffCreatedDate]
      ,[StaffActive]
      ,[StaffActiveDescription]
      ,[StaffDescription]
      ,[StaffProviderType]
      ,[StaffProviderTypeDescription]
      ,[StaffRegistryId]
      ,[StaffUserId]
      ,[StaffUserClinician]
	,[DeptKey]
      ,[DeptName]
      ,[OrganizationType]
      ,[OrganizationTypeDescription]
      ,[Organization]
      ,[DeptSource]
      ,d.[DeptEnterpriseID]
      ,[DeptPDRegistryID]
      ,[DeptLocalCMCId]
      ,[LocalCMCOrgType]
      ,[LocalCMCOrgTypeDescription]
      ,[DeptODSCode]
      ,[DeptOpenDate]
      ,[DeptCloseDate]
		,AssessorEmail 
 
		from [ETL_Local_PROD].[dbo].[AT_IndToOrg] sd
		join [AT_Staff] s on sd.StaffEnterpriseID = s.StaffEnterpriseID
		join [ETL_Local_PROD].[dbo].[AT_PD_Dept] d on sd.DeptEnterpriseID = d.DeptEnterpriseID
		left join
		-- Correct de-dup handling MS 19.2.17
		  (select 
		  distinct 
		  Email as AssessorEmail, 
		  StaffEnterpriseId as AssessorEmailEid,
		  DeptEnterpriseId as AssessorEmailWorkbaseEid,
		-- Correct de-dup handling MS 19.2.17
		  ROW_NUMBER() over (PARTITION by StaffEnterpriseId,DeptEnterpriseId order by StaffEnterpriseId,DeptEnterpriseId, email) as rn
		-- nhs.net check no longer needed once IndOrgEmails in use MS 30.1.17
		  from #IndOrgEmails) ae on sd.StaffEnterpriseId = AssessorEmailEId
		-- Correct de-dup handling MS 19.2.17
		   and sd.DeptEnterpriseId = AssessorEmailWorkbaseEid and (ae.rn=1 or ae.rn is null)



--ALTER view [dbo].[Cache-AuditApprovals] as
-- Amended for PD Upgrade
  
 --select top 5 * from [ETL_Local_PROD].[dbo].[AT_AssessorDQInfo] 
 
 --select cast([Individual]+cast([DeptKey] as varchar(200)) as Nvarchar(400)) as DQKey,* into [ETL_Local_PROD].[dbo].[AT_AssessorDQInfo] from #AssessorDQInfo
 
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  /**


  
  ALTER TABLE [ETL_Local_PROD].[dbo].[AT_AssessorDQInfo] 
  ALTER COLUMN DQKey NVARCHAR(400) NOT NULL;



  ALTER TABLE [ETL_Local_PROD].[dbo].[AT_AssessorDQInfo] 
   ADD CONSTRAINT PK_DQKey PRIMARY KEY CLUSTERED (DQKey);

  ALTER INDEX PK_AuditTransactionOrder_ItemID ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  DISABLE
  go
  ALTER INDEX PK_AuditTransactionOrder_ItemID ON [ETL_Local_PROD].[dbo].[AuditPatient_New] REBUILD
  go

  
  CREATE INDEX KeyStaffDept
ON [ETL_Local_PROD].[dbo].[AT_AssessorDQInfo]   (DQKey,[deptpdregistryid],StaffRegistryId);


 so.StaffRegistryId	= r.From_StaffRegistryId and so.deptpdregistryid = r.From_deptpdregistryid 


  CREATE INDEX CareRecord_PatAuditIndex
ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  (CMC_ID, PatAuditID);

**/

--  	from [ETL_Local_PROD].[dbo].[AT_CarePlanVersion] a
--Left join ETL_PROD.dbo.CMC_CarePlan cp on cp.ItemId = a.LastCarePlan  
--Left join ETL_PROD.dbo.CMC_CarePlan cp1 on cp1.ItemId = a.FirstCarePlan  
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


    if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_Patient_General]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_Patient_General]

	select 
	
	po.CMC_ID,
	NHS_Number,
	'Yes' as OnNewSystem,
	a.LastestVersion as VersionNumber,
	a.GenusId,
	a.FirstVersion as FirstVersionNumber,
	ps.itemid as PatientSummary,
	p.itemid as Patient,
	cp.ItemId as Care_Plan,
	 rc.runcomplete  as DataLoadDate,

	-- Genuine Available Care Plan Check
	-- 7 = Consent Withdrawn
	case when c.Type = '7' then 'Y' else 'N' end as IsSoftDeleted,
	--* 
	--pad.CombinedAddress as #MAIN_ADDRESS,

	-- Metadata
	cp.DateLastSaved,
	cp1.EnteredBy as OriginalEnteredBy,
	-- Simplified version
	isnull(cast(demo.Add_Date as date),convert(date,aup.ActionTime)) as Add_Date,
	cp1.LastApprovedBy as OriginalApprovedBy,
	--ap.OriginalApproverWorkbaseEId ,
	null as OriginalApproverWorkbaseEId,
	'Completed' as OriginalAssessmentStatus,
	isnull(cast(demo.Date_original_Approval as date),convert(date,auz.ActionTime)) as Date_Original_Approval,
	isnull(la.AuditId,cp1.EnteredBy) as LatestEnteredBy,
	isnull(la.ActionTime,cast(demo.Date_Latest_Assessment as datetime)) as Date_Latest_Assessment,
	cp.LastApprovedBy as LatestApprovedBy,
	cp.LastApprovedTime as Date_Latest_Approval,

	
		-- Add title code MS 7.3.16
		NamePrefix as PatTitle,
		npc.Description as TITLE,
		n.GivenName as FORENAME,
		n.MiddleName,
		-- deal with duff apostrophes MS 11.2.17
		replace(n.FamilyName,'&apos;','''') as SURNAME,
		n.CCPreferredName as PreferredName,
		cast(p.DateOfBirth as date) as DoB,
		isnull(
		  DateDiff(year,p.DateOfBirth,getdate()) -
			case
			  when dbo.Date(year(getdate()),month(p.DateOfBirth),day(p.DateOfBirth)) <
				getdate() then 0
			  else 1 end,-1) as Age,
		p.Gender as GENDER,
		p.MaritalStatus as MARITALSTATUS,
		p.EthnicGroup as ETHNICITY,
		p.LivingCondDetails,
		p.Religion as RELIGION,
		p.PrimaryLanguage as PrimaryLanguage,
		p.PrimaryLangDetails,
		cast(p.DateOfDeath as date) as DoD,
		p.DeathLocation,
		p.DeathSourceInfo,
		p.DeathLocationOther,
		p.DeathVariance,
		p.DeathVarianceOther,
		case p.IsProtected when 0 then 'N' else 'Y' end as Restricted,
		p.PDSOverride,
		-- Currently no non-null values
		case
		  when pds.[fact of death] is not null and pds.[Fact of Death] = 'D' then
		 cast(pds.[date of death] as date) 
		  else null end as DoD_PDS,
		case [Fact of Death]
		  when 'D' then 'Deceased'
		  else 'Living' end as DeceasedPDS,
		pds.[Trace Result NHS Number] as PDS_NHS_Number,
		case 
		  when pds.[Record Type] is null then
			case
			  when pn.NHS_Number is null then '03.Not present, not yet traced'
			  else '02.Present, not yet traced' end 
		  when pds.[Record Type] = '30' then '01.Present and verified'
		  when pds.[Record Type] = '20' then '01.Missing but successfully identified via trace'
		  when pds.[Record Type] = '33' then '01.Present, but replacement indicated by trace'
		  when pds.[Record Type] = '40' then '01.Present, but replacement indicated by trace'
		  else
			case
			  when pn.NHS_Number is null then '04.Missing, trace attempted, no match or multiple matches found'
			  else '04.Present, trace attempted, no match or multiple matches found' end
		  end as PDS_Status,
	(select CONVERT(date,[Latest Add Date Reconciled]) from PDSStatistics) as PDS_Reconciliation_Date,

	case when pscmain.Main_LastAddressflag = 1 then pscmain.CombinedAddress end as MAIN_ADDRESS,
	case when pscmain.Main_LastAddressflag = 1 then pscmain.PostalCode end as MAIN_POSTCODE,
	case when pscmain.Main_LastAddressflag = 1 then pscmain.CombinedAddress end as PRIMARY_ADDRESS,
	case when pscmain.Main_LastAddressflag = 1 then pscmain.PostalCode end as PRIMARY_POSTCODE,
	case when pscTemp.Crr_Temp_LastAddressflag = 1 then pscTemp.CombinedAddress end as CURRENT_ADDRESS,
	case when pscTemp.Crr_Temp_LastAddressflag = 1 then pscTemp.PostalCode end as CURRENT_POSTCODE,
	case when pscSecond.Second_LastAddressflag = 1 then pscSecond.CombinedAddress end as SECONDARY_ADDRESS,
	case when pscSecond.Second_LastAddressflag = 1 then pscSecond.PostalCode end as SECONDARY_POSTCODE,
	hct.ContactValue  as Home_Phone,
	mct.contactType as Mobile_Phone,
	wct.contactType as Work_Phone,
	ect.contactType as Email,
	oct.contactType as Other_Phone,

 
		-- GP Information
		gp.Surgery as GP_Practice,
		gp.CCG,
		gp.CommissioningArea,
		gp.London_CCG_ODS as London_CCG_ODS,
		-- Add more general CCG ODS code MS 7.3.16
		gp.CCG_ODS,


			-- Consent Information
		c.Clinician as ConsentedBy,
		 cast(c.DateObtained as date)  as ConsentedDate,
		c.Type as CONSENT,
		c.Comments as MC_DET,
		p.ReqCopy as REQ_COPY,
		cp.PlannedReviewer as PlannedReviewer,
		cast(cp.PlannedReviewTime as date)  as PlannedReviewDate,

		-- Contacts
		c.POADocLocation,

		-- Medical Background
		pr.Clinician as PrognosisByRef,
		rtrim(isnull(pri.StaffTitleDescription + ' ','') + isnull(pri.StaffForename + ' ','') + isnull(pri.StaffSurname,'')) as PrognosisClinicianName,
		pri.DeptName as PrognosisClinicianWorkbase,
-- add Prognosis Clinician Workbase ODS MS 18.3.17
pri.DeptODSCode as PrognosisClinicianWorkbaseODS,
		pr.FamilyAwareProgDetails as PA_FAMPRODDETAILS,
		pr.FamilyAwarePrognosis as PA_FAMPROD,
		pr.PatientAwareProgDetails as PA_PRODDETAILS,
		pr.PatientAwarePrognosis as PA_PROD,
		pr.TimeFrame,
		pr.TimeFrameUnits as ALT_PROGNOSIS,
		pr.Surprise as Surprise,
		cast(pr.UpdatedOn as date) as DATE_PROGNOSIS,
		mb.ADRTDetails,
		mb.ADRTExists as ADRTExists,
		mb.DisabilityDetails as COMM_DIFF_DETAIL,
		mb.FamilyAwareDiagDetails as C_AWAREDETAILS,
		mb.FamilyAwareDiagnosis as C_AWARE,
		mb.HaveDisability as COMMDIFF,
		mb.LevelOfTrtmnt as CEILTREAT,
		mb.LevelOfTrtmntDetails as CT_DET,
		mb.OtherSignifHx as SIGNIFICANT_MEDICAL,
		mb.PatientAwareDiagDetails as P_AWAREDETAILS,
		mb.PatientAwareDiagnosis as P_AWARE,
		mb.WHOPerf as WHPERF,
		cast(mb.WHOPerfTime as date) as WHP_DATE,
		-- Handle primary and secondary cancers properly MS 2.5.16
		dc.DiagnosisCategory as Classified_Diagnosis,
		dc.DiagnosisCode as DiagnosisCode,

		-- CPR
		cast(cpr.CPRReviewDate as date) as REVIEW_DATE,
		cpr.ChildInvolv,
		-- Currently no non-null values
		cpr.ChildParentConsult,
		-- Currently no non-null values
		cpr.Clinician as CPRBy,
		cpr.ClinicianAware as VALIDAD,
		cpr.AdditionalDetail as POSITION,
		cast(cpr.ClinicianTime as date) as DNARDATE1,
		cpr.CourtOrder,
		-- Currently no non-null values
		cpr.DNACPRFormLocation as ORDER_YES,
		cpr.DNACPRFormUploaded,
		-- Currently no non-null values
		cpr.Decision as CARDIO_YN,
		cast(cpr.DecisionTime as date) as CPRDECDATE,
		cpr.FamilyDiscussion as RESUS_FAMILY,
		cpr.FamilyDiscussionComments as RESUS_FAMDET,
		cast(cpr.FamilyDiscussionTime as date) as FamilyDiscussionTime,
		case cpr.HasBeenAgreed when 0 then 'N' else 'Y' end as HasBeenAgreed,
		cpr.JudgeCourt,
		-- Currently no non-null values
		cpr.JudgeCourtLocation,
		-- Currently no non-null values
		cpr.JudgeCourtTime,
		-- Currently no non-null values
		cpr.OtherTeamMemb as NAMEMEM,
		cpr.PatientDiscussion as RESUS_PATIENT,
		cpr.PatientDiscussionComments as RESUS_PATIENTDET,
		cast(cpr.PatientDiscussionTime as date) as PatientDiscussionTime,
		cpr.PtAbleToDecide as HAVECAP,
		cpr.WelfareAttourney as APPOINTWA,
		cpr.WhyCPRInapp as CLINPROB,
		ap1.ApproverName as DNARNAME1,
		ap2.ApproverName as DNARNAME2,
		ap3.ApproverName as DNARNAME3,
		cast(ap1.ApprovalTime as date)as DNARDATE2,
		cast(ap2.ApprovalTime as date) as DNARDATE3,
		cast(ap3.ApprovalTime as date) as DNARDATE4,

	 -- Social Background
	pkg.DS1500 as DS1500,
	pkg.Equipment as EQUIP,
	pkg.EquipmentNotes as EQUIP_DETAIL,
	pkg.FamilySupport as FAM_SUPPORT,
	pkg.FamilySupportNotes as FAM_SUPPORT_Y,
	pkg.HomecareHelp as HOMECARE,
	pkg.HomecareHelpNotes as HOMECARE_DET,
	pkg.PatientReceipt as CAREPLAN,
	pkg.PatientReceiptNotes as CAREPLAN_DETAIL,

	 -- Medications
	ms.Anticoags as Anticoags,
	ms.Insulin as Insulin,
	ms.MedListLocation,
	ms.Opioids as OPIOID,
	ms.OtherInfo as MED_OTH,
	ms.Steroids as Steroids,


	 -- Preferences
	pf.CulturalRelNeeds as CULTURAL,
	pf.FamilyAwarePref as FAMILY_AWAR,
	pf.OrganDonat as WISHES,
	pf.OrganDonatDet as WISHES_YES,
	pf.PatientWishes as PERCARE_PLAN,
	pf.PlaceCare as PPC,
	pf.PlaceCareDet as PPDDiscuss,
	pf.PlaceCareDet,
	-- unconfuse things
	pf.PlaceDeath1 as PPD1,
	pf.PlaceDeath1Det as PPCDiscuss,
	-- unconfuse things
	pf.PlaceDeath1Det,
	pf.PlaceDeath2 as PPD2,
	pf.PlaceDeath2Det

 into [ETL_Local_PROD].[dbo].[AT_Patient_General]

	from [ETL_Local_PROD].[dbo].[AT_CarePlanVersion] a
Left join ETL_PROD.dbo.CMC_CarePlan cp on cp.ItemId = a.LastCarePlan  
Left join ETL_PROD.dbo.CMC_CarePlan cp1 on cp1.ItemId = a.FirstCarePlan  
Left join ETL_PROD.dbo.CMC_PatientSummary ps on ps.ItemID =  a.LastPatientSummary  
Left join ETL_PROD.dbo.CMC_Patient p on p.ItemId = ps.Patient  
Left join ETL_PROD.dbo.CMC_Name n on n.itemid = p.Name  
Left join AT_CMCID po on po.Patient = ps.ItemID
Left join AT_NHSNumbers pn on pn.Patient = ps.ItemID
--left join [ETL_Local_PROD].[dbo].[AT_AuditApprovals] ap on ap.cmc_id = a.FirstCarePlan  

--select top 40 * from #AuditApprovals

Left join [ETL_Local_PROD].[dbo].[AT_PatientRegistered_GP] gp on gp.patientsummary = ps.ItemId
left join ETL_PROD.dbo.CMC_CarePackage pkg on pkg.ItemId = ps.CarePackage
left join ETL_PROD.dbo.CMC_Consent c on c.ItemId = p.Consent
left join ETL_PROD.dbo.CMC_CPR cpr on ps.CPR = cpr.ItemId
left join ETL_PROD.dbo.CMC_MedicalBackground mb on ps.MedicalBackground = mb.ItemId
left join ETL_PROD.dbo.CMC_MedicationSummary ms on ms.ItemId = ps.MedicationSummary
left join ETL_PROD.dbo.CMC_PatientSummary_PatientPreferences psf on ps.ItemId = psf.PatientSummary
left join ETL_PROD.dbo.CMC_PatientPreference pf on psf.PatientPreference = pf.ItemId
left join ETL_PROD.dbo.CMC_Prognosis pr on ps.Prognosis = pr.ItemId
left join [ETL_Local_PROD].[dbo].[AT_StaffDeptContext] pri on pri.ProviderOrgContext = pr.Clinician

left join (select * from #PatientCPRApprovers where rn=1) ap1 on cpr.ItemId = ap1.CPR
left join (select * from #PatientCPRApprovers where rn=2) ap2 on cpr.ItemId = ap2.CPR
left join (select * from #PatientCPRApprovers where rn=3) ap3 on cpr.ItemId = ap3.CPR

--select * from AT_PatAddress where CMC_ID = 100007994
left join AT_PatAddress pscmain on pscmain.CMC_ID = a.GenusID and pscmain.Main_LastAddressFlag = 1
left join AT_PatAddress pscSecond on pscSecond.CMC_ID = a.GenusID and pscSecond.Second_LastAddressFlag = 1
left join AT_PatAddress pscTemp on pscTemp.CMC_ID = a.GenusID and pscTemp.Crr_Temp_LastAddressflag = 1
 --select * from [AT_CarePlanVersion] 

--select * from #PatTele where CMC_ID =  100013863
--left join #PatTele hct on hct.Patient = ps.Patient 
left join #PatTele hct on hct.Patient = ps.Patient and hct.contactType = 'HOME' 
left join #PatTele mct on mct.Patient = ps.Patient and mct.contactType = 'MOBILE' 
left join #PatTele wct on wct.Patient = ps.Patient and wct.contactType = 'WORK' 
left join #PatTele ect on ect.Patient = ps.Patient and ect.contactType = 'EMAIL'
left join #PatTele oct on oct.Patient = ps.Patient and oct.contactType = 'OTHER'

left join ETL_PROD.dbo.Coded_NamePrefix npc on npc.code = n.NamePrefix
--select * from #PatDiagnosis
left join AT_PatDiagnosis dc on dc.Patient = po.Patient  and dc.PatDiagRN = 1
 
left join (select top 1 * from ETL_PROD.dbo.CMC_RUN_COMPLETE where ItemId=1) rc on 1=1

--select * from #AuditPatient
 
left join [ETL_Local_PROD].[dbo].[AuditPatient_New]aup on aup.CMC_ID = po.CMC_ID and aup.ActionType = 'Create' and aup.ActionTypeOrder = 1
left join [ETL_Local_PROD].[dbo].[AuditPatient_New]auz on auz.CMC_ID = po.CMC_ID and auz.ActionType = 'Publish' and auz.ActionTypeOrder = 1
left join (
select cmc_id,add_date,Date_Original_Assessment,Date_Latest_Assessment,original_approval_date as date_original_approval from Protocol.OldSystemCarePlans
union all
select cmc_id,add_date,Date_Original_Assessment,Date_Latest_Assessment,original_approval_date as date_original_approval from Protocol.OldSystemCarePlansMigratedDuplicates
) demo on demo.CMC_ID = po.CMC_ID

left join [ETL_Local_PROD].[dbo].[AT_Accurate_Entered_by] la on la.patientsummary = ps.ItemId
left join Load.PDS on pds.[Local PID] = po.CMC_ID




--select * from [ETL_Local_PROD].[dbo].[AT_AuditApprovals] where to_StaffRegistryId is null
  /**


  
  ALTER TABLE  [ETL_Local_PROD].[dbo].[AT_AuditApprovals]
  ALTER COLUMN cmc_id bigint NOT NULL;
 


  ALTER TABLE [ETL_Local_PROD].[dbo].[AT_AuditApprovals]
   ADD CONSTRAINT PK_CMCID_Key PRIMARY KEY CLUSTERED (cmc_id);

  ALTER INDEX PK_AuditTransactionOrder_ItemID ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  DISABLE
  go
  ALTER INDEX PK_AuditTransactionOrder_ItemID ON [ETL_Local_PROD].[dbo].[AuditPatient_New] REBUILD
  go

  
  CREATE INDEX fromStaffDept
ON [ETL_Local_PROD].[dbo].[AT_AuditApprovals]   (CMC_ID,[From_StaffRegistryId],From_deptpdregistryid);

  CREATE INDEX ToStaffDept
ON [ETL_Local_PROD].[dbo].[AT_AuditApprovals]   (CMC_ID,[To_StaffRegistryId],To_deptpdregistryid);



 


  CREATE INDEX CareRecord_PatAuditIndex
ON [ETL_Local_PROD].[dbo].[AuditPatient_New]  (CMC_ID, PatAuditID);

**/


  --if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_AuditApprovals]') is not null
	truncate table [ETL_Local_PROD].[dbo].[AT_AuditApprovals]
	insert into  [ETL_Local_PROD].[dbo].[AT_AuditApprovals]
	select  
	p.cmc_id,
	OriginalApprovedBy,
	LatestApprovedBy,
	cast(ao.StaffRegistryId as nvarchar(255)) as From_StaffRegistryId,
	cast(ao.deptpdregistryid as nvarchar(255))  as From_deptpdregistryid,
	cast(al.StaffRegistryId as nvarchar(255))  as To_StaffRegistryId,
	cast(al.deptpdregistryid as nvarchar(255))  as To_deptpdregistryid,
	--so.StaffEnterpriseID as OriginalApproverEID,
	null as OriginalApproverEID,
	--sl.StaffEnterpriseID as LatestApproverEID,
	null as LatestApproverEID,
	--ISNULL(so.StaffTitleDescription+' ','') + ISNULL(so.StaffForename+' ','') + so.StaffSurname as OriginalApprover,
	--ISNULL(sl.StaffTitleDescription+' ','') + ISNULL(sl.StaffForename+' ','') + sl.StaffSurname as LatestApprover,
	cast(null as varchar(max)) as OriginalApprover,
	cast(null as varchar(max)) as LatestApprover, 
	cast(null as varchar(max)) as OriginalApproverEmail,
	cast(null as varchar(max)) as LatestApproverEmail,
	cast(null as varchar(max)) as OriginalApproverJobTitle,
	cast(null as varchar(max)) as LatestApproverJobTitle,
	
	
	null as OriginalApproverWorkbaseEid,
	null as LatestApproverWorkbaseEid,
	cast(null as varchar(max)) as OriginalApproverWorkbase,
	cast(null as varchar(max)) as LatestApproverWorkbase,
	cast(null as varchar(max)) as OriginalApproverProfGroup,
	cast(null as varchar(max)) as LatestApproverProfGroup,
	cast(null as varchar(255)) as OriginalApproverWorkbaseODS,
	cast(null as varchar(255)) as LatestApproverWorkbaseODS,
	cast(null as varchar(max)) as OriginalApproverWorkbaseEmail,
	cast(null as varchar(max)) as LatestApproverWorkbaseEmail,
	cast(null as varchar(max)) as OriginalApproverODS,
	cast(null as varchar(max)) as LatestApproverODS

 

	from [ETL_Local_PROD].[dbo].[AT_Patient_General] p
	left join [AuditPatient-CarePlan] ao on ao.FromCarePlan = p.OriginalApprovedBy  
										and ao.ActionType = 'publish'
	left join [AuditPatient-CarePlan] al on al.FromCarePlan = p.LatestApprovedBy 
										and al.ActionType = 'publish'



	update r

			set r.OriginalApproverEID = so.StaffEnterpriseID,
				--r.LatestApproverEID = sl.StaffEnterpriseID,
				r.OriginalApprover = ISNULL(so.StaffTitleDescription+' ','') + ISNULL(so.StaffForename+' ','') + so.StaffSurname,
				--r.LatestApprover = ISNULL(sl.StaffTitleDescription+' ','') + ISNULL(sl.StaffForename+' ','') + sl.StaffSurname,
				r.OriginalApproverEmail = so.AssessorEmail,
				--r.LatestApproverEmail = sl.AssessorEmail,
				r.OriginalApproverJobTitle = so.StaffProviderTypeDescription ,
				--r.LatestApproverJobTitle = sl.StaffProviderTypeDescription,
				---- Add Approver ODS codes MS 11.3.17
				r.OriginalApproverODS = so.StaffODSCode
				--,
				--r.LatestApproverODS = sl.StaffODSCode


			from [ETL_Local_PROD].[dbo].[AT_AuditApprovals]r
 			left join [ETL_Local_PROD].[dbo].[AT_AssessorDQInfo]  so on so.StaffRegistryId	= r.From_StaffRegistryId 
																	and so.deptpdregistryid = r.From_deptpdregistryid 
		   --left join [ETL_Local_PROD].[dbo].[AT_AssessorDQInfo]  sl on sl.StaffRegistryId	= r.To_StaffRegistryId 
					--												and sl.deptpdregistryid = r.To_deptpdregistryid 
			where From_StaffRegistryId is not null
			
	


	

	update t

			set  t.LatestApproverEID = sl.StaffEnterpriseID,
				t.LatestApprover = ISNULL(sl.StaffTitleDescription+' ','') + ISNULL(sl.StaffForename+' ','') + sl.StaffSurname,
				t.LatestApproverEmail = sl.AssessorEmail,
				t.LatestApproverJobTitle = sl.StaffProviderTypeDescription,
				-- Add Approver ODS codes MS 11.3.17
				t.LatestApproverODS = sl.StaffODSCode


			from [ETL_Local_PROD].[dbo].[AT_AuditApprovals]t
  
		   left join [ETL_Local_PROD].[dbo].[AT_AssessorDQInfo]  sl on sl.StaffRegistryId	= t.To_StaffRegistryId 
																	and sl.deptpdregistryid = t.To_deptpdregistryid 
			where To_StaffRegistryId is not null
			







	--select count(*) from #AssessorDQInfo


	update s

			set s.OriginalApproverWorkbaseEid = do.DeptEnterpriseID,
				s.LatestApproverWorkbaseEid = dl.DeptEnterpriseID,
				s.OriginalApproverWorkbase = do.DeptName,
				s.LatestApproverWorkbase = dl.DeptName,
				s.OriginalApproverProfGroup = do.LocalCMCOrgTypeDescription,
				s.LatestApproverProfGroup = dl.LocalCMCOrgTypeDescription,
				s.OriginalApproverWorkbaseODS = do.DeptODSCode,
				s.LatestApproverWorkbaseODS = dl.DeptODSCode,
				s.OriginalApproverWorkbaseEmail = do.WorkbaseEmail,
				s.LatestApproverWorkbaseEmail = dl.WorkbaseEmail
	 
			from [ETL_Local_PROD].[dbo].[AT_AuditApprovals]s
			left join #WorkbaseDQInfo do on do.DeptPDRegistryID = s.From_deptpdregistryid 
			left join #WorkbaseDQInfo dl on dl.DeptPDRegistryID = s.To_deptpdregistryid 



			update x 
			
					set x.OriginalApproverWorkbaseEId = y.OriginalApproverWorkbaseEid				
			
			from [ETL_Local_PROD].[dbo].[AT_Patient_General]x
			left join [ETL_Local_PROD].[dbo].[AT_AuditApprovals]y on y.cmc_id = x.cmc_id  






		
  
	