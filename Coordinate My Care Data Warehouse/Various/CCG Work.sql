USE [ETL_Local_PROD]
GO
------------------------------------------------------START OF DEPARTMENT-------------------------------------------------------------------------------------------------------------------------

if OBJECT_ID ('Tempdb..#CacheOrg') is not null
	drop table #CacheOrg
select a.ItemId as Organization, 
cast(  a.Source as varchar(25)) as DeptSource,
EnterpriseID as DeptEnterpriseId,
PDRegistryID as DeptPDRegistryId,
c.Name as DeptName ,
c.ItemId as OrgItem,
b.OrganizationType,
cc.[Truncated Name] as CCGName, 
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
left join [ETL_Local_PROD].[dbo].[CCG] cc on cc.[Organisation Code] = b.OrganizationType
where a.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
  and a.ItemId not like 'PS|%|%|%|%|%|LCA'
  and a.Source  = 'PD'




  --select top 100 * from #Department where organization = '100000006O'





--select * from #CacheOrg where organization = '100000006O'

--select * from  [ETL_Local_PROD].[dbo].[CCG]





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
  where TypeCodedValue is null
or TypeCodedValue = 'Member'

--select * from #PDOrgToOrg

  if OBJECT_ID ('Tempdb..#ArwanPDOrgToOrg') is not null
	drop table #ArwanPDOrgToOrg
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

into #ArwanPDOrgToOrg

from ETL_PROD.dbo.CMC_OrgToOrg o
join PDDepartment dc on dc.DeptEnterpriseID = o.ChildOrganizationEID
join PDDepartment dp on dp.DeptEnterpriseID = o.ParentOrganizationEID
left join ETL_PROD.dbo.Coded_OrgOrgRelationshipType t on TypeCodedValue = t.code
left join
  (select orgtoorg,StartDate,EndDate,
          ROW_NUMBER() over (partition by OrgToOrg order by startdate desc) as rn
  from ETL_PROD.dbo.CMC_OrgToOrg_DateSpan od
  left join ETL_PROD.dbo.CMC_DateSpan ds on od.DateSpan = ds.ItemId) d 
  on o.ItemId = d.OrgToOrg and d.rn=1



  select 
  a.* ,
  '¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬' as Gap,
  b.*
  
  from #ArwanPDOrgToOrg a
  left join #PDOrgToOrg b on b.ChildOrganizationEID = a.ChildOrganizationEID
  and b.ParentOrganizationEID = a.ParentOrganizationEID
  where b.ChildOrganizationEID is null















   if OBJECT_ID ('Tempdb..#Department') is not null ---Query Logic here
	drop table #Department
 
SELECT 
d1.CCGName,
d1.Organization, 
d1.DeptPDRegistryId,
d1.DeptLocalCMCID,  
d1.LocalCMCOrgType,
d1.LocalCMCOrgTypeDescription,
d1.DeptODSCode,
d1.deptsource, 
d1.DeptOpenDate,
d1.DeptCloseDate,
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

FROM         #CacheOrg AS d1 WITH (nolock) 
LEFT OUTER JOIN #PDOrgToOrg AS o1 WITH (nolock) ON d1.DeptEnterpriseID = o1.ChildOrganizationEID AND (o1.EndDate IS NULL OR
                      CAST(o1.EndDate AS date) > '2015-05-01') AND (o1.StartDate IS NULL OR
                      CAST(o1.StartDate AS date) <= CAST(GETDATE() AS DATE)) 
LEFT OUTER JOIN #CacheOrg AS d2 WITH (nolock) ON d2.DeptEnterpriseID = o1.ParentOrganizationEID 
LEFT OUTER JOIN  #PDOrgToOrg AS o2 WITH (nolock) ON o1.ParentOrganizationEID = o2.ChildOrganizationEID AND (o2.EndDate IS NULL OR
                      CAST(o2.EndDate AS date) > '2015-05-01') AND (o2.StartDate IS NULL OR
                      CAST(o2.StartDate AS date) <= CAST(GETDATE() AS DATE)) 
LEFT OUTER JOIN  #CacheOrg AS d3 WITH (nolock) ON d3.DeptEnterpriseID = o2.ParentOrganizationEID 
LEFT OUTER JOIN #PDOrgToOrg AS o3 WITH (nolock) ON o2.ParentOrganizationEID = o3.ChildOrganizationEID AND (o3.EndDate IS NULL OR
                      CAST(o3.EndDate AS date) > '2015-05-01') AND (o3.StartDate IS NULL OR
                      CAST(o3.StartDate AS date) <= CAST(GETDATE() AS DATE)) 
LEFT OUTER JOIN #CacheOrg AS d4 WITH (nolock) ON d4.DeptEnterpriseID = o3.ParentOrganizationEID 
LEFT OUTER JOIN #PDOrgToOrg AS o4 WITH (nolock) ON o3.ParentOrganizationEID = o4.ChildOrganizationEID AND (o4.EndDate IS NULL OR
                      CAST(o4.EndDate AS date) > '2015-05-01') AND (o4.StartDate IS NULL OR
                      CAST(o4.StartDate AS date) <= CAST(GETDATE() AS DATE)) 
LEFT OUTER JOIN #CacheOrg AS d5 WITH (nolock) ON d5.DeptEnterpriseID = o4.ParentOrganizationEID 
LEFT OUTER JOIN #PDOrgToOrg AS o5 WITH (nolock) ON o4.ParentOrganizationEID = o5.ChildOrganizationEID AND (o5.EndDate IS NULL OR
                      CAST(o5.EndDate AS date) > '2015-05-01') AND (o5.StartDate IS NULL OR
                      CAST(o5.StartDate AS date) <= CAST(GETDATE() AS DATE)) 
 LEFT OUTER JOIN #CacheOrg AS d6 WITH (nolock) ON d6.DeptEnterpriseID = o5.ParentOrganizationEID 
 LEFT OUTER JOIN #PDOrgToOrg AS o6 WITH (nolock) ON o5.ParentOrganizationEID = o6.ChildOrganizationEID AND (o6.EndDate IS NULL OR
                      CAST(o6.EndDate AS date) > '2015-05-01') AND (o6.StartDate IS NULL OR
                      CAST(o6.StartDate AS date) <= CAST(GETDATE() AS DATE)) 
LEFT OUTER JOIN #CacheOrg AS d7 WITH (nolock) ON d7.DeptEnterpriseID = o6.ParentOrganizationEID

--select * from #Department where organization = '100087983O' order by name1
--select * from #Department where eid7 = 100031712
--select count(*) from #Department 
--select  * from #CacheOrg where organization = '100087983O'
--select  * from #CacheOrg where DeptEnterpriseID = 100087983
--select * from #Department where organization = '100087983O' order by name6
--select top 5 *	from #CacheOrg 

--select top 5 *	from #PDOrgToOrg where childOrganizationEID  = 100087983 
--select  * from #CacheOrg where DeptEnterpriseID = 100087983
select  * from #CacheOrg  order by CCGName
------------------------------------------------------END OF DEPARTMENT-------------------------------------------------------------------------------------------------------------------------

   if OBJECT_ID ('Tempdb..#Staff') is not null
	drop table #Staff
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

into #Staff

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


   if OBJECT_ID ('Tempdb..#StaffDeptContext') is not null
	drop table #StaffDeptContext

select 
poc.ItemId as ProviderOrgContext, 
s.*, 
d.*

 into #StaffDeptContext

from ETL_PROD.dbo.CMC_ProviderOrgContext poc
left join ETL_PROD.dbo.CMC_IndividualProvider ip on poc.Provider = ip.ItemId
left join ETL_PROD.dbo.CMC_Individual i on i.PDRegistryID = ip.RegistryID
left join #Staff s on s.Individual = i.ItemID
left join #CacheOrg  d on d.Organization = poc.Organization
-- Exclude LastClinicalApprover rows introduced in 15.1 release 
where ip.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
AND ip.ItemId not like 'PS|%|%|%|%|%|LCA'
AND poc.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
AND poc.ItemId not like 'PS|%|%|%|%|%|LCA'


--select * from #Staff
--select * from #StaffDeptContext




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
(SELECT
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
left join #StaffDeptContext a on pa.Provider = a.ProviderOrgContext
left join ETL_PROD.dbo.CMC_IndividualProvider ip on ip.ItemId = b.Provider) sel1
-- Add title and role descriptions MS 19.2.16
left join ETL_PROD.dbo.Coded_NamePrefix np1 on StaffTitle = np1.code
left join ETL_PROD.dbo.Coded_NamePrefix np2 on NamePrefix = np2.code
left join ETL_PROD.dbo.Coded_DocumentProviderRole dpr on Role = dpr.code
left join ETL_PROD.dbo.Coded_DocumentOrganizationRole dor on OrgRole = dor.code








select CMC_ID, 
PatientSummary, 
RegisteredGP, 
CCG,
sel1.CCGName,
-- Add new CCG ODS field for all, not just London MS 7.3.16
case when CCG = 'NHS SURREY DOWNS CCG' then NULL else DeptODSCode end as London_CCG_ODS,
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
			  CCGName,
			ROW_NUMBER() over (partition by c.patientsummary
			-- MS 3.11.16 proper sequencing
			order by
			case when h.typedesc6='CCG' then 0 when h.typedesc5='CCG' then 0 else 1 end,
			h.name6,h.name5) as prn,
			DeptEnterpriseId as SurgeryEId,
			StaffEnterpriseId as GPEId,
			DeptName as Surgery
			from #PatientHSCContacts c
			left join #Department h on c.DeptEnterpriseID = h.eid7
			left join Protocol.ODSReconciliationCandidates m on c.DeptLocalCMCId = m.dept_id
			where role = 'REG'
			-- take end date into account MS 21.3.16
			and (c.ToTime is null or cast(c.ToTime as date) > CAST(getdate() as DATE))
			and (c.FromTime is null or cast(c.FromTime as date) <= CAST(getdate() as DATE))


			) sel1
-- Remove any possible duplicates MS 21.3.16
left join (select 
			*,ROW_NUMBER() over (PARTITION by deptname 
			-- MS 3.11.16 proper sequencing 
			order by deptenterpriseid) as drn 
			from #CacheOrg
			) d
  on sel1.CCG = d.DeptName
where (drn=1 or drn is null) and prn=1


--select * from Protocol.ODSReconciliationCandidates

--GO

--	SELECT NAME AS ObjectName
--	,schema_name(o.schema_id) AS SchemaName
--	,type
--	,o.type_desc
--FROM sys.objects o
--WHERE o.is_ms_shipped = 0
--	AND o.NAME LIKE '%ODSReconciliationCandidates%'
--ORDER BY o.NAME

   if OBJECT_ID ('Tempdb..#ArwanDept') is not null
	drop table #ArwanDept
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

into #ArwanDept

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

and DeptSource = 'PD'




--select count(*) from #Department

--select count(*) from #ArwanDept
--select count(*) from #CacheOrg


--select top 5* from #ArwanDept
--select top 5* from #CacheOrg


--select 
--a.*,
--'¦¦¦¦¦¦¦¦¦¦¦¦¦¦¦¦¦¦¦¦¦¦¦¦¦¦' as GAP,
--b.*
--from #CacheOrg a
--left join #ArwanDept b on b.Organization = a.Organization
--						and b.DeptPDRegistryId = a.DeptPDRegistryId
--Where b.Organization is null



