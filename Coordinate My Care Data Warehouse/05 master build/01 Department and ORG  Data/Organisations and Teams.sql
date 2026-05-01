	
	USE [ETL_Local_PROD]
GO
/****** Object:  StoredProcedure [dbo].[AT__REDWOOD_CLI_Extract_SP]    Script Date: 25/03/2021 13:09:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




alter PROCEDURE [dbo].[AT__Provider_directory_Organsations_Departments] 
-- Amended for PD Upgrade
AS
BEGIN
	
	
	
	IF OBJECT_ID('tempdb..#Orgs') IS NOT NULL 
		dROP TABLE #Orgs
  
		select
		distinct
		null as id,
		null as ProRN,
		z.DeptName as [Team]
		,z.DeptODSCode
		,z.deptEnterpriseID as EnterpriseID
		,z.DeptPDRegistryId
		,cast(null as date) as ORgOpenDate
		,cast(null as date) as ORgCloseDate
		,null as ParentOrganizationEID
		,cast(null as varchar(max)) as Parent
		,cast(null as varchar(255)) as TypeCodedValue
		,cast(null as varchar(200))as OrgStatus
		,cast(null as varchar(max))as OrgType
		,null as IsParentFlag
		,null as IgnoreFlag
		,cast(null as varchar(255)) as PostCode
		,Cast(null as varchar(255)) as CCG
		,Cast(null as Varchar(255)) as [Local Authority]
				
		into #Orgs
		
		FROM [ETL_Local_PROD].[dbo].[AuditPatient_New]a with (nolock)
	 
		left join(  
				--pd_DEPT
				-----------------------------------------------------------------------------------------------------------------------------------
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

 

				from 
				(
		
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
	 

				from 
				(
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


				from ETL_PROD.dbo.cmc_organization with (nolock)
				where ETL_PROD.dbo.cmc_organization.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
				and ETL_PROD.dbo.cmc_organization.ItemId not like 'PS|%|%|%|%|%|LCA'
				)raw
				left join 
				(
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

				from ETL_PROD.dbo.cmc_organization with (nolock)
				where ETL_PROD.dbo.cmc_organization.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
				and ETL_PROD.dbo.cmc_organization.ItemId not like 'PS|%|%|%|%|%|LCA'
				)
				pd on raw.registryid = pd.pdregistryid and raw.Source is null
										and raw.RegistryID is not null and pd.PDRegistryID is not null
				) o 
				left join 
				(
				select 
				Organization,
				MIN(organizationtype) as OrganizationType 
				from ETL_PROD.dbo.CMC_Organization_OrganizationTypeCodes  with (nolock) 
				group by Organization
				)t  on o.PDItemId = t.Organization
				left join ETL_PROD.dbo.Coded_OrgType tl  with (nolock) on tl.Code = t.OrganizationType
				left join ETL_PROD.dbo.Coded_LocalCMCOrgType ol  with (nolock) on ol.code = o.LocalCMCOrgType
				left join ETL_PROD.dbo.CMC_OrganizationName n  with (nolock) on n.ItemId = o.Name
				-- Exclude Last Clinical Approver rows for 15.1 release
				where n.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
				AND n.ItemId not like 'PS|%|%|%|%|%|LCA'
				and o.DeptSource  = 'PD'
------------------------------------------------------------------------------------------------- 
			)z on z.DeptPDRegistryID = a.DeptPDRegistryID
			

			Where z.deptEnterpriseID is not null
			--and z.deptEnterpriseID = 100144440


			--drop table #OrgBackUp 
			--select * into #OrgBackUp from #Orgs
			--drop table #Orgs
			--select * into #Orgs  from #OrgBackUp
			--select * from #Orgs where EnterpriseID = 100126848
			
			update aa

			set aa.Parent = e.Name,
			aa.ParentOrganizationEID = c.ParentOrganizationEID,
			aa.ORgOpenDate = a.OpenDate,
			aa.ORgCloseDate = a.CloseDate,
			aa.TypeCodedValue = h.Description,
			aa.OrgType = f.Description,
			aa.OrgStatus = x.Description


			from #Orgs aa
			inner join [ETL_PROD].[dbo].[CMC_Organization] a  with (nolock)  on a.EnterpriseID = aa.[EnterpriseID] and a.Source = 'pd'
			left join [ETL_PROD].[dbo].[CMC_OrganizationName]b  with (nolock)  on b.[ItemId] = a.ItemId
			inner join [ETL_PROD].[dbo].[CMC_OrgToOrg]c  with (nolock)  on c.ChildOrganizationEID = aa.EnterpriseID and c.TypeCodedValue in ('MEMBER','09') and (deleted <> 1 or deleted is null)--not in ( '05','01') 
			left join [ETL_PROD].[dbo].[Coded_OrgStatus]x on x.Code = a.StatusCodedValue
			inner join [ETL_PROD].[dbo].[CMC_Organization]d  with (nolock)  on d.EnterpriseID = c.ParentOrganizationEID and d.CloseDate is null and d.LocalCMCOrgType not in ( 106,11)
			left join [ETL_PROD].[dbo].[CMC_OrganizationName]e  with (nolock)  on e.[ItemId] = d.ItemId  and e.Source = 'pd'
			left join [ETL_PROD].[dbo].[Coded_OrgOrgRelationshipType]h  with (nolock)  on h.Code = c.TypeCodedValue
			left join [ETL_PROD].[dbo].[Coded_LocalCMCOrgType]f  with (nolock)  on f.Code = a.LocalCMCOrgType


			delete from #Orgs where team = 'CMC TEST DOCTORS'--where OrgStatus = 'closed' or team = 'CMC TEST DOCTORS'
				--select * from #Orgs where EnterpriseID =100126869 

					--select * from #Orgs order by parent

			--select 
			--c.* ,
			--d.*,
			--h.Description,
			--e.name,
			--f.Description
			--from [ETL_PROD].[dbo].[CMC_OrgToOrg] c
			--left join [ETL_PROD].[dbo].[Coded_OrgOrgRelationshipType]h on h.Code = c.TypeCodedValue
			--inner join [ETL_PROD].[dbo].[CMC_Organization]d on d.EnterpriseID = c.ParentOrganizationEID and d.CloseDate is null and d.LocalCMCOrgType <> 106 and d.Source = 'pd'
			--left join [ETL_PROD].[dbo].[CMC_OrganizationName]e on e.[ItemId] = d.ItemId
			--left join [ETL_PROD].[dbo].[Coded_LocalCMCOrgType]f on f.Code = d.LocalCMCOrgType
			--where ChildOrganizationEID = 100126848 
			--and c.TypeCodedValue in ('MEMBER','09') 









			update r

			set r.Parent = s.[Team or GP],
			r.OrgType = 'General Practice'

			from #Orgs r 
			inner join [ETL_Local_PROD].[ODSData].[searchods]s on s.ODS = r.DeptODSCode and s.Type = 'General Medical Practice'
			
			where r.OrgType = 'General Practice' or R.OrgType is null
			--select * from #Orgs order by Team
			
			--where OrgType = 'General Practice'
			--select * from #Orgs where charindex('OOH',team)>0
  

			--			select * from #Orgs where DeptODSCode is null
			--			select * from #Orgs order by Parent,  Team
			--			select * from #Orgs where OrgStatus = 'closed'
			--			select * from #Orgs where Team in ('ELFT CH DEMENTIA SERVICE TEAM')
			--select * from [ETL_Local_PROD].[ODSData].[searchods] where [Team or GP] = 'THE NELSON MEDICAL PRACTICE'
			--select * from AT_PD_Dept where DeptName =  'THE NELSON MEDICAL PRACTICE'
		--select * from [ETL_Local_PROD].[ODSData].[searchods] where ods in ('E87024',	'E87742')
			--select * from [ETL_Local_PROD].[ODSData].[searchods] where charindex('CASTLEVIEW',[team or gp])>0 order by [team or gp]
			--select * from #Orgs where charindex('jefferies',[team])>0

			update r

			set r.Parent = r.team

			from #Orgs r 

			where OrgType = 'General Practice'
			and DeptODSCode is null
		



			update f

				set f.Parent = e.Name

			from #Orgs f
			left join [ETL_PROD].[dbo].[CMC_Organization]d on d.EnterpriseID = f.ParentOrganizationEID --and d.CloseDate is null and d.LocalCMCOrgType <> 106
			left join [ETL_PROD].[dbo].[CMC_OrganizationName]e on e.[ItemId] = d.ItemId  and e.Source = 'pd'
			where f.Parent is null and f.ParentOrganizationEID is not null



			Update x

				set x.ParentOrganizationEID = x.EnterpriseID,
					x.Parent = x.Team 
			from #Orgs x
			where x.ParentOrganizationEID is null or ParentOrganizationEID = 100099077

			update z
				set z.IsParentFlag = 1
			from #Orgs z
			where (EnterpriseID = ParentOrganizationEID) OR (parent = Team)



						update f

				set f.ParentOrganizationEID = EnterpriseID,
					f.Parent = f.Team

			from #Orgs f
			where OrgType =  'General Practice'


			update r

			set r.OrgType = t.Description
			--select r.*,t.DEscription

			from #Orgs r
			inner join [ETL_PROD].[dbo].[CMC_Organization] s on s.EnterpriseID = r.ParentOrganizationEID
			inner join [ETL_PROD].[dbo].[Coded_LocalCMCOrgType]t on t.Code = s.LocalCMCOrgType
			where r.OrgType is null
					 


						--			select * from #Orgs where DeptODSCode is null
			--			select * from #Orgs order by Parent,  Team
			--select * from [ETL_Local_PROD].[ODSData].[searchods]

			update f
					set f.PostCode = g.Postcode
			From #Orgs f
			left join [ETL_Local_PROD].[ODSData].[searchods]g on g.ODS = f.DeptODSCode

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

		if object_id('tempdb..#Temp2') is not null
	 drop table #Temp2

		select 
		distinct  
		ltrim(rtrim(REPLACE(CCG, ' CCG', ''))) as CCG, 
		STP,
		[NHS England REgion],
		[PCDS],
		REPLACE([PCDS] , ' ', '') as [PCDS_NoGaps] ,
		left(PCDS,7) as [PCDS_7],
		left(PCDS,6) as [PCDS_6],
		left(PCDS,5) as [PCDS_5],
		left(PCDS,4) as [PCDS_4]
		into #Temp2
		from [ETL_Local_PROD].[dbo].[AT_ENGLISH_POSTCODE_DATA]


			if object_id('tempdb..#Temp3') is not null
	 drop table #Temp3

		select 
		distinct  
		ltrim(rtrim(REPLACE(CCG, ' CCG', ''))) as CCG, 
		STP,
		[NHS England REgion],
		[Local Authority],
		[PCDS],
		REPLACE([PCDS] , ' ', '') as [PCDS_NoGaps] ,
		left(PCDS,7) as [PCDS_7],
		left(PCDS,6) as [PCDS_6],
		left(PCDS,5) as [PCDS_5],
		left(PCDS,4) as [PCDS_4]
		into #Temp3
		from [ETL_Local_PROD].[dbo].[AT_ENGLISH_POSTCODE_DATA_NEW]

--------------------------------------------------------------------------------------------------------------------------------------------------------
	update s

	set s.CCG = coalesce(pc.ccg,pc2.ccg )

	from  [#Orgs]s

left join  [#Temp2]pc on pc.PCDS = s.POSTCODE
left join  [#Temp2]pc2 on pc2.[PCDS_NoGaps] = REPLACE(s.POSTCODE , ' ', '')





	update s

	set s.CCG = coalesce(ss.ccg,t.ccg)

	from  [#Orgs]s
	left join  [#Temp2]ss on ss.PCDS_7 = left(s.POSTCODE,7)
	left join  [#Temp2]t on t.PCDS_6 = left(s.POSTCODE,6)

	where s.CCG is null 


	update s

	set s.CCG = coalesce(u.ccg,v.ccg)

	from  [#Orgs]s
	left join  [#Temp2]u on u.PCDS_5 = left(s.POSTCODE,5)
	left join  [#Temp2]v on v.PCDS_4 = left(s.POSTCODE,4)

	where s.CCG is null 



	---LOCAL AUTHORITIES


	update s
			set s.[Local Authority] = coalesce(pc.[Local Authority],pc2.[Local Authority])
	from [#Orgs]s 
left join  [#Temp3]pc on pc.PCDS = s.POSTCODE
left join  [#Temp3]pc2 on pc2.[PCDS_NoGaps] = REPLACE(s.POSTCODE , ' ', '')




	update s

	set s.[Local Authority] = coalesce(ss.[Local Authority],t.[Local Authority])

	from  [#Orgs]s
	left join  [#Temp3]ss on ss.PCDS_7 = left(s.POSTCODE,7)
	left join  [#Temp3]t on t.PCDS_6 = left(s.POSTCODE,6)

	where s.[Local Authority] is null 


	update s

	set s.[Local Authority] = coalesce(u.[Local Authority],v.[Local Authority])

	from  [#Orgs]s
	left join  [#Temp3]u on u.PCDS_5 = left(s.POSTCODE,5)
	left join  [#Temp3]v on v.PCDS_4 = left(s.POSTCODE,4)

	where s.[Local Authority] is null 


			if object_id('tempdb..#TempOT') is not null
			drop table #TempOT

			SELECT   b.Description as OrgTypeName
			,a.EnterpriseID
			,ROW_NUMBER() over (partition by a.EnterpriseID order by b.Code ) as rn
			into #TempOT
			from [ETL_PROD].[dbo].[CMC_Organization]a
			inner join [ETL_PROD].[dbo].[CMC_Organization_OrganizationTypeCodes]aa on aa.organization = a.ItemId
			inner join [ETL_PROD].[dbo].[Coded_OrgType]b on b.Code = aa.OrganizationType
			inner join
					(
					select distinct EnterpriseID from #Orgs where OrgType is null
					)c on c.EnterpriseID = a.EnterpriseID
  --where Organization = '100043269o'

  --select * from #TempOT order by EnterpriseID
  delete from #TempOT where rn = 2


  Update f
		set f.OrgType = g.OrgTypeName
  from [#Orgs]f
  inner join #TempOT g on g.EnterpriseID = f.EnterpriseID 

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
			 	   if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_Organisation_to_Department_Provider_Directory]') is not null
 		drop table [AT_Organisation_to_Department_Provider_Directory]
			
			select
			
			EnterpriseID,
			ParentOrganizationEID,
			Team,
			DeptODSCode,
			DeptPDRegistryId,
			Parent,
			IsParentFlag,
			OrgType,
			b.TeamType as DerivedTeamType,
			a.PostCode,
			a.CCG,
			--c.NEW_CCG_Pseudo,
			c.STP,
			[Local Authority],
			ORgOpenDate,
			ORgCloseDate

			into [AT_Organisation_to_Department_Provider_Directory]

			from #Orgs a
  left join [ETL_Local_PROD].[dbo].[AT_Commissioner_Team_Org_Lookup]b on b.ActivityTeam = a.Team
  left join  [ETL_Local_PROD].[Reference].[STP]c on c.CCGLONG_TRUNC = a.CCG
			order by Parent, IsParentFlag desc,Team


			 --select * from #Orgs order by Parent
 --select * from  [AT_Organisation_to_Department_Provider_Directory]



 end