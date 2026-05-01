
		if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_NewTeamPersonActivityTest]') is not null
		drop table [ETL_Local_PROD].[dbo].[AT_NewTeamPersonActivityTest]
		  select 
		  a.CMC_ID,
		  actionTime,
		  ActionType,
		  case ActionType when 'custom-consent_removed' then 'delete' else REPLACE(ActionType,'custom-','') end as DerivedActionType,
		  b.StaffEnterpriseID,
		  b.StaffForename,
		  b.StaffSurname,
		   ltrim(coalesce(b.StaffTitle,'')+' '+ coalesce(b.StaffForename,'')+' '+ coalesce(b.StaffSurname,'')) as StaffFullName,
		  StaffUserId,
		  StaffODSCode,
		  StaffActiveDescription,
		  StaffProviderTypeDescription,
		 d.PDRegistryID,
		 e.ItemId,
		  e.Name as Team,
		  d.ODSCode,
		  d.EnterpriseID,
		  isnull(CCG,'(Care plan not currently published)') as PatientCCG,
		  --d.name  as OrgNameName
		  f.[Parent Org],
		  f.TeamType,
		 case Domain when '%HS_EMIS' then 'EMIS' when '%HS_PC' then 'Portal' when '%HS_Smartcard' then 'Smartcard' else 'CMC' end as App
		  --,
		  --f.Parent as ParentOrg

  
		  into [ETL_Local_PROD].[dbo].[AT_NewTeamPersonActivityTest]


		  FROM [ETL_Local_PROD].[dbo].[AuditPatient_New]a
		  left join [ETL_PROD].[dbo].[CMC_AuditLogin]aa on aa.ItemId = a.LoginReference
		  inner join  [ETL_Local_PROD].[dbo].[AT_Staff]b on b.StaffRegistryID = a.StaffRegistryID
		  inner join [ETL_PROD].[dbo].[CMC_Organization]d on d.PDRegistryID  = a.DeptPDRegistryId
		  inner join [ETL_PROD].[dbo].[CMC_OrganizationName]e on e.ItemID = d.name
		  left join [ETL_Local_PROD].[dbo].[AT_Commissioner_Team_Org_Lookup]f on f.ActivityTeam = e.Name
		  left join [ETL_Local_PROD].[dbo].[AT_Patient_General]g on g.CMC_ID = a.CMC_ID
  --where e.Name in ('The Argyle Nursing Homes Service')

		  Where Convert(date,actionTime) >= '2019-04-01'
 
 
		  --and 
		  --(
				--charindex('print',ActionType)>0
		  --or	charindex('view',ActionType)>0
		  --or	charindex('revise',ActionType)>0
		  --or	charindex('create',ActionType)>0
		  --or	charindex('consent_removed',ActionType)>0
		  --or	charindex('publish',ActionType)>0

		  --)
 
  
			order by OverAllOrder



			--select distinct Domain from [ETL_PROD].[dbo].[CMC_AuditLogin]


			select  
				top 50000 * from[ETL_PROD].[dbo].[CMC_AuditLogin]

			select  * from [ETL_Local_PROD].[dbo].[AT_NewTeamPersonActivityTest] where TeamType in  ('Urgent Care','Urgent Care Centre') and DerivedActionType = 'view'

 	  			select  
				top 50000 a.* ,b.*
				FROM [ETL_Local_PROD].[dbo].[AuditPatient_New] a
				inner join [ETL_PROD].[dbo].[CMC_AuditLogin]b on b.ItemId = a.LoginReference


				select count(*) FROM [ETL_Local_PROD].[dbo].[AT_Patient_General] where DoD_PDS is not null --37384

 select 
 CMC_ID,
 StaffFullName,
 convert(date,ActioNtime) as ActionDate
 from [ETL_Local_PROD].[dbo].[AT_NewTeamPersonActivityTest]

 where team in  ('The Argyle Nursing Homes Service')
 and derivedactiontype = 'Create'
 union all
 select  
 CMC_ID, 
 null as StaffFullName,
 Date_Original_Approval
  FROM [ETL_Local_PROD].[dbo].[AT_Patient_General] where GP_Practice in  ('The Argyle Nursing Homes Service')
 and cmc_ID not in ( select 
 CMC_ID
 from [ETL_Local_PROD].[dbo].[AT_NewTeamPersonActivityTest]

 where team in  ('The Argyle Nursing Homes Service')
 and derivedactiontype = 'Create'
  )

	 

 select 
 * 
 from [ETL_Local_PROD].[dbo].[AT_NewTeamPersonActivityTest]

 --where (charindex('Imperial',[Parent Org])>0 OR charindex('Imperial',TeamType)>0)
 --and team <> 'IMPERIAL COLLEGE HEALTH CENTRE'

-- Update x

--	set x.TeamType	= 'Urgent Care'

--from  [ETL_Local_PROD].[dbo].[AT_Commissioner_Team_Org_Lookup]x

--select * from [ETL_Local_PROD].[dbo].[AT_Commissioner_Team_Org_Lookup] order by teamtype

-- where ActivityTeam in ('CARE UK - CMC USER')
select top 80 * from [ETL_Local_PROD].[dbo].[AT_AuditApprovals]

			 select * from [ETL_Local_PROD].[dbo].[AT_NewTeamPersonActivityTest] where [Parent Org] is null
			select  * FROM [ETL_Local_PROD].[dbo].[AT_Commissioner_Team_Org_Lookup]
			select distinct Name from [ETL_PROD].[dbo].[CMC_OrganizationName] order by name

    select  * FROM[ETL_PROD].[dbo].[CMC_Organization] where EnterpriseID =  100043410
   select  * FROM[ETL_PROD].[dbo].[CMC_Organization] where  PDRegistryID = '198234'
      select  * FROM [ETL_PROD].[dbo].[CMC_OrgToOrg] where EnterpriseID = 100099147
	  select  * FROM [ETL_PROD].[dbo].[CMC_OrgToOrg] where itemid = '100043410O'
	     select * FROM [ETL_PROD].[dbo].[CMC_OrganizationName] where charindex('Argyle',Name)>0 and source = 'PD' order by name

select * FROM[ETL_Local_PROD].[dbo].[AT_PDToOrg] where ParentOrganizationEID = 100079332

   select top 5* FROM [ETL_PROD].[dbo].[CMC_OrgToOrg] where EnterpriseID = 100043410
   --select top 5* FROM[ETL_Local_PROD].[dbo].[AT_PDToOrg]
   select top 5* FROM[ETL_PROD].[dbo].[CMC_Organization]
   select top 5* FROM [ETL_PROD].[dbo].[CMC_OrganizationName]
  select  * FROM [ETL_PROD].[dbo].[CMC_IndToOrg] where  enterpriseID = 100079332
    select  * FROM[ETL_PROD].[dbo].[CMC_Organization]
 select top 5* from [ETL_Local_PROD].[dbo].[AT_PDToOrg]
select top 5* from  [ETL_Local_PROD].[dbo].[AT_Staff]
  select top 5* from [ETL_Local_PROD].[dbo].[AT_Dept]
 select top 5* FROM [ETL_Local_PROD].[dbo].[AuditPatient_New]
select distinct  ActionType FROM [ETL_Local_PROD].[dbo].[AuditPatient_New] order by ActionType

select * from  [ETL_Local_PROD].[dbo].[AT_Staff] where StaffEnterpriseID = 100079332
    select  * FROM [ETL_PROD].[dbo].[CMC_IndToOrg] where  EnterpriseID = 100079332
  select top 10* from [ETL_Local_PROD].[dbo].[AT_IndToOrg]  where StaffEnterpriseID = 100079332
    select  * FROM [ETL_PROD].[dbo].[CMC_IndToOrg] where  IndividualEID = 100079332

select * from    ETL_PROD.dbo.CMC_UserClinician order by UserClinicianType

select * from   ETL_PROD.dbo.CMC_Individual

select top 10* from [ETL_Local_PROD].[dbo].[AT_IndToOrg] where StaffEnterpriseID = 100079332


select 'Audit_Table'
select top 50*  FROM [ETL_Local_PROD].[dbo].[AuditPatient_New] where StaffRegistryID = 577405

select 'AT_Staff'
select * from  [ETL_Local_PROD].[dbo].[AT_Staff] where StaffSurname = 'Opoku-Darko'
select 'CMC_IndToOrg'
   select  * FROM [ETL_PROD].[dbo].[CMC_IndToOrg] where  EnterpriseID = 100079332

select 'CMC_Organisation'
   select  * FROM[ETL_PROD].[dbo].[CMC_Organization] where  PDRegistryID = '568253'

  select  * from [ETL_Local_PROD].[dbo].[AT_PDToOrg]where charindex('Imperial',parent)>0

   select * FROM [ETL_PROD].[dbo].[CMC_OrganizationName] where charindex('Imperial',Name)>0
   

   select 
  a.CMC_ID,
  actionTime,
  ActionType,
  case ActionType when 'custom-consent_removed' then 'delete' else REPLACE(ActionType,'custom-','') end as DerivedActionType,
  b.StaffEnterpriseID,
  b.StaffForename,
  b.StaffSurname,
   ltrim(coalesce(b.StaffTitle,'')+' '+ coalesce(b.StaffForename,'')+' '+ coalesce(b.StaffSurname,'')) as StaffFullName,
  StaffUserId,
  StaffODSCode,
  StaffActiveDescription,
  StaffProviderTypeDescription,
  c.Deleted,
  e.Name as Team,
  d.ODSCode,
  d.EnterpriseID,
  f.ChildOrganizationEID,
  f.ParentOrganizationEID,
  h.Name as ParentOrg
  --,
  --f.Parent as ParentOrg

  
  --into [ETL_Local_PROD].[dbo].[AT_NewTeamPersonActivityTest]
  FROM [ETL_Local_PROD].[dbo].[AuditPatient_New]a
  inner join  [ETL_Local_PROD].[dbo].[AT_Staff]b on b.StaffRegistryID = a.StaffRegistryID
  inner join [ETL_PROD].[dbo].[CMC_IndToOrg]c on c.EnterpriseID = b.StaffEnterpriseID
  inner join [ETL_PROD].[dbo].[CMC_Organization]d on d.EnterpriseID = c.IndividualEID
  inner join [ETL_PROD].[dbo].[CMC_OrganizationName]e on e.ItemID = d.name
  left join [ETL_PROD].[dbo].[CMC_OrgToOrg]f on f.ChildOrganizationEID = d.EnterpriseID  

  inner join [ETL_PROD].[dbo].[CMC_Organization]g on g.EnterpriseID = f.ParentOrganizationEID
    inner join [ETL_PROD].[dbo].[CMC_OrganizationName]h on h.ItemID = g.name
  --where cmc_ID = 100062645
  Where Convert(date,actionTime) >= '2018-04-01'
  and   cmc_ID = 100055492
  --and ActionType not in ('custom-patient_found','custom-lr_claimed','custom-lr_expired','custom-print','custom-pds_update')
  and 
  (
		charindex('print',ActionType)>0
  or	charindex('view',ActionType)>0
  or	charindex('revise',ActionType)>0
  or	charindex('create',ActionType)>0
  or	charindex('consent_removed',ActionType)>0
  or	charindex('publish',ActionType)>0

  )
  --ActionType	like 'print%'
  --or ActionType	like 'view%'
  --or ActionType	like 'revise%'
  --or ActionType	like 'create%'
  -- or ActionType	like 'consent_removed'
  --)


order by OverAllOrder