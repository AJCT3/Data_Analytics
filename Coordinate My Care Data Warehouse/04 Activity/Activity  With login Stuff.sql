			if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_NewTeamPersonActivityTest]') is not null
		drop table [ETL_Local_PROD].[dbo].[AT_NewTeamPersonActivityTest]
		  select 
		  cast(REPLACE(a.GenusId,'PatientSummary||','')as Nvarchar(75)) as CMC_ID,
		  	cast(REPLACE(FromVersionId,'PatientSummary','PS')as Nvarchar(75))  as FromPatientSummary,
			cast(REPLACE(ToVersionId,'PatientSummary','PS')as Nvarchar(75))  as ToPatientSummary,
			cast(ltrim(rtrim(RIGHT(GenusId,CHARINDEX('||',REVERSE(GenusId))-1)))as Nvarchar(75)) as GenusID,
			a.ItemId as AuditID,
			aa.ItemId as LoginID,
			aa.UserLoginId,
			aa.SystemID,
			aa.ExternalSessionId,
			aa.LastLogin,
			aa.Success,
			aa.reason,
			aa.SoftFailure,
		  actionTime,
		  ActionType,
		  a.comment,
		  a.modelName,
		  a.Recordname,
		  a.indRegistryID,
		  a.OrgRegistryID,
		  a.Roles,
		  a.appID,
		  a.LoginRowID,
		  case ActionType when 'custom-consent_removed' then 'delete' else REPLACE(ActionType,'custom-','') end as DerivedActionType,
		  cast( null as int) as StaffEnterpriseID,
		  cast(null as varchar(500)) as  StaffForename,
		  cast(null as varchar(500)) as  StaffSurname,
		 (select splitdata from (select splitdata,ROW_NUMBER() over (order by offset) as rn from [ETL_Local_PROD].[dbo].[SplitString](Actor,'|')) sel1 where rn=1) as StaffRegistryId,
		 (select splitdata from (select splitdata,ROW_NUMBER() over (order by offset) as rn from [ETL_Local_PROD].[dbo].[SplitString](Actor,'|')) sel1 where rn=3) as DeptPDRegistryId,
		   ltrim(coalesce(b.StaffTitle,'')+' '+ coalesce(b.StaffForename,'')+' '+ coalesce(b.StaffSurname,'')) as StaffFullName,
		  cast(null as varchar(max)) as  StaffUserId,
		  cast(null as varchar(max)) as  StaffODSCode,
		  cast(null as varchar(max)) as  StaffActiveDescription,
		  cast(null as varchar(max)) as  StaffProviderTypeDescription,
		  cast(null as varchar(255)) as  PDRegistryID,--d
		  cast(null as varchar(36)) as  CMC_OrganizationNameItemId,
		  cast(null as varchar(max)) as  Team,--e.name
		  cast(null as varchar(max)) as  ODSCode,--d
		  cast( null as int) as EnterpriseID,
		  --isnull(CCG,'(Care plan not currently published)') as PatientCCG,
		  cast(null as varchar(max)) as  PatientCCG,
		  --d.name  as OrgNameName
		  cast(null as nvarchar(255)) as  [Parent Org], --f
		  cast(null as varchar(255)) as  TeamType, --f
		 case aa.Domain when '%HS_EMIS' then 'EMIS' when '%HS_PC' then 'Portal' when '%HS_Smartcard' then 'Smartcard' else 'CMC' end as App,
		 aa.Domain 

		  --,
		  --f.Parent as ParentOrg

  
		  into [ETL_Local_PROD].[dbo].[AT_NewTeamPersonActivityTest]


		  FROM [ETL_PROD].[dbo].[CMC_AuditData]a with (nolock)
		  left join [ETL_PROD].[dbo].[CMC_AuditLogin]aa with (nolock) on aa.ItemId = a.LoginRowId
		  left join [ETL_PROD].[dbo].[CMC_ActivityLog]bb on bb.SessionID = aa.CspSessionId
		  inner join  [ETL_Local_PROD].[dbo].[AT_Staff]b with (nolock) on b.StaffRegistryID =  bb.UserRegistryID
		  inner join [ETL_PROD].[dbo].[CMC_Organization]d with (nolock) on d.PDRegistryID  = bb.OrganizationRegistryID
		  inner join [ETL_PROD].[dbo].[CMC_OrganizationName]e with (nolock) on e.ItemID = d.name
		  left join [ETL_Local_PROD].[dbo].[AT_Commissioner_Team_Org_Lookup]f with (nolock) on ltrim(rtrim(f.ActivityTeam)) = bb.OrganizationName
		  --left join [ETL_Local_PROD].[dbo].[AT_Patient_General]g with (nolock) on g.CMC_ID = cast(REPLACE(a.GenusId,'PatientSummary||','')as Nvarchar(75))
  --where e.Name in ('The Argyle Nursing Homes Service')

		  Where Convert(date,actionTime) >= '2019-04-01'
		  --select top 500 * from [AT_NewTeamPersonActivityTest]

		  select top 5* from [ETL_PROD].[dbo].[CMC_AuditData]



		  select 
		  top 
		  50 a.*,'AAAAAAAAAAAAAAAADDDDDDDDDDDDDDDDDDD',b.*
		  from [ETL_PROD].[dbo].[CMC_AuditLogin]a
		  left join [ETL_PROD].[dbo].[CMC_ActivityLog]b on b.SessionID = a.CspSessionId