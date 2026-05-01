			
	declare @StartDate Date
	set @StartDate = convert(datetime, cast(year(dateadd(month, -3, getdate())) as varchar(10)) + '-04-01', 120)
	print @StartDate

 	IF OBJECT_ID('[ETL_Local_PROD].[dbo].[AT_Login_Details]') IS NOT NULL 
	dROP TABLE [ETL_Local_PROD].[dbo].[AT_Login_Details]

	  SELECT  
	   a.[ItemId]
      ,[SessionID]
      ,[Active]
      ,[UserRegistryID]
      ,[OrganizationRegistryID]
      ,[UserID]
      ,[UserRole]
      ,[UserFullName]
      ,[OrganizationName]
	  ,b.TeamType
	  ,b.[Parent Org]
	  --, ROW_NUMBER() over (partition by [UserRegistryID] order by [LoginRowId] ) as rn
	  ,ROW_NUMBER() OVER(PARTITION BY [UserRegistryID] ORDER by [LoginRowId],[LogoutReason] ASC) AS RN2
	  ,null as RowBeforlaterFlag  
      ,[LoginTime]
      ,[LogoutTime]
	  ,Cast(null as datetime) as FinalLoginTime
	  ,Cast(null as datetime) as FinalLogoutTime
      ,[LogoutReason]
      ,[SessionDuration]
	  ,([SessionDuration]/60) as SessionDurationInMinutes
	  ,null as NewSessionDuratiom
	  ,null as NewSessionDurationMinutes
      ,[UserAgent]
	  ,case
		when [IsMobile] = 1 and  charindex('iPhone',[UserAgent])> 0 then 'iPhone'
		when [IsMobile] = 1 and  charindex('iPad',[UserAgent])> 0 then 'iPad'
		When [IsMobile] = 1 and  (CHARINDEX('Android',[UserAgent])>0 and CHARINDEX('Mobile Safari',[UserAgent])>0) then 'Android mobile'
		When [IsMobile] = 1 and  (CHARINDEX('Android',[UserAgent])>0 and CHARINDEX('Mobile',[UserAgent])= 0) then 'Android Tablet'
		When [IsMobile] = 0 and  (CHARINDEX('Linux',[UserAgent])>0 and CHARINDEX('Mobile',[UserAgent])= 0) then 'PC'
		when [IsMobile] = 0 and  charindex('Windows NT',[UserAgent])> 0 then 'PC'
		when [IsMobile] = 0 and  charindex('Macintosh; Intel Mac OS X',[UserAgent])> 0 then 'iPad'
		when [IsMobile] = 0 and (charindex('mobile',[UserAgent])>0 and charindex('Windows NT',[UserAgent])> 0)  then 'Tablet PC'
		when [IsMobile] = 0 and (charindex('mobile',[UserAgent])>0 and charindex('Macintosh; Intel Mac OS X',[UserAgent])> 0)  then 'iPad'
		else [UserAgent]
		End as DerivedDevicetype
      ,[Browser]
      ,[BrowserVersion]
      ,[BrowserMode]
      ,a.[LoginRowId]
      ,[IsMobile]

	  into [ETL_Local_PROD].[dbo].[AT_Login_Details]
	  --,b.*
  FROM [ETL_PROD].[dbo].[CMC_ActivityLog]a with (nolock)
  left join [ETL_Local_PROD].[dbo].[AT_Commissioner_Team_Org_Lookup]b with (nolock) on b.ActivityTeam = a.[OrganizationName]
  where 
 
  convert(date,loginTime) >= dateadd(year,-1,@StartDate)
 
  order by a.ItemId

 
		update x
				set x.RowBeforlaterFlag = Case when  (a.LogoutTime > x.LoginTime) then 1 else null end
 
		
		from [ETL_Local_PROD].[dbo].[AT_Login_Details]x		
		LEFT JOIN [ETL_Local_PROD].[dbo].[AT_Login_Details]a on  a.UserID = x.UserID
								and  a.rn2 = x.RN2 - 1

	
 

 
		update x
		set  x.FinalLoginTime = x.LoginTime,
			x.FinalLogoutTime = case when a.RowBeforlaterFlag = 1 and x.LogoutTime < a.LogoutTime then a.LogoutTime else x.LogoutTime end
			
		
		from [ETL_Local_PROD].[dbo].[AT_Login_Details]x		
		LEFT JOIN [ETL_Local_PROD].[dbo].[AT_Login_Details]a on  a.UserID = x.UserID
									and a.rn2 = (x.rn2 + 1)


		update r
				set r.NewSessionDuratiom = DATEDIFF(second,FinalLoginTime,FinalLogoutTime),
					r.NewSessionDurationMinutes = DATEDIFF(second,FinalLoginTime,FinalLogoutTime)/60

		from [ETL_Local_PROD].[dbo].[AT_Login_Details]r


		delete from [ETL_Local_PROD].[dbo].[AT_Login_Details] where RowBeforlaterFlag = 1




		declare @StartDate Date
	set @StartDate = convert(datetime, cast(year(dateadd(month, -3, getdate())) as varchar(10)) + '-04-01', 120)
	print @StartDate


			if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_CarePlanData]') is not null
			drop table [ETL_Local_PROD].[dbo].[AT_CarePlanData]
		  
			select 
				cast(ltrim(rtrim(RIGHT(GenusId,CHARINDEX('||',REVERSE(GenusId))-1)))as Nvarchar(75)) as GenusID,
				a.ItemId as AuditID,
				cast(null as varchar(36)) as AuditLoginID,
				cast(null as varchar(max)) as UserLoginId,
				cast (null as varchar(max)) as SystemID,
				cast (null as varchar(max)) as ExternalSessionId,
				cast(null as datetime) as LastLogin,
				cast(null as bit) as Success,
				cast (null as varchar(max)) as reason,
				cast(null as bit) as SoftFailure,
				actionTime,
				ActionType,
				a.comment,
				a.modelName,
				a.Recordname,
				a.indRegistryID as StaffRegistryID,
				a.OrgRegistryID as EnterpriseID,
				a.Roles,
				a.appID,
				a.LoginRowID,
				case ActionType when 'custom-consent_removed' then 'delete' else REPLACE(ActionType,'custom-','') end as DerivedActionType,
				--ROW_NUMBER() OVER(PARTITION BY [UserRegistryID] ORDER by [LoginRowId],[LogoutReason] ASC) AS RN2,
				null as ActivityOrder,
				cast (null as varchar(max)) as StaffFullName,
				cast (null as varchar(max)) as StaffUserId,
				cast (null as varchar(max)) as StaffActiveDescription,
				cast (null as varchar(max)) as StaffProviderTypeDescription,
				cast (null as varchar(255)) as PDRegistryID,
				cast (null as varchar(max)) as userRole,
				cast (null as varchar(max)) as OrganizationName,
				cast(null as datetime) as LoginTime,
				cast(null as datetime) as LogOutTime,
				cast (null as varchar(max)) as LogoutReason,
				cast(null as int)as SessionDuration,
				cast (null as varchar(max)) as UserAgent,
				cast (null as varchar(max)) as Browser,
				cast (null as varchar(max)) as BrowserVersion,
				cast (null as varchar(max)) as BrowserMode,
				cast(null as bit) as IsMobile,
				cast (null as varchar(max)) as ODSCode,
				cast(null as varchar(max)) as  PatientCCG,
				cast (null as varchar(255)) as  [Parent Org],
				cast (null as varchar(255)) as  TeamType, 
				cast(null as varchar(255)) as App,
				cast(null as varchar(max)) as Domain, 
				cast (null as varchar(255)) as  OrganisationCCG,
				cast (null as varchar(255)) as  OrgSTP,
				cast (null as varchar(255)) as  OrgNHSRegion
  
		  into [ETL_Local_PROD].[dbo].[AT_CarePlanData]


		  FROM [ETL_PROD].[dbo].[CMC_AuditData]a with (nolock)
	 

		  Where Convert(date,actionTime) >= @StartDate
		  and (Actor <> 'System'  )
		and RecordName = 'PatientSummary'
  

		--select *  from  [AT_NewTeamPersonActivityTest] where  LoginRowID = 1050930 
		--select *  from  [ETL_PROD].[dbo].[CMC_ActivityLog] where  LoginRowID = 1050930 


		update r

				set r.userRole = s.userRole,
					r.OrganizationName = s.OrganizationName,
					r.logintime = s.logintime,
					r.LogOutTime = s.LogOutTime,
			        r.LogoutReason = s.LogoutReason,
					r.SessionDuration = s.SessionDuration,
					r.UserAgent = s.UserAgent,
					r.Browser = s.Browser,
					r.BrowserVersion = s.BrowserVersion,
					r.BrowserMode = s.BrowserMode,
					r.IsMobile = s.IsMobile,
					r.StaffUserId = s.UserID
	
		from [ETL_Local_PROD].[dbo].[AT_CarePlanData]r
		left join [ETL_PROD].[dbo].[CMC_ActivityLog]s on s.LoginRowID = r.LoginRowID
	 



		update r

				set r.AuditLoginID = aa.ItemId,
					r.UserLoginId = aa.UserLoginId,
					r.SystemID = aa.SystemID,
					r.ExternalSessionId = aa.ExternalSessionId,
					r.LastLogin = aa.LastLogin,
					r.success = aa.Success,
					r.reason = aa.reason,
					r.SoftFailure = aa.SoftFailure,
					r.Domain = aa.Domain

		from [ETL_Local_PROD].[dbo].[AT_CarePlanData]r
		left join [ETL_PROD].[dbo].[CMC_AuditLogin]aa on aa.ItemId = r.LoginRowId


		--select top 1 * from [AT_NewTeamPersonActivityTest]
 





		update r

	
		set r.App = case r.Domain when '%HS_EMIS' then 'EMIS' when '%HS_PC' then 'Portal' when '%HS_Smartcard' then 'Smartcard' else 'CMC' end, 
			r.StaffFullName =  ltrim(coalesce(b.StaffTitle,'')+' '+ coalesce(b.StaffForename,'')+' '+ coalesce(b.StaffSurname,'')),
			r.StaffActiveDescription =  b.StaffActiveDescription,
			r.StaffProviderTypeDescription =  b.StaffProviderTypeDescription
 

		from [ETL_Local_PROD].[dbo].[AT_CarePlanData]r
		inner join  [ETL_Local_PROD].[dbo].[AT_Staff]b with (nolock) on b.StaffRegistryID =  r.StaffRegistryID


		update r

		set r.odscode = d.ODSCode,
			r.[Parent Org] = f.[Parent Org],
			r.TeamType = f.TeamType,
			r.OrganisationCCG = g.CCG,
			r.OrgSTP = g.STP,
			r.OrgNHSRegion = g.[NHS Region],
			r.PDRegistryID =  d.PDRegistryID 

		from [ETL_Local_PROD].[dbo].[AT_CarePlanData]r
		  inner join [ETL_PROD].[dbo].[CMC_Organization]d with (nolock) on d.PDRegistryID  = r.EnterpriseID
		  inner join [ETL_PROD].[dbo].[CMC_OrganizationName]e with (nolock) on e.ItemID = d.name
		  left join [ETL_Local_PROD].[dbo].[AT_Commissioner_Team_Org_Lookup]f with (nolock) on ltrim(rtrim(f.ActivityTeam)) = r.OrganizationName
		  left join [ETL_Local_PROD].[dbo].[AT_ODS_Data]g on g.[Organisation Code] = d.ODSCode
	


	update r

		set r.PatientCCG = isnull(CCG,'(Care plan not currently published)') 


	from [ETL_Local_PROD].[dbo].[AT_CarePlanData]r
	left join [ETL_Local_PROD].[dbo].[AT_Patient_General]h with (nolock) on h.CMC_ID = ltrim(rtrim(r.GenusID))




	update r

			set r.LoginTime = coalesce(d.FinalLoginTime,r.LoginTime),
				r.LogOutTime = coalesce(d.FinalLogoutTime,r.logouttime)


	from [ETL_Local_PROD].[dbo].[AT_CarePlanData]r
	left join [ETL_Local_PROD].[dbo].[AT_Login_Details]d on d.LoginRowId = r.LoginRowId




		  --select top 500  * FROM  [ETL_Local_PROD].[dbo].[AT_CarePlanData] where genusid = 100050482 and DerivedActionType = 'view' and LogoutReason <> 'second_login'


		  
	select * from [ETL_Local_PROD].[dbo].[AT_CarePlanData] where LoginRowId in( 877245,877267) and DerivedActionType = 'View'
	 select * from [ETL_Local_PROD].[dbo].[AT_Login_Details] where LoginRowId in( 877245,877267)


	select * from [ETL_Local_PROD].[dbo].[AT_CarePlanData] where DerivedActionType = 'view' and LogoutReason <> 'second_login' order by ActionTime


		select * from [ETL_Local_PROD].[dbo].[AT_CarePlanViewData] where CMC_ID = 100050482 order by ActionTime
		select * from [ETL_Local_PROD].[dbo].[AT_CarePlanData] where GenusID = 100050482 and DerivedActionType = 'view' order by ActionTime