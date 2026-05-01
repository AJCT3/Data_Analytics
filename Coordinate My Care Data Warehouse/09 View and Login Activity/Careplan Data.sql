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
	  ,f.TeamType
	  ,f.[Parent Org]
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
	  , g.CCG as OrganisationCCG
		,g.STP as OrgSTP
		 ,g.[NHS Region] as OrgNHSRegion

	  into [ETL_Local_PROD].[dbo].[AT_Login_Details]
	  --,b.*
  FROM [ETL_PROD].[dbo].[CMC_ActivityLog]a with (nolock)
  --left join [ETL_PROD].[dbo].[CMC_AuditLogin]b on b.CspSessionId = a.SessionID
  	  inner join [ETL_PROD].[dbo].[CMC_Organization]d with (nolock) on d.PDRegistryID  = a.OrganizationRegistryID
		  inner join [ETL_PROD].[dbo].[CMC_OrganizationName]e with (nolock) on e.ItemID = d.name
		  left join [ETL_Local_PROD].[dbo].[AT_Commissioner_Team_Org_Lookup]f with (nolock) on f.ActivityTeam = a.[OrganizationName]
		  left join [ETL_Local_PROD].[dbo].[AT_ODS_Data]g on g.[Organisation Code] = ODSCode


 

  where 
  --OrganizationName = 'LONDON AMBULANCE SERVICE NHS TRUST'
  --and 
  --convert(date,loginTime) >= '2020-01-01' and convert(date,loginTime)  <= '2020-01-31'
  convert(date,loginTime) >= dateadd(year,-1,@StartDate)
  --and LogoutReason = 'timeout'
  --and b.TeamType = 'Urgent Care'
  --and USERID = 'AbeyewardeneOrani'
  --and b.Success = 1
  --and b.LastLogin = a.LoginTime
  order by a.ItemId

  --select * from [ETL_Local_PROD].[dbo].[AT_Commissioner_Team_Org_Lookup]
  	/** 
	
		this part flags multiple rows for the same patient
		
	**/

--select top 50 * FROM [ETL_PROD].[dbo].[CMC_ActivityLog]
 
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
				
				
					IF OBJECT_ID('tempdb..#tempCreate') IS NOT NULL 
					 dROP TABLE #tempCreate
 
				select
				distinct
				cmc_id
				into #tempCreate
				 FROM [ETL_Local_PROD].[dbo].[AuditPatient_New]
				 where charindex('create',ActionType)>0



					declare @StartDate Date
	set @StartDate = convert(datetime, cast(year(dateadd(month, -3, getdate())) as varchar(10)) + '-04-01', 120)
	print @StartDate
				
				
				
				if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_CarePlanData]') is not null
		drop table [ETL_Local_PROD].[dbo].[AT_CarePlanData]
		  select 
		  a.CMC_ID,
		  actionTime,
		  ActionType,
		  comment,
		  case ActionType when 'custom-consent_removed' then 'delete' else REPLACE(ActionType,'custom-','') end as DerivedActionType,
		  cast(null as bigint) as ActivityOrderNo,
		  i.EnterpriseID as StaffEnterpriseID,
		 n.GivenName +' '+ n.FamilyName as StaffFullName,
		  cc.UserID as StaffUserId,
		  i.ODSCode as StaffODSCode,
		 tu.Description as  StaffActiveDescription,
		  tt.Description as StaffProviderTypeDescription,
		 d.PDRegistryID,
		 e.ItemId,
		  e.Name as Team,
		  h.ccg as OriginalOrgCCG,
		  [Parent CCG] as OrgCCG,
		  h.[NHS Region] ,
		  d.ODSCode,
		  d.EnterpriseID,
		  isnull(g.CCG,'(Care plan not currently published)') as PatientCCG,
		  --d.name  as OrgNameName
		  f.[Parent Org],
		  f.TeamType,
		 case Domain when '%HS_EMIS' then 'EMIS' when '%HS_PC' then 'Portal' when '%HS_Smartcard' then 'Smartcard' else 'CMC' end as App
		  --,
		  --f.Parent as ParentOrg

  
		  into [ETL_Local_PROD].[dbo].[AT_CarePlanData]


		  FROM [ETL_Local_PROD].[dbo].[AuditPatient_New]a with (nolock)
		  left join [ETL_PROD].[dbo].[CMC_AuditLogin]aa with (nolock) on aa.ItemId = a.LoginReference
		  --inner join  [ETL_Local_PROD].[dbo].[AT_Staff]b with (nolock) on b.StaffRegistryID = a.StaffRegistryID
		  left join ETL_PROD.dbo.CMC_Individual i on i.EnterpriseID = a.StaffRegistryID
			left join ETL_PROD.dbo.CMC_Name n on i.Name = n.ItemId
			left join ETL_PROD.dbo.CMC_UserIdentifier u on i.PDRegistryID = u.Extension
			left join ETL_PROD.dbo.CMC_UserClinician cc on u.UserClinician = cc.itemid
			left join ETL_PROD.dbo.Coded_IndStatus tu on i.StatusCode = tu.Code
			left join (select *,ROW_NUMBER() over (PARTITION by individual order by individual) as rn from ETL_PROD.dbo.CMC_Individual_ProviderTypes) pt on i.ItemId = pt.Individual and pt.rn=1
			left join ETL_PROD.dbo.Coded_IndType tt on pt.ProviderType = tt.Code
		  inner join [ETL_PROD].[dbo].[CMC_Organization]d with (nolock) on d.PDRegistryID  = a.DeptPDRegistryId
		  inner join [ETL_PROD].[dbo].[CMC_OrganizationName]e with (nolock) on e.ItemID = d.name
		  left join [ETL_Local_PROD].[dbo].[AT_Commissioner_Team_Org_Lookup]f with (nolock) on ltrim(rtrim(f.ActivityTeam)) = ltrim(rtrim(e.Name))
		  left join [ETL_Local_PROD].[dbo].[AT_Patient_General]g with (nolock) on g.CMC_ID = a.CMC_ID
		  left join [ETL_Local_PROD].[dbo].[AT_ODS_Data]h on h.[Organisation Code] = d.ODSCode
  --where e.Name in ('The Argyle Nursing Homes Service')

		  Where a.CMC_ID in (select cmc_id from #tempCreate)

		--  select * from ETL_PROD.dbo.CMC_Individual_ProviderTypes
		-- select * from  [ETL_PROD].[dbo].[CMC_Organization]
		-- select * from  [ETL_PROD].[dbo].[CMC_OrganizationName]
		--select * from   ETL_PROD.dbo.Coded_IndType
		 --select top 5 * from  [ETL_PROD].[dbo].[CMC_AuditLogin]
--		  ALTER TABLE [ETL_Local_PROD].[dbo].[AT_CarePlanData]
--ADD ActivityOrderNo BigInt;


update r
		set r.OrgCCG = h.CCG,
			r.[NHS Region] = h.[NHS Region],
			r.ODSCode = h.[Organisation Code]
		
from [ETL_Local_PROD].[dbo].[AT_CarePlanData]r
left join [ETL_Local_PROD].[dbo].[AT_ODS_Data]h on h.[Parent Org Name] = ltrim(rtrim(r.[Parent Org]))

where OrgCCG is Null


update r
		set r.OrgCCG = h.CCG,
			r.[NHS Region] = h.[NHS Region],
			r.ODSCode = h.[Organisation Code]
		
from [ETL_Local_PROD].[dbo].[AT_CarePlanData]r
left join [ETL_Local_PROD].[dbo].[AT_ODS_Data]h on h.[Organisation Name] = ltrim(rtrim(r.Team))

where OrgCCG is Null



update r
		set r.ActivityOrderNo = a.RN2
		
from [ETL_Local_PROD].[dbo].[AT_CarePlanData]r
inner join 
			(
			select
			CMC_ID,
			DerivedActionType, 
			actiontime,
			ROW_NUMBER() OVER(PARTITION BY CMC_ID,DerivedActionType ORDER by actiontime  ) AS RN2
			from [ETL_Local_PROD].[dbo].[AT_CarePlanData]
		 
			)a on a.CMC_ID = r.CMC_ID
			and a.ActionTime = r.ActionTime
				and a.DerivedActionType = r.DerivedActionType



update r
		set r.OriginalOrgCCG = coalesce(OriginalOrgCCG,orgccg)
		
from [ETL_Local_PROD].[dbo].[AT_CarePlanData]r

	 
		 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_CarePlanData_WithLoginDetails]') is not null
		 drop table [ETL_Local_PROD].[dbo].[AT_CarePlanData_WithLoginDetails]
		  Select
			b.ItemId As LoginItemID, 
			a.*, 
			b.FinalLoginTime, 
			b.FinalLogoutTime, 
			b.NewSessionDurationMinutes As SessionDurationMinutes, 
			b.LogoutReason, 
			b.UserRole, 
			b.UserAgent, 
			b.DerivedDevicetype, 
			b.Browser, 
			b.BrowserVersion, 
			b.LoginRowId 

			into [ETL_Local_PROD].[dbo].[AT_CarePlanData_WithLoginDetails]

			From ETL_Local_PROD.dbo.AT_CarePlanData a 
			Left Join ETL_Local_PROD.dbo.AT_Login_Details b On b.UserID = a.StaffUserId 
														And b.FinalLoginTime <= a.actionTime 
														And b.FinalLogoutTime >= a.actionTime 
			--Where (b.LogoutReason <> 'second_login' Or b.LogoutReason Is Null) 
			--And a.[Parent Org] = 'LONDON AMBULANCE SERVICE NHS TRUST' 
		 --and CMC_ID = 100050125

			And a.DerivedActionType = 'view' 
			Order By a.actionTime



 
		  Select
			b.ItemId As LoginItemID, 
			a.*, 
			b.FinalLoginTime, 
			b.FinalLogoutTime, 
			b.NewSessionDurationMinutes As SessionDurationMinutes, 
			b.LogoutReason, 
			b.UserRole, 
			b.UserAgent, 
			b.DerivedDevicetype, 
			b.Browser, 
			b.BrowserVersion, 
			b.LoginRowId ,
			ROW_NUMBER() OVER(PARTITION BY CMC_ID,b.LoginRowId,DerivedActionType ORDER by actiontime  ) AS RN2
		 

			From ETL_Local_PROD.dbo.AT_CarePlanData a 
			Left Join ETL_Local_PROD.dbo.AT_Login_Details b On b.UserID = a.StaffUserId 
														And b.FinalLoginTime <= a.actionTime 
														And b.FinalLogoutTime >= a.actionTime 
			--Where (b.LogoutReason <> 'second_login' Or b.LogoutReason Is Null) 
			--And a.[Parent Org] = 'LONDON AMBULANCE SERVICE NHS TRUST' 
		 and CMC_ID = 100050125
			and comment = 'Published, View-only'
			And a.DerivedActionType = 'view' 
			Order By a.actionTime

			select * from [ETL_PROD].[dbo].[CMC_ActivityLog] where UserID = 'AbbottUrsula' and convert(date,LoginTime) = '2019-06-11'order by LoginTime


			select * FROM [ETL_Local_PROD].[dbo].[AuditPatient_New]where ActionTime between '2019-06-11 15:30:00.000' and '2019-06-11 16:15:00.000' order by ActionTime
			select * FROM [ETL_Local_PROD].[dbo].[AT_Patient_General] where CMC_ID = 100056724

			select * from ETL_Local_PROD.dbo.AT_CarePlanData  where CMC_ID = 100017316    order by ActionTime
			select * from [ETL_Local_PROD].[dbo].[AuditPatient_New]where CMC_ID = 100056724 and  ActionType = 'Publish'  and ActionTypeOrder = 1
			
			
			
			SELECT *
  FROM [ETL_Local_PROD].[dbo].[AT_ODS_Data]
  where charindex ('BARNDOC',[Organisation Name])>0
			--select * from ETL_Local_PROD.dbo.AT_Login_Details where UserID = 'AbbottUrsula' order by LoginTime
			select * from [ETL_PROD].[dbo].[CMC_ActivityLog] where UserID = 'AbbottUrsula' and convert(date,LoginTime) = '2019-06-11'order by LoginTime


				select distinct team, [Parent Org],OrgCCG,PatientCCG   from ETL_Local_PROD.dbo.AT_CarePlanData  where DerivedActionType = 'publish'
					and ActivityOrderNo = 1

					select 
					[Parent Org],
					actiontime,
					Count(*) as Total  
					from ETL_Local_PROD.dbo.AT_CarePlanData 
					where OrgCCG ='NHS Haringey CCG' 
					and DerivedActionType = 'publish'
					and ActivityOrderNo = 1
					group by [Parent Org],ActionTime 


					select 
					*
					from ETL_Local_PROD.dbo.AT_CarePlanData 
					--where OrgCCG ='WHITTINGTON'
					where charindex ('WHITTINGTON',team)>0
					and DerivedActionType = 'publish'
					and ActivityOrderNo = 1
					and convert(date,ActionTime) >= '2019-04-01'


					select 
					GP_Practice,
					Date_Original_Approval,
					CMC_ID
					
					FROM [ETL_Local_PROD].[dbo].[AT_Patient_General]
					where CCG ='NHS Haringey CCG' 
					and convert(date,Date_Original_Approval) >= '2019-04-01'

					charindex ('WHITTINGTON',[Organisation Name])>0
						
			SELECT *
  FROM [ETL_Local_PROD].[dbo].[AT_ODS_Data]
  where charindex ('WHITTINGTON',[Organisation Name])>0 --and charindex ('meadow',[Organisation Name])>0

  	SELECT *
  FROM [ETL_Local_PROD].[dbo].[AT_ODS_Data]
  where charindex ('ROYAL TRINITY',[Parent Org Name])>0 




	select 
					*
					from ETL_Local_PROD.dbo.AT_CarePlanData 
					where charindex ('GUY''S AND ST THOMAS'' NHS FOUNDATION TRUST',team)>0
					where OrgCCG ='NHS Haringey CCG' 
					and DerivedActionType = 'publish'
					and ActivityOrderNo = 1
					and convert(date,ActionTime) >= '2019-04-01'


  					
			SELECT distinct ODSType
  FROM [ETL_Local_PROD].[dbo].[AT_ODS_Data]

  select distinct TeamType from [AT_Commissioner_Team_Org_Lookup]