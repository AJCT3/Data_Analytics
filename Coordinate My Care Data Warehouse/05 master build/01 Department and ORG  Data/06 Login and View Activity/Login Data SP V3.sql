USE [ETL_Local_PROD]
GO
/****** Object:  StoredProcedure [dbo].[AT_Loging_Data]    Script Date: 17/08/2020 12:34:41 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




ALTER PROCEDURE [dbo].[AT_Loging_Data] 
-- Amended for PD Upgrade
AS
BEGIN



	
	--declare @StartDate Date
	--set @StartDate = dateadd(year,-1,convert(datetime, cast(year(dateadd(month, -3, getdate())) as varchar(10)) + '-04-01', 120))
	--print @StartDate
		 


				IF OBJECT_ID('tempdb..#LoginStatus') IS NOT NULL 
					 dROP TABLE #LoginStatus
				select cspsessionid,num,
				case
				  when Success='Y' and Failure='N' then 'direct'
				  when Success='Y' and Failure='Y' then 'login'
				  else 'failed' end as LoginStatus 
				  into #LoginStatus
				  from
				(select cspsessionid,
				case max(cast(success as smallint)) when 1 then 'Y' else 'N' end as Success,
				case MIN(cast(success as smallint)) when 0 then 'Y' else 'N' end as Failure,
				COUNT(*) as num
				
				from etl_PROD.dbo.CMC_AuditLogin a
				group by cspsessionid) sel1
				order by CspSessionId,num

			

--declare @StartDate Date
--	set @StartDate = dateadd(year,-2,convert(datetime, cast(year(dateadd(month, -3, getdate())) as varchar(10)) + '-04-01', 120))


 	IF OBJECT_ID('[ETL_Local_PROD].[dbo].[AT_Login_Details]') IS NOT NULL 
	dROP TABLE [ETL_Local_PROD].[dbo].[AT_Login_Details]

		SELECT  
		a.[ItemId]
      ,a.[SessionID]
      ,[Active]
      ,[UserRegistryID]
	  ,cast(null as int) as StaffEnterpriseID
      ,[OrganizationRegistryID]
	  ,CAST(NULL AS INT) AS deptenterpriseid
	  ,cast(null as varchar(max)) as ODSCode
	  ,cast(null as varchar(max)) as StaffProviderTypeDescription
	  --,h.StaffProviderTypeDescription
	  ,cast(null as varchar(max)) as StaffActiveDescription
	  --,h.StaffActiveDescription
      ,[UserID]
      ,[UserRole]
      ,[UserFullName]
      ,[OrganizationName]
	  ,cast(null as date) as StaffOrgStartDate
	  ,cast(null as date) as StaffOrgEndDate
	  ,cast(null as varchar(255)) as TeamType
	  --,f.TeamType
	  ,cast(null as varchar(255)) as [Parent Org]
	  --,f.[Parent Org]
	  ,ROW_NUMBER() over (order by sessionid, a.itemid ) as rn
	  ,ROW_NUMBER() OVER(PARTITION BY [UserRegistryID] ORDER by [LoginTime],a.itemid  ASC) AS RN2
	  ,null AS RN3
	  ,null as RowBeforlaterFlag 
	  ,b.LastLogin
      ,[LoginTime]
      ,[LogoutTime]
	  ,Cast(null as datetime) as FinalLoginTime
	  ,Cast(null as datetime) as FinalLogoutTime
      ,a.[LogoutReason]
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
		,case
-- 16.3.17 MS success/failure now goes in here
-- need to look up EMIS organisation on failures [also wherever there is a single ind/org / no activity log entry, EMIS / auto-flagging]
  when Success = 0 then 'failed'
-- this works for now, we'll have to think of something later .....
  when domain = '%HS_EMIS' and (browser is null or loginstatus = 'direct') then 'direct'
  when domain = '%HS_Smartcard' and userid is not null then 'direct'
  else 'login' end as ActionType
      ,[Browser]
      ,[BrowserVersion]
      ,[BrowserMode]
      ,a.[LoginRowId]
      ,[IsMobile]
	  ,b.Domain
	  ,case b.Domain when '%HS_EMIS' then 'EMIS' when '%HS_PC' then 'Portal' when '%HS_Smartcard' then 'Smartcard' else 'CMC' end as App
	  ,b.Success
	  ,b.SoftFailure
	  ,cast(null as varchar(255)) as PostCode
	  ,cast(null as varchar(255)) as OrganisationCCG
	  --, g.CCG as OrganisationCCG
	  ,cast(null as varchar(255)) as OrgSTP
		--,g.STP as OrgSTP
		,cast(null as varchar(255)) as [NHS Region]
		 --,g.[NHS Region] as OrgNHSRegion

	  into [ETL_Local_PROD].[dbo].[AT_Login_Details]

  FROM [ETL_PROD].[dbo].[CMC_ActivityLog]a with (nolock)
  left join [ETL_PROD].[dbo].[CMC_AuditLogin]b with (nolock) on b.ItemId = a.LoginRowId 
  left join #LoginStatus i on i.CspSessionId = a.SessionID

  order by a.ItemId





  update x
	set x.StaffEnterpriseID = y.StaffEnterpriseID,
		x.deptenterpriseid = z.DeptEnterpriseID,
		x.StaffProviderTypeDescription = y.StaffProviderTypeDescription,
	    x.StaffActiveDescription = y.StaffActiveDescription,
		x.ODSCode = coalesce(z.[DeptODSCode],d.ODSCode)

  from [ETL_Local_PROD].[dbo].[AT_Login_Details]x
  left join [ETL_Local_PROD].[dbo].[AT_Staff]y on y.StaffRegistryId = x.UserRegistryID	
  left join [ETL_Local_PROD].[dbo].[AT_PD_Dept] z on z.DeptPDRegistryID = x.OrganizationRegistryID
  left join [ETL_PROD].[dbo].[CMC_Organization]d with (nolock) on d.PDRegistryID  = x.OrganizationRegistryID

   update x
	set x.StaffORgEndDate = io.EndDate,
		x.StaffOrgStartDate = io.StartDate

  from [ETL_Local_PROD].[dbo].[AT_Login_Details]x
  join [ETL_Local_PROD].[dbo].[AT_IndToOrg] io on io.StaffEnterpriseId = x.StaffEnterpriseId



  --select top 500  *   from  [ETL_Local_PROD].[dbo].[AT_Login_Details]
  update a


		set [OrganizationName] = case	
									 when a.OrganizationName = 'ST GEORGES UNIVERSITY HOSPITALS EMERGENCY DEPARTMENT' then 'ST GEORGE''S UNIVERSITY HOSPITALS EMERGENCY DEPARTMENT'
									 when a.OrganizationName = 'BARKING, HAVERING AND REDBRIDGE UNIVERSITY HOSPITALS NHS TRUST' then 'BARKING HAVERING AND REDBRIDGE UNIVERSITY HOSPITALS NHS TRUST'
									 when a.OrganizationName = 'CNWL Community Independence Service K and C' then 'CNWL Community Independence Service K & C'
									 when a.OrganizationName = 'ROYAL BROMPTON & HAREFIELD NHS FOUNDATION TRUST'then 'ROYAL BROMPTON and HAREFIELD NHS FOUNDATION TRUST' 
									 when a.OrganizationName in ('LCW','LCW OOH GPs') then 'LCW 111'
									 when a.OrganizationName = 'MARIE CURIE CANCER CARE' then 'MARIE CURIE'
									 when a.OrganizationName = 'MICHAEL SOBELL HOUSE/MVCC' then 'MICHAEL SOBELL HOUSE HOSPICE'
									 when a.OrganizationName in ('ST JOSEPHS HOSPICE ( )','ST JOSEPHS HOSPICE (DO NOT USE)','ST JOSEPHS HOSPICE') then 'ST.JOSEPH''S HOSPICE'
									 when a.OrganizationName = 'ST LUKES HOSPICE CMC USER (HARROW)' then 'ST LUKES HOSPICE (HARROW)'
									 when a.OrganizationName = 'TRINITY HOSPICE - CMC User' then 'THE ROYAL TRINITY HOSPICE - CMC User'
									 when a.OrganizationName = 'The Pines Care Home with Nursing' then 'THE PINES NURSING HOME'
									 when a.OrganizationName in ('UNIVERSITY COLLEGE HOSPITAL EMERGENCY DEPARTMENT','University College London Hospital A & E Dept') then 'UCLH EMERGENCY DEPARTMENT'
									 when a.OrganizationName = 'SOUTH LONDON DOCTORS URGENT CARE' then 'South London Doctors Urgent Care 111'
									 when a.OrganizationName = '111 LAS' then '111 LAS SEL CAS'
									 when a.OrganizationName = 'LONDON CENTRAL AND WEST UNSCHEDULED CARE COLLABORATIVE' then 'LCW 111'
									 else OrganizationName
									 end

  from [ETL_Local_PROD].[dbo].[AT_Login_Details]a





  update a

    set 
	    a.TeamType = f.TeamType,
	    a.[Parent Org] =  f.[Parent Org],
		a.PostCode = coalesce(hh.Postcode,g.postcode,f.postcode),
		a.OrganisationCCG = f.[Team Specified CCG]


  from  [ETL_Local_PROD].[dbo].[AT_Login_Details]a
	left join [ETL_PROD].[dbo].[CMC_Organization]d with (nolock) on d.PDRegistryID  = a.OrganizationRegistryID
	left join [ETL_PROD].[dbo].[CMC_OrganizationName]e with (nolock) on e.ItemID = d.name
	left join [ETL_Local_PROD].[dbo].[AT_Commissioner_Team_Org_Lookup]f with (nolock) on f.ActivityTeam = a.[OrganizationName]
	left join [ETL_Local_PROD].[dbo].[AT_ODS_Data]g on g.[Organisation Code] = a.ODSCode
	left join [ETL_Local_PROD].[ODSData].[searchods]hh on hh.ODS = a.ODSCode



	update a

		set a.PostCode = coalesce(hhh.postcode,gg.postcode,hhhh.postcode,ggg.postcode)


	from  [ETL_Local_PROD].[dbo].[AT_Login_Details]a
	
	left join [ETL_Local_PROD].[dbo].[AT_Commissioner_Team_Org_Lookup]f with (nolock) on f.ActivityTeam = a.[OrganizationName]
	left join [ETL_Local_PROD].[dbo].[AT_ODS_Data]gg on gg.[Organisation Name] = a.OrganizationName
	left join [ETL_Local_PROD].[ODSData].[searchods]hhh on hhh.[Team or GP] = a.OrganizationName
		left join [ETL_Local_PROD].[dbo].[AT_ODS_Data]ggg on ggg.[Organisation Name] = f.[Parent Org]
	left join [ETL_Local_PROD].[ODSData].[searchods]hhhh on hhhh.[Parent] = f.[Parent Org]

	where a.PostCode is null


		if object_id('tempdb..#Temp1') is not null
	 drop table #Temp1

		select 
		distinct  
		ltrim(rtrim(REPLACE(CCG, ' CCG', ''))) as CCG, 
		STP,
		[NHS England REgion] 
		into #Temp1
		from [ETL_Local_PROD].[dbo].[AT_ENGLISH_POSTCODE_DATA]




	update f
		set			f.OrganisationCCG =  coalesce(hh.[GP CCG],f.OrganisationCCG,i.ccg),
					f.OrgSTP = coalesce(hhh.stp,k.stp,i.stp),
					f.[NHS Region] = coalesce(k.[NHS England REgion],i.[NHS England REgion])


	from [ETL_Local_PROD].[dbo].[AT_Login_Details]f
	left join [ETL_Local_PROD].[ODSData].[searchods]hh on hh.ODS = f.ODSCode
	left join [ETL_Local_PROD].[Reference].[STP]hhh on hhh.CCGLONGNAME = hh.[GP CCG]
	left join [ETL_Local_PROD].[dbo].[AT_ENGLISH_POSTCODE_DATA]i on i.PCDs = f.Postcode
	left join #Temp1 k on k.CCG = f.OrganisationCCG

 

	 --select top 50 * from [ETL_Local_PROD].[dbo].[AT_Login_Details]where OrganizationName = 'LANFRANC MEDICAL CENTRE'
 



	IF OBJECT_ID('tempdb..#OrgS') IS NOT NULL 
	dROP TABLE #OrgS
	select
	distinct
	OrganizationName,
	deptenterpriseid

	into #OrgS

	FROM [ETL_Local_PROD].[dbo].[AT_Login_Details]
	Where deptenterpriseid is not null



	update f

		set f.deptenterpriseid = g.deptenterpriseid

	from [ETL_Local_PROD].[dbo].[AT_Login_Details]f
	left join #OrgS g on g.OrganizationName = f.OrganizationName
	where f.deptenterpriseid is null


		update x
				set x.RowBeforlaterFlag = Case when  (a.LogoutTime >= x.LoginTime) then 1 else null end
 
		
	from [ETL_Local_PROD].[dbo].[AT_Login_Details]x		
	LEFT JOIN [ETL_Local_PROD].[dbo].[AT_Login_Details]a on  a.UserID = x.UserID
							and  a.rn2 = x.RN2 - 1

	
		update x
	
	set  x.FinalLoginTime = x.LoginTime,
			x.FinalLogoutTime = case when a.RowBeforlaterFlag = 1 and x.LogoutTime < a.LogoutTime then a.LogoutTime else x.LogoutTime end
			--x.FinalLogoutTime = case when (a.LoginTime <= x.LogoutTime) and a.LogoutTime > x.LogoutTime then a.LogoutTime Else x.LogoutTime end
		
	from [ETL_Local_PROD].[dbo].[AT_Login_Details]x		
	LEFT JOIN [ETL_Local_PROD].[dbo].[AT_Login_Details]a on  a.UserID = x.UserID
								and a.rn2 = (x.rn2 + 1)


update x
		set  x.FinalLogoutTime = a.FinalLoginTime			
		
	from [ETL_Local_PROD].[dbo].[AT_Login_Details]x		
	LEFT JOIN [ETL_Local_PROD].[dbo].[AT_Login_Details]a on  a.UserID = x.UserID
								and a.rn2 = (x.rn2 + 1)
	where x.FinalLogoutTime is null



update r
		set r.NewSessionDuratiom = case when r.LogoutTime is null then null else  DATEDIFF(second,FinalLoginTime,FinalLogoutTime) end,
			r.NewSessionDurationMinutes = case when r.LogoutTime is null then null else  DATEDIFF(second,FinalLoginTime,FinalLogoutTime)/60 end

from [ETL_Local_PROD].[dbo].[AT_Login_Details]r


delete from [ETL_Local_PROD].[dbo].[AT_Login_Details] where  TeamType in ( 'EXCLUDE','CMC TEAM') 

		update d
				set d.RN3 = a.RN3
 
		
	from [ETL_Local_PROD].[dbo].[AT_Login_Details]d		
	LEFT JOIN (
				select 
				sessionID, 
				ItemID,
				ROW_NUMBER() OVER(PARTITION BY sessionid ORDER by itemid  ASC) AS RN3 
				from [ETL_Local_PROD].[dbo].[AT_Login_Details]
				where RowBeforlaterFlag is null
			)a on  a.SessionID = d.SessionID
			and a.ItemId = d.ItemId
						




							
 	IF OBJECT_ID('[ETL_Local_PROD].[dbo].[AT_Logins_Daily]') IS NOT NULL 
	dROP TABLE [ETL_Local_PROD].[dbo].[AT_Logins_Daily]
	SELECT  
       [UserRegistryID]
      ,[OrganizationRegistryID]
      ,[UserID]
      ,[UserRole]
      ,[UserFullName]
      ,[OrganizationName]
	  ,deptenterpriseid
	  ,StaffEnterpriseID
	  ,StaffORgStartDate
	  ,StaffORgEndDate
      ,[TeamType]
      ,[Parent Org]
      ,Convert(date,[FinalLoginTime]) as LoginDate
      ,sum([NewSessionDurationMinutes]) as SessionDuration
      ,[DerivedDevicetype]

	  ,PostCode
      ,[OrganisationCCG]
      ,[OrgSTP]
      ,[NHS Region]
	  ,Domain
	  ,App
	  ,ActionType
	  into [ETL_Local_PROD].[dbo].[AT_Logins_Daily]

	 FROM [ETL_Local_PROD].[dbo].[AT_Login_Details]
	 where RowBeforlaterFlag is null
	 group by
	   [UserRegistryID]
      ,[OrganizationRegistryID]
      ,[UserID]
      ,[UserRole]
      ,[UserFullName]
      ,[OrganizationName]
	  ,deptenterpriseid
	  ,StaffEnterpriseID
	  ,StaffORgStartDate
	  ,StaffORgEndDate
      ,[TeamType]
      ,[Parent Org]
	  ,Convert(date,[FinalLoginTime])
      ,[DerivedDevicetype]
	  ,PostCode
      ,[OrganisationCCG]
      ,[OrgSTP]
      ,[NHS Region]
	  ,Domain
	  ,App
	  ,ActionType



	  --Hourly


	   	IF OBJECT_ID('[ETL_Local_PROD].[dbo].[AT_Logins_Hourly]') IS NOT NULL 
	dROP TABLE [ETL_Local_PROD].[dbo].[AT_Logins_Hourly]
	SELECT  
       [UserRegistryID]
      ,[OrganizationRegistryID]
      ,[UserID]
      ,[UserRole]
      ,[UserFullName]
      ,[OrganizationName]
	  ,deptenterpriseid
	  ,StaffEnterpriseID
	  ,StaffORgStartDate
	  ,StaffORgEndDate
      ,[TeamType]
      ,[Parent Org]
      ,Convert(date,[FinalLoginTime]) as LoginDate
	  ,DATEPART(HOUR, [FinalLoginTime]) as LoginHour
      ,sum([NewSessionDurationMinutes]) as SessionDuration
      ,[DerivedDevicetype]
	  ,PostCode
      ,[OrganisationCCG]
      ,[OrgSTP]
      ,[NHS Region]
	  ,Domain
	  ,App
	  ,ActionType
	  into [ETL_Local_PROD].[dbo].[AT_Logins_Hourly]

	 FROM [ETL_Local_PROD].[dbo].[AT_Login_Details]
	 where RowBeforlaterFlag is null
	 group by
	   [UserRegistryID]
      ,[OrganizationRegistryID]
      ,[UserID]
      ,[UserRole]
      ,[UserFullName]
      ,[OrganizationName]
	  ,deptenterpriseid
	  ,StaffEnterpriseID
	  ,StaffORgStartDate
	  ,StaffORgEndDate
      ,[TeamType]
      ,[Parent Org]
	  ,Convert(date,[FinalLoginTime])
	  ,DATEPART(HOUR, [FinalLoginTime])
      ,[DerivedDevicetype]
	  ,PostCode
      ,[OrganisationCCG]
      ,[OrgSTP]
      ,[NHS Region]
	  ,Domain
	  ,App
	  ,ActionType

-----------------------------------------------------------------------------------------------------------------------------------------------------------------
	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[IndOrgEmails]') is not null
	drop table [ETL_Local_PROD].[dbo].[IndOrgEmails] 


	SELECT 
	distinct 
	StaffEnterpriseId,
	DeptEnterpriseId,
	em.Address as Email,
	-- Add email location MS 5.2.17
	lo.EnterpriseID as EmailLocation,
	-- Add isPrimary flag MS 19.3.17
	em.isPrimary

	into [ETL_Local_PROD].[dbo].[IndOrgEmails] 

	  FROM [ETL_PROD].[dbo].[CMC_Location] lo  with (nolock)
	  join [ETL_Local_PROD].[dbo].[AT_IndToOrg] io  with (nolock) on lo.OrganizationEID = io.deptenterpriseid and lo.IndividualEID = io.StaffEnterpriseID
	-- change CMC_Location_Contacts/CMC_ContactInfo_Emails to CMC_Location_Emails  
	  join ETL_PROD.dbo.CMC_Location_Emails cp  with (nolock) on cp.Location = Lo.ItemId
	  join ETL_PROD.dbo.CMC_Email em  with (nolock) on cp.Email = em.ItemId
	-- Active [ETL_Local_PROD].[dbo].[AT_IndToOrg]s only MS 21.2.17
	  where (StartDate is null or CAST(startdate as date) <= CAST(getdate() as date))
	  and (endDate is null or CAST(enddate as date) > CAST(getdate() as date))





	IF OBJECT_ID('tempdb..#StaffLoginIds') IS NOT NULL 
	dROP TABLE #StaffLoginIds
	select 
	StaffUserId, 
	LoginId, 
	StaffRegistryId as LoginIdStaffRegistryId

	into #StaffLoginIds

	from AT_Staff s  with (nolock)
	join ETL_PROD.dbo.CMC_UserLoginId lc on s.StaffUserClinician = lc.UserClinician and lc.DomainCode = '%HS_CC'
	where staffuserid is not null


	--create view [dbo].[StaffSmartcards] as

	 	IF OBJECT_ID('tempdb..#StaffSmartcards') IS NOT NULL 
		dROP TABLE #StaffSmartcards
		select StaffUserId, 
		LoginId as Smartcard, 
		StaffRegistryId as SmartcardStaffRegistryId

		into #StaffSmartcards

		from AT_Staff s  with (nolock)
		join ETL_PROD.dbo.CMC_UserLoginId lc on s.StaffUserClinician = lc.UserClinician and lc.DomainCode = '%HS_SMARTCARD'
		where staffuserid is not null


		--[StaffEMIS] as upt ohere...
-- Add organisation MS 14.3.17


	 	IF OBJECT_ID('tempdb..#StaffEMIS') IS NOT NULL 
		dROP TABLE #StaffEMIS
		select StaffUserId, LoginId as EMIS, StaffRegistryId as EMISStaffRegistryId, DeptPDRegistryId as EMISDeptPDRegistryId
		
		into #StaffEMIS

		from AT_Staff s  with (nolock)
		join ETL_PROD.dbo.CMC_UserLoginId lc on s.StaffUserClinician = lc.UserClinician
		and lc.DomainCode = '%HS_EMIS'
		join AT_PD_Dept d on case when charindex('^',loginid)=0 then NULL else LEFT(loginid,charindex('^',loginid)-1) end = d.DeptPDRegistryID
		where staffuserid is not null
		



IF OBJECT_ID('[ETL_Local_PROD].[dbo].[Logins]') IS NOT NULL 
	 dROP TABLE [ETL_Local_PROD].[dbo].[Logins]
	select
	s.StaffUserId,
	s.StaffEnterpriseId,
	s.StaffRegistryId,
	ISNULL(s.StaffForename + ' ','') + ISNULL(s.StaffSurname,'') as StaffName,
	d.DeptName,
	d.DeptODSCode,
	d.DeptEnterpriseId,
	d.DeptPDRegistryID,
	io.StartDate as Created,
	io.EndDate as Removed,
	isnull(io.CMCRoleDescription,'isUnknown') as CMCRoleDescription,
	em.Email as StaffEmail,
	s.StaffProviderTypeDescription as StaffJobTitle,
	s.StaffActiveDescription,
	lo.LoginId,
	sc.Smartcard,
	-- Add EMIS login info MS 16.3.16
	es.EMIS,
	-- Handle expiration date on CMC_UserLoginID ?
	-- Add old system staff id MS 25.3.16
	s.StaffLocalCMCId,
	-- Add indtoorg itemid MS 22.1.17
	indtoorg,
	case when au.declinedlastprompt = 0 then au.ConfirmationDate else null end as AUPConfirmationDate
 
	into [ETL_Local_PROD].[dbo].[Logins]

	from AT_Staff s  with (nolock)
	join [ETL_Local_PROD].[dbo].[AT_IndToOrg] io on s.StaffEnterpriseId = io.StaffEnterpriseId
	join AT_PD_Dept d on io.DeptEnterpriseId = d.DeptEnterpriseId
	-- Tidy up de-duping MS 18.2.17
	left join (select StaffUserId, LoginId,
			ROW_NUMBER() over (partition by StaffUserId order by LoginId) as rn
			from #StaffLoginIds) lo on s.StaffUserId = lo.StaffUserId and lo.rn=1
	left join (select StaffUserId, Smartcard,
			ROW_NUMBER() over (partition by StaffUserId order by Smartcard) as rn
			from #StaffSmartcards) sc on s.StaffUserId = sc.StaffUserId and sc.rn=1
	left join
		   (select StaffUserId, EMIS,
			ROW_NUMBER() over (partition by StaffUserId order by EMIS) as rn
			from #StaffEMIS) es on s.StaffUserId = es.StaffUserId and es.rn=1
	-- MS 4.11.16 IndOrg emails
	left join (select StaffEnterpriseId, DeptEnterpriseId, Email,
			ROW_NUMBER() over (partition by StaffEnterpriseId,deptenterpriseid order by StaffEnterpriseId,deptenterpriseid) as rn
			from IndOrgEmails) em on s.StaffEnterpriseId = em.StaffEnterpriseId and d.deptenterpriseid = em.deptenterpriseid and em.rn=1
	-- MS 22.1.17 add AUP confirmation information
	left join ETL_PROD.dbo.CMC_UserTermsOfUseAcceptance au  with (nolock) on au.UserRegID = s.StaffRegistryId and au.UserLogonOrg = d.DeptPDRegistryID
	where (LoginId is not null or Smartcard is not null)


		end