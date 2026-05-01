USE [ETL_Local_PROD]
 




 



 	IF OBJECT_ID('tempdb..#StaffLoginIds') IS NOT NULL 
	dROP TABLE #StaffLoginIds
	select 
	StaffUserId, 
	LoginId, 
	StaffRegistryId as LoginIdStaffRegistryId

	into #StaffLoginIds

	from AT_Staff s
	join ETL_PROD.dbo.CMC_UserLoginId lc on s.StaffUserClinician = lc.UserClinician and lc.DomainCode = '%HS_CC'
	where staffuserid is not null


	--create view [dbo].[StaffSmartcards] as

	 	IF OBJECT_ID('tempdb..#StaffSmartcards') IS NOT NULL 
		dROP TABLE #StaffSmartcards
		select StaffUserId, 
		LoginId as Smartcard, 
		StaffRegistryId as SmartcardStaffRegistryId

		into #StaffSmartcards

		from AT_Staff s
		join ETL_PROD.dbo.CMC_UserLoginId lc on s.StaffUserClinician = lc.UserClinician and lc.DomainCode = '%HS_SMARTCARD'
		where staffuserid is not null


		--[StaffEMIS] as
-- Add organisation MS 14.3.17


	 	IF OBJECT_ID('tempdb..#StaffEMIS') IS NOT NULL 
		dROP TABLE #StaffEMIS
		select StaffUserId, LoginId as EMIS, StaffRegistryId as EMISStaffRegistryId, DeptPDRegistryId as EMISDeptPDRegistryId
		
		into #StaffEMIS

		from AT_Staff s
		join ETL_PROD.dbo.CMC_UserLoginId lc on s.StaffUserClinician = lc.UserClinician
		and lc.DomainCode = '%HS_EMIS'
		join AT_PD_Dept d on case when charindex('^',loginid)=0 then NULL else LEFT(loginid,charindex('^',loginid)-1) end = d.DeptPDRegistryID
		where staffuserid is not null


 	IF OBJECT_ID('tempdb..#StaffDeptAll') IS NOT NULL 
	dROP TABLE #StaffDeptAll
	select
	s.StaffUserId,
	isnull(StaffTitleDescription+' ','') + ISNULL(StaffForename+' ','') + ISNULL(StaffSurname,'') as StaffName,
	DeptName,
	s.StaffEnterpriseID,
	d.DeptEnterpriseID,
	io.StartDate,
	io.EndDate,
	-- created date
	-- staff email for this department
	StaffProviderTypeDescription as StaffJobTitle,
	-- Active status
	LoginId,
	Smartcard,
	DeptODSCode,
	-- MS 18.2.17 add indtoorg itemid
	IndToOrg,
	-- MS 16.3.17 add staff and dept registry ids
	DeptPDRegistryID as IODeptPDRegistryId,
	StaffRegistryId as IOStaffRegistryId

	into #StaffDeptAll

	from Staff s
	join AT_IndToOrg io on s.StaffEnterpriseId = io.StaffEnterpriseId
	join AT_PD_Dept d on io.DeptEnterpriseId = d.DeptEnterpriseID
	-- Tidy up de-dup MS 18.2.17
	left join (select *,ROW_NUMBER() over (partition by staffuserid order by loginid) as rn from #StaffLoginIds) sl on sl.StaffUserId = s.staffuserid and sl.rn=1
	left join (select *,ROW_NUMBER() over (partition by staffuserid order by smartcard) as rn from #StaffSmartcards) ss on ss.StaffUserId = s.staffuserid and ss.rn=1



	--select * from #StaffDeptAll


	 	IF OBJECT_ID('tempdb..#DomainLogins') IS NOT NULL 
		dROP TABLE #DomainLogins
	--ALTER view [dbo].[Cache-DomainLogins] as
		;with singles (StaffuserID,DeptPDRegistryId) as
						(
							select 
							io.StaffuserID,
							iodeptpdregistryid as DeptPDRegistryId 
							from
							(
								select 
								* 
								from (
										select 
										staffenterpriseid,
										staffuserid,COUNT(*) as num 
										from #StaffDeptAll 
										group by 
										staffenterpriseid,
										staffuserid
										) sel1 
								where num=1
							) sel2
						join #StaffDeptAll io on sel2.StaffEnterpriseID=io.StaffEnterpriseID
						)

		select 
		Source,
		StaffUserId,
		LoginId,
		LoginStaffRegistryId,
		LoginDeptPDRegistryId 

		into #DomainLogins

		from
			(
			select 
			'%HS_EMIS' as Source,
			se.StaffUserId,
			emis as LoginId,
			EMISStaffRegistryId as LoginStaffRegistryId,
			EMISDeptPDRegistryId as LoginDeptPDRegistryId, 
			ROW_NUMBER() over (partition by emis order by emis) as lrn
			from #StaffEMIS se

			union all

			select 
			'%HS_Smartcard' as Source,
			se.StaffUserId,
			smartcard as LoginId,
			SmartcardStaffRegistryId as LoginStaffRegistryId,
			s.DeptPDRegistryID as LoginDeptPDRegistryId,
			ROW_NUMBER() over (partition by smartcard order by smartcard) as lrn
			from #StaffSmartcards se 
			left join singles s on se.staffuserid=s.staffuserid

			union all

			select 
			'%HS_CC' as 
			Source,
			se.StaffUserId,
			LoginId,
			LoginIdStaffRegistryId,
			s.DeptPDRegistryID as LoginDeptPDRegistryId,
			ROW_NUMBER() over (partition by loginid order by loginid) as lrn
			from #StaffLoginIds se left join singles s on se.staffuserid=s.staffuserid

			union all

			select 
			distinct 
			'%HS_CC' as Source, 
			StaffUserId, 
			StaffUserId, 
			IOStaffRegistryId as LoginStaffRegistryId,
			IODeptPDRegistryId as LoginDeptPDRegistryId, 
			1 as lrn 
			from #StaffDeptAll 
			where StaffUserId like 'System%'

			) sel1
			where lrn=1 




			--select * from #DomainLogins



-- Cross-reference Activity Log to make sure each of its entries points to the right AuditLogin record. Omit anything with a proper login reference.
	 	IF OBJECT_ID('tempdb..#AuditLoginDeDup') IS NOT NULL 
		dROP TABLE #AuditLoginDeDup

		;with ActivityLog as 
							(
							select 
							* 
							from etl_prod.dbo.CMC_ActivityLog 
							where LoginRowId is null
							),
		Matches as
				(
				select 
				a.ItemId as ActivityLogItemId, 
				l.ItemId as AuditLoginItemId, 
				UserID, 
				UserLoginId, 
				LastLogin, 
				logintime, 
				sessionid,
				ROW_NUMBER() over (partition by a.itemid order by abs(datediff(second,lastlogin,logintime))) as rn
				from ActivityLog a
				join #DomainLogins d on a.UserRegistryID = d.LoginStaffRegistryId
				join etl_prod.dbo.CMC_AuditLogin l 
				on d.LoginId = l.UserLoginId and a.SessionID = l.cspsessionid
				where LastLogin <= logintime
				)
		select ActivityLogItemId,AuditLoginItemId into #AuditLoginDeDup from Matches where rn=1


	 	
		-- MS 13.3.17 ensure sessions with multi different logins are handled correctly
		
		IF OBJECT_ID('tempdb..#AuditAuthenticationRaw1') IS NOT NULL 
		dROP TABLE #AuditAuthenticationRaw1
 

		;with 
		activitylog as (
						select
						a.SessionID as ActivityLogSessionId,
						a.Browser,
						a.BrowserMode,
						a.BrowserVersion,
						a.UserAgent,
						-- add isMobile flag MS 21.1.17
						isMobile,
						a.LoginTime as ActivityLoginTime,
						a.LogoutTime as ActivityLogoutTime,
						a.UserRegistryId as ActivityLogUserRegistryId,
						a.OrganizationRegistryID as ActivityLogOrganizationRegistryId,
						LoginRowId,
						ItemId as ActivityLogItemId
						from ETL_PROD.dbo.CMC_ActivityLog a
						),
						matched as
								(
								select la.* 
								from etl_prod.dbo.cmc_auditlogin la
								join activitylog al on al.loginrowid is not null and al.loginrowid = la.itemid
								)

		select 
		la.*,
		al.* 

		into #AuditAuthenticationRaw1

		from matched la
		join activitylog al on al.loginrowid is not null and al.loginrowid = la.itemid

		union all

		select 
		la.*,
		al.* 
		from (
				select 
				* 
				from etl_prod.dbo.cmc_auditlogin except select * from matched
			) la
		left join AuditLoginDeDup ld on la.ItemId = ld.AuditLoginItemId
		left join activitylog al on al.ActivityLogItemId = ld.ActivityLogItemId


 
 	IF OBJECT_ID('tempdb..#AARaw2Staff') IS NOT NULL 
		dROP TABLE #AARaw2Staff
 --ALTER view [dbo].[Cache-AARaw2Staff] as

		select 
		r.*,
		StaffUserId as ActivityLogUserId 

		into #AARaw2Staff

		from #AuditAuthenticationRaw1 r 
		left join AT_Staff s on r.ActivityLogUserRegistryId=s.StaffRegistryId


     
 	IF OBJECT_ID('tempdb..#AARaw2Domain') IS NOT NULL 
		dROP TABLE #AARaw2Domain
		--ALTER view [dbo].[Cache-AARaw2Domain] as

		select 
		r1.*,
		l.StaffUserId as LoginStaffUserId, 
		l.LoginStaffRegistryId,
		l.LoginDeptPDRegistryId 

		into #AARaw2Domain

		from [#AARaw2Staff] r1 
		left join #DomainLogins l
		on r1.UserLoginId = l.loginid
		and r1.Domain = l.Source
		where r1.Domain <> '%HS_CC'
		and (r1.Domain <> '%HS_Smartcard' or r1.UserLoginId <> 'smartcard')


				 	IF OBJECT_ID('tempdb..#AARaw2DomainTwo') IS NOT NULL 
					dROP TABLE #AARaw2DomainTwo
		--AARaw2DomainTwo] as

					select 
					r1.*,
					l.StaffUserId as LoginStaffUserId, 
					l.LoginStaffRegistryId,
					l.LoginDeptPDRegistryId 

					into #AARaw2DomainTwo

					from #AARaw2Staff r1 
					left join #DomainLogins l
					on r1.UserLoginId = l.staffuserid
					and r1.Domain = l.Source
					where r1.Domain = '%HS_CC'



--Cache-AARaw2DomainThree] as


				 	IF OBJECT_ID('tempdb..#AARaw2DomainThree') IS NOT NULL 
					dROP TABLE #AARaw2DomainThree

					select 
					r1.*,
					l.StaffUserId as LoginStaffUserId,  
					l.LoginStaffRegistryId,
					l.LoginDeptPDRegistryId 

					into #AARaw2DomainThree

					from #AARaw2Staff r1 
					left join #DomainLogins l
					on r1.ActivityLogUserId = l.staffuserid
					and r1.Domain = l.Source
					where r1.Domain = '%HS_Smartcard' and r1.UserLoginId = 'smartcard'







		 	IF OBJECT_ID('tempdb..#AARaw2LoginAdded') IS NOT NULL 
			dROP TABLE #AARaw2LoginAdded
		--ALTER view [dbo].[Cache-AARaw2LoginAdded] as

			select 
			* 
			into #AARaw2LoginAdded

			from #AARaw2Domain 
			
			union all 
			
			select 
			* 
			from #AARaw2DomainTwo 
			
			union all 
			
			select * from #AARaw2DomainThree


			--select * from #AARaw2LoginAdded
------------------------------------------------------------------------------------------------------------------------------
		 	IF OBJECT_ID('tempdb..#StaffUserId') IS NOT NULL 
			dROP TABLE #StaffUserId
			select 
			distinct 
			StaffUserId 
			into #StaffUserId 
			from AT_Staff	



	 if OBJECT_ID ('Tempdb..#AuditLogon') is not null
	drop table #AuditLogon
	select
	ItemId as [Audit] 
	
      ,[ActionTime]
      ,[ActionType]
      ,cast(Actor as varchar(255)) as UserId
   
	-- Add CMC_AuditLogin linkage info MS 4.7.16
	,LoginRowId as LoginReference
	into #AuditLogon
 
	from ETL_PROD.dbo.CMC_AuditData a 
	inner join #StaffUserId b on b.StaffUserId = a.Actor
	Where 
	 not exists (select [Audit] from [ETL_Local_PROD].[dbo].[AuditLogin_New]  z where a.ItemId = z.[Audit])
	 and 
	 a.ActionType  in ('Login','Logout')

 
 
 select * into [ETL_Local_PROD].[dbo].[AuditLogin_New]  from #AuditLogon
 
   

	insert into [ETL_Local_PROD].[dbo].[AuditLogin_New] 

	SELECT
	*
 
  FROM   #AuditLogon x

 

 drop table #AuditLogon

 

	 




			IF OBJECT_ID('tempdb..#AARaw2LoginAudit') IS NOT NULL 
			DROP TABLE #AARaw2LoginAudit

			select 
			l.ItemId as LoginReference,
			ActivityLogUserRegistryId, 
			ActivityLogOrganizationRegistryId,
			Audit, 
			ActionTime, 
			ActionType

			into #AARaw2LoginAudit

			from [ETL_Local_PROD].[dbo].[AuditLogin_New]  a
			join #AARaw2LoginAdded l on a.UserId = l.ActivityLogUserId 
			and a.ActionTime = l.ActivityLoginTime
			where ActionType='Login'






			IF OBJECT_ID('tempdb..#AARaw2AuditPatientLoginReference') IS NOT NULL 
			dROP TABLE #AARaw2AuditPatientLoginReference
			--ALTER view [dbo].[Cache-AARaw2AuditPatientLoginReference] as
			select 
			a.LoginReference,
			a.StaffRegistryId,a.DeptPDRegistryId, 
			MIN(PatAuditID) as Audit,
			MIN(ActionTime) as ActionTime, 
			'Login' as ActionType 

			into #AARaw2AuditPatientLoginReference

			from AuditPatient_New a
			where LoginReference is not null
			group by a.LoginReference,
			a.StaffRegistryId,
			a.DeptPDRegistryId


			--select top 10 * from  AuditPatient_New 


				IF OBJECT_ID('tempdb..#AuditAuthenticationRaw2') IS NOT NULL 
				dROP TABLE #AuditAuthenticationRaw2

			--ALTER view [dbo].[Cache-AuditAuthenticationRaw2] as
				select 
				distinct * 
				
				into #AuditAuthenticationRaw2
				
				from #AARaw2LoginAdded
				join #AARaw2AuditPatientLoginReference au
				on au.LoginReference = itemid

				union all

				select 
				distinct l.*,
				a.* 
				from #AARaw2LoginAdded l
				left join #AARaw2AuditPatientLoginReference au on au.LoginReference = itemid
				left join #AARaw2LoginAudit a on a.loginreference=l.itemid
				where au.Audit is null 




				--[Cache-AuditLoginSequence] as
--select *,case when loginstaffuserid is null then 1 else ROW_NUMBER() over (partition by loginstaffuserid,logindate order by logintime) end as rn from [AuditAuthenticationRaw]
	IF OBJECT_ID('tempdb..#AuditAuthenticationRaw') IS NOT NULL 
	dROP TABLE #AuditAuthenticationRaw

--ALTER view [dbo].[Cache-AuditAuthenticationRaw] as
	;with cleanup as
	(select
	isnull(LoginStaffUserId,ISNULL(ActivityLogUserId,StaffUserId)) as LoginStaffUserId,
	isnull(LoginStaffRegistryId,ISNULL(ActivityLogUserRegistryID,a.StaffRegistryId)) as LoginStaffRegistryId,
	isnull(LoginDeptPDRegistryId,ISNULL(ActivityLogOrganizationRegistryID,DeptPDRegistryId)) as LoginDeptPDRegistryId,
	UserAgent,
	Browser,
	BrowserMode,
	BrowserVersion,
	isMobile,
	isnull(CspSessionId,ActivityLogSessionId) as CSPSessionId,
	Domain,
	UserLoginId as LoginId,
	Success,
	Reason,
	LastLogin as AuthenticationTime,
	cast(lastlogin as date) as AuthenticationDate,
	ActivityLoginTime as LoginTime,
	cast(ActivityLoginTime AS date) as LoginDate,
	ActivityLogoutTime as LogoutTime,
	Audit as AuditItemId,
	ActivityLogItemId,
	ItemId as LoginItemId
	from 
	AuditAuthenticationRaw2 a 
	left join Staff s on a.staffregistryid = s.StaffRegistryId)

		select
		cast(LoginStaffUserId as varchar(255)) as LoginStaffUserId,
		loginstaffregistryid,
		logindeptpdregistryid,
		UserAgent,
		Browser,
		BrowserMode,
		BrowserVersion,
		isMobile,
		CspSessionId,
		Domain,
		LoginId,
		Success,
		Reason,
		MIN(authenticationtime) as authenticationTime,
		MIN(authenticationdate) as authenticationdate,
		Min(LoginTime) as LoginTime,
		MIN(LoginDate) as LoginDate,
		MAX(LogoutTime) as LogoutTime,
		min(AuditItemId) as AuditItemId, 
		MIN(ActivityLogItemId) as ActivityLogItemId, 
		MIN(LoginItemId) as LoginItemId

		into #AuditAuthenticationRaw

		from cleanup
		group by
		LoginStaffUserId,
		loginstaffregistryid,
		logindeptpdregistryid,
		UserAgent,
		Browser,
		BrowserMode,
		BrowserVersion,
		isMobile,
		CspSessionId,
		Domain,
		LoginId,
		Success,
		Reason


			IF OBJECT_ID('tempdb..#AuditLoginSequence') IS NOT NULL 
			dROP TABLE #AuditLoginSequence
 
			select 
			*,
			case when loginstaffuserid is null then 1 else ROW_NUMBER() over (partition by loginstaffuserid,logindate order by logintime) end as rn 

			into #AuditLoginSequence

			from [#AuditAuthenticationRaw]



			IF OBJECT_ID('tempdb..#AARaw2LoginAdded') IS NOT NULL 
			dROP TABLE #AARaw2LoginAdded

			select 
			* 

			into #AARaw2LoginAdded

			from #AARaw2Domain 
			
			union all 
			
			select 
			* 
			from #AARaw2DomainTwo 
			
			union all 
			
			select 
			* 
			from #AARaw2DomainThree






--			ALTER view [dbo].[UserAgents] as
--select distinct useragent from
--(select distinct useragent from AuditAuthentication
--union all
--select distinct BrowserUserAgent from PortalDataPivot
--where BrowserUserAgent<>'''Mozilla\/5.0 (compatible; DuckDuckBot-Https\/1.1; https:\/\/duckduckgo.com\/duckduckbot)''') s1
--where useragent is not null

 

			IF OBJECT_ID('[ETL_Local_PROD].[dbo].[AT_AuditAuthentication]') IS NOT NULL 
			dROP TABLE [ETL_Local_PROD].[dbo].[AT_AuditAuthentication]

--ALTER view [dbo].[Cache-AuditAuthentication] as

			select 
			loginstaffUserId as UserId,
			LoginstaffRegistryId as UserRegistryId,
			LoginDeptPDRegistryID as OrganizationRegistryId,
			UserAgent,
			Browser,
			BrowserMode,
			BrowserVersion,
			isMobile,
			CspSessionId,
			Domain,
			LoginId,
			LoginTime,
			isnull(case
			  when loginstaffUserId is null then LogoutTime
			  when LogoutTime is null then
				(select top 1 next.logouttime
				  from #auditloginsequence next
				  where next.loginstaffuserid=ls.loginstaffuserid
					and next.logindate=ls.logindate
					and next.rn>ls.rn
					and next.LogoutTime is not null
					order by next.rn)
			  else LogoutTime end,
			  dateadd(second,-1,dateadd(day,1,cast(logindate as datetime)))) as LogoutTime,
			AuditItemId as LoginAuditId, 
			LoginItemId, 
			Success, 
			Reason, 
			AuthenticationTime

			into [ETL_Local_PROD].[dbo].[AT_AuditAuthentication]

			from #auditloginsequence ls



			--select * from [ETL_Local_PROD].[dbo].[AT_AuditAuthentication]


			--ALTER view [dbo].[Cache-AvailabilityLog] as
select ItemId as id,
cast(actiontime as datetime) as EventDateTime,
cast(actiontime as date) as EventDate,
case when MRNs='0' then 'N' else 'Y' end as match,
'EMIS' as Service, DeptEnterpriseID, Team
from ServiceSearch a
-- logic here heeds to take close dates into account and also to follow Gareth's instructions re filtering
join (select deptodscode,deptenterpriseid,ROW_NUMBER() over (PARTITION by deptodscode order by deptodscode) as rn from PDDepartment) d on a.ODS = DeptODSCode and d.rn=1
-- enforce uniqueness MS 29.5.16
join (select *,
row_number() over (partition by activitydepartmentid order by team) rn 
from Reporting.DisambiguatedActivityTeams) at on at.ActivityDepartmentID = d.DeptEnterpriseID and at.rn=1
GO




SELECT Count(*)
  FROM [ETL_Local_PROD].[dbo].[AvailabilityLog]  --340978179 rows