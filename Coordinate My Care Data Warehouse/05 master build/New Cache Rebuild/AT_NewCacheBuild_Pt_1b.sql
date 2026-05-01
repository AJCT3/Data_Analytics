USE [ETL_Local_PROD]
GO
/****** Object:  StoredProcedure [dbo].[NewCacheUpdated]    Script Date: 02/03/2020 08:32:57 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/** =============================================
 What a f**king mess! And you can quote me on that.. A. Turner March 2020
---- =============================================**/

alter PROCEDURE [dbo].[AT_NewCacheUpdated_Pt_1b]

AS

BEGIN

		delete from AllDataAnswers
		exec dbo.DePivotPatientDetail


		
	 	IF OBJECT_ID('tempdb..#StaffUserId') IS NOT NULL 
			dROP TABLE #StaffUserId
			select 
			distinct 
			StaffUserId 
			into #StaffUserId 
			from [ETL_Local_PROD].[dbo].[AT_Staff]	 with (nolock)


		insert into [ETL_Local_PROD].[dbo].[AuditLogon]
		select
			ItemId as [Audit] 
	
			  ,[ActionTime]
			  ,[ActionType]
			  ,cast(Actor as varchar(255)) as UserId
   
			-- Add CMC_AuditLogin linkage info MS 4.7.16
			,LoginRowId as LoginReference

			from ETL_PROD.dbo.CMC_AuditData a  with (nolock)
			where  a.ActionType  in ('Login','Logout')
			and not exists (select [Audit] from [ETL_Local_PROD].[dbo].[AuditLogon]  z where a.ItemId = z.[Audit])
			and actionTIme >= dateadd(week, -3,getdate())




			

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

	from AT_Staff s  with (nolock)
	join AT_IndToOrg io  with (nolock) on s.StaffEnterpriseId = io.StaffEnterpriseId
	join AT_PD_Dept d  with (nolock) on io.DeptEnterpriseId = d.DeptEnterpriseID
	-- Tidy up de-dup MS 18.2.17
	left join (select *,ROW_NUMBER() over (partition by staffuserid order by loginid) as rn from #StaffLoginIds) sl on sl.StaffUserId = s.staffuserid and sl.rn=1
	left join (select *,ROW_NUMBER() over (partition by staffuserid order by smartcard) as rn from #StaffSmartcards) ss on ss.StaffUserId = s.staffuserid and ss.rn=1



	--select * from #StaffDeptAll


	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[DomainLogins]') is not null
		drop table [ETL_Local_PROD].[dbo].[DomainLogins] 
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

		into [ETL_Local_PROD].[dbo].[DomainLogins] 

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




	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AuditLoginDeDup]') is not null
		drop table [ETL_Local_PROD].[dbo].[AuditLoginDeDup] 

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
				join [ETL_Local_PROD].[dbo].[DomainLogins] d  with (nolock) on a.UserRegistryID = d.LoginStaffRegistryId
				join etl_prod.dbo.CMC_AuditLogin l  with (nolock)
				on d.LoginId = l.UserLoginId and a.SessionID = l.cspsessionid
				where LastLogin <= logintime
				)
		select ActivityLogItemId,AuditLoginItemId into [ETL_Local_PROD].[dbo].[AuditLoginDeDup]  from Matches where rn=1



		 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AuditAuthenticationRaw1]') is not null
		drop table [ETL_Local_PROD].[dbo].[AuditAuthenticationRaw1] 
 

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
						cast(a.UserRegistryId as varchar(100))  as ActivityLogUserRegistryId,
						cast(a.OrganizationRegistryID as varchar(100)) as ActivityLogOrganizationRegistryId,
						LoginRowId,
						ItemId as ActivityLogItemId
						from ETL_PROD.dbo.CMC_ActivityLog a  with (nolock)
						),
						matched as
								(
								select la.* 
								from etl_prod.dbo.cmc_auditlogin la  with (nolock)
								join activitylog al on al.loginrowid is not null and al.loginrowid = la.itemid
								)

		select 
		la.*,
		al.* 

		into [ETL_Local_PROD].[dbo].[AuditAuthenticationRaw1] 

		from matched la
		join activitylog al  with (nolock) on al.loginrowid is not null and al.loginrowid = la.itemid

		union all

		select 
		la.*,
		al.* 
		from (
				select 
				* 
				from etl_prod.dbo.cmc_auditlogin except select * from matched
			) la
		left join AuditLoginDeDup ld with (nolock)on la.ItemId = ld.AuditLoginItemId
		left join activitylog al with (nolock) on al.ActivityLogItemId = ld.ActivityLogItemId





		 
CREATE NONCLUSTERED INDEX [ActivityLogUserRegistryId] ON [dbo].[AuditAuthenticationRaw1] 
(
	[ActivityLogUserRegistryId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]


 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AARaw2Staff]') is not null
		drop table [ETL_Local_PROD].[dbo].[AARaw2Staff] 
 --ALTER view [dbo].[Cache-AARaw2Staff] as

		select 
		[ItemId]
      ,cast([Domain] as [nvarchar](50)) as Domain
      ,cast([UserLoginId] as  [nvarchar](50)) as [UserLoginId]
      ,[SystemId]
      ,[ExternalSessionId]
      ,[CspSessionId]
      ,[LastLogin]
      ,[Success]
      ,[Reason]
      ,[SoftFailure]
      ,[ActivityLogSessionId]
      ,[Browser]
      ,[BrowserMode]
      ,[BrowserVersion]
      ,[UserAgent]
      ,[isMobile]
      ,[ActivityLoginTime]
      ,[ActivityLogoutTime]
      ,[ActivityLogUserRegistryId]
      ,[ActivityLogOrganizationRegistryId]
      ,[LoginRowId]
      ,[ActivityLogItemId]
	  ,cast(StaffUserId as [nvarchar](50)) as ActivityLogUserId 

		into [ETL_Local_PROD].[dbo].[AARaw2Staff] 

		from  [ETL_Local_PROD].[dbo].[AuditAuthenticationRaw1]  r  with (nolock)
		left join AT_Staff s with (nolock) on r.ActivityLogUserRegistryId=s.StaffRegistryId


  
	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AARaw2Domain]') is not null
		drop table [ETL_Local_PROD].[dbo].[AARaw2Domain] 
		--ALTER view [dbo].[Cache-AARaw2Domain] as

		select 
		r1.*,
		l.StaffUserId as LoginStaffUserId, 
		l.LoginStaffRegistryId,
		l.LoginDeptPDRegistryId 

		into [ETL_Local_PROD].[dbo].[AARaw2Domain] 

		from  [ETL_Local_PROD].[dbo].[AARaw2Staff]  r1  with (nolock)
		left join DomainLogins l with (nolock)
		on r1.UserLoginId = l.loginid
		and r1.Domain = l.Source
		where r1.Domain <> '%HS_CC'
		and (r1.Domain <> '%HS_Smartcard' or r1.UserLoginId <> 'smartcard')
 
		CREATE NONCLUSTERED INDEX [UserLoginId] ON [dbo].[AARaw2Domain] 
		(
			[UserLoginId] ASC
		)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

		CREATE NONCLUSTERED INDEX [Domain] ON [dbo].[AARaw2Domain] 
		(
			[Domain] ASC
		)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

		CREATE NONCLUSTERED INDEX [ActivityLogUserId] ON [dbo].[AARaw2Domain] 
		(
			[ActivityLogUserId] ASC
		)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]


		
	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AARaw2DomainTwo]') is not null
		drop table [ETL_Local_PROD].[dbo].[AARaw2DomainTwo] 
		--AARaw2DomainTwo] as

					select 
					r1.*,
					l.StaffUserId as LoginStaffUserId, 
					l.LoginStaffRegistryId,
					l.LoginDeptPDRegistryId 

					into [ETL_Local_PROD].[dbo].[AARaw2DomainTwo] 

					from AARaw2Staff r1 
					left join DomainLogins l
					on r1.UserLoginId = l.staffuserid
					and r1.Domain = l.Source
					where r1.Domain = '%HS_CC'



CREATE NONCLUSTERED INDEX [ActivityLogUserId] ON [dbo].[AARaw2DomainTwo] 
(
	[ActivityLogUserId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX [Domain] ON [dbo].[AARaw2DomainTwo] 
(
	[Domain] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX [UserLoginId] ON [dbo].[AARaw2DomainTwo] 
(
	[UserLoginId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]


--------------------------------------------------
-- [dbo].[AARaw2DomainThree]
--------------------------------------------------

	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AARaw2DomainThree]') is not null
		drop table [ETL_Local_PROD].[dbo].[AARaw2DomainThree] 

					select 
					r1.*,
					l.StaffUserId as LoginStaffUserId,  
					l.LoginStaffRegistryId,
					l.LoginDeptPDRegistryId 

					into [ETL_Local_PROD].[dbo].[AARaw2DomainThree]

					from AARaw2Staff r1 
					left join DomainLogins l
					on r1.ActivityLogUserId = l.staffuserid
					and r1.Domain = l.Source
					where r1.Domain = '%HS_Smartcard' and r1.UserLoginId = 'smartcard'


--drop table [dbo].[AARaw2DomainThree]
 
CREATE NONCLUSTERED INDEX [ActivityLogUserId] ON [dbo].[AARaw2DomainThree] 
(
	[ActivityLogUserId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX [Domain] ON [dbo].[AARaw2DomainThree] 
(
	[Domain] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX [UserLoginId] ON [dbo].[AARaw2DomainThree] 
(
	[UserLoginId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

--------------------------------------------------
-- [dbo].[AARaw2LoginAdded]
--------------------------------------------------


--drop table [dbo].[AARaw2LoginAdded]


	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AARaw2LoginAdded]') is not null
		drop table [ETL_Local_PROD].[dbo].[AARaw2LoginAdded] 
		--ALTER view [dbo].[Cache-AARaw2LoginAdded] as

			select 
			* 
			into [ETL_Local_PROD].[dbo].[AARaw2LoginAdded] 

			from AARaw2Domain 
			
			union all 
			
			select 
			* 
			from AARaw2DomainTwo 
			
			union all 
			
			select * from AARaw2DomainThree




CREATE NONCLUSTERED INDEX [ActivityLogUserId] ON [dbo].[AARaw2LoginAdded] 
(
	[ActivityLogUserId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX [Domain] ON [dbo].[AARaw2LoginAdded] 
(
	[Domain] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX [UserLoginId] ON [dbo].[AARaw2LoginAdded] 
(
	[UserLoginId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX [ActivityLoginTime] ON [dbo].[AARaw2LoginAdded] 
(
	[ActivityLoginTime] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]


--------------------------------------------------
-- dbo.[AARaw2LoginAudit]
--------------------------------------------------

--drop table dbo.[AARaw2LoginAudit]
	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AARaw2LoginAudit]') is not null
		drop table [ETL_Local_PROD].[dbo].[AARaw2LoginAudit] 

			select 
			l.ItemId as LoginReference,
			ActivityLogUserRegistryId, 
			ActivityLogOrganizationRegistryId,
			Audit, 
			ActionTime, 
			ActionType

			into [ETL_Local_PROD].[dbo].[AARaw2LoginAudit] 

			from [ETL_Local_PROD].[dbo].[AuditLogon]  a with (nolock)
			join AARaw2LoginAdded l on a.UserId = l.ActivityLogUserId 
			and a.ActionTime = l.ActivityLoginTime
			where ActionType='Login'




CREATE NONCLUSTERED INDEX [LoginReference] ON [dbo].[AARaw2LoginAudit] 
(
	[LoginReference] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

--------------------------------------------------
--  dbo.[AARaw2AuditPatientLoginReference] 
--------------------------------------------------

--drop table dbo.[AARaw2AuditPatientLoginReference] 
	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AARaw2AuditPatientLoginReference]') is not null
		drop table [ETL_Local_PROD].[dbo].[AARaw2AuditPatientLoginReference] 
		select 
			a.LoginReference,
			a.StaffRegistryId,a.DeptPDRegistryId, 
			MIN(Audit) as Audit,
			MIN(ActionTime) as ActionTime, 
			'Login' as ActionType 

			into [ETL_Local_PROD].[dbo].[AARaw2AuditPatientLoginReference] 

			from AuditPatient a with (nolock)
			where LoginReference is not null
			group by a.LoginReference,
			a.StaffRegistryId,
			a.DeptPDRegistryId

CREATE NONCLUSTERED INDEX [LoginReference] ON [dbo].[AARaw2AuditPatientLoginReference] 
(
	[LoginReference] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

--------------------------------------------------
--  dbo.[AuditAuthenticationRaw2] 
--------------------------------------------------

--drop table AuditAuthenticationRaw2
	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AuditAuthenticationRaw2]') is not null
		drop table [ETL_Local_PROD].[dbo].[AuditAuthenticationRaw2] 

			--ALTER view [dbo].[Cache-AuditAuthenticationRaw2] as
				select 
				distinct * 
				
				into [ETL_Local_PROD].[dbo].[AuditAuthenticationRaw2]
				
				from AARaw2LoginAdded
				join AARaw2AuditPatientLoginReference au
				on au.LoginReference = itemid

				union all

				select 
				distinct l.*,
				a.* 
				from AARaw2LoginAdded l
				left join AARaw2AuditPatientLoginReference au  with (nolock) on au.LoginReference = itemid
				left join AARaw2LoginAudit a  with (nolock) on a.loginreference=l.itemid
				where au.Audit is null 

-----------------------------------------
------------- New code ended ------------
-----------------------------------------
PRINT 'End Section Six A'
PRINT GETDATE()

WAITFOR DELAY '00:05:00' -- WAIT SIX after new code added in January 2018



PRINT 'Start Section SEVEN'
PRINT GETDATE()

/*PRINT 'End Section Ten'
PRINT GETDATE()

WAITFOR DELAY '00:05:00' -- WAIT TEN after new code added in January 2018

PRINT 'Start Section Eleven'
PRINT GETDATE()
*/
--drop table AuditAuthenticationRaw


	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AuditAuthenticationRaw]') is not null
		drop table [ETL_Local_PROD].[dbo].[AuditAuthenticationRaw] 

 
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

		into [ETL_Local_PROD].[dbo].[AuditAuthenticationRaw] 

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


 

CREATE NONCLUSTERED INDEX [Sequencer] ON [dbo].[AuditAuthenticationRaw] 
(
	[LoginStaffUserId] ASC,
	[logindate] ASC,
	[LoginTime] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]



--drop table AuditLoginSequence
	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AuditLoginSequence]') is not null
		drop table [ETL_Local_PROD].[dbo].[AuditLoginSequence] 
 
			select 
			*,
			case when loginstaffuserid is null then 1 else ROW_NUMBER() over (partition by loginstaffuserid,logindate order by logintime) end as rn 

			into [ETL_Local_PROD].[dbo].[AuditLoginSequence] 

			from [AuditAuthenticationRaw]

------------------------------------------------------------------------------------------------------------------------
--Makeshit Team eaudit makeup for next query....


------------------------------------------------------------------------------------------------------------------------
--drop table AuditAuthentication
IF OBJECT_ID('[ETL_Local_PROD].[dbo].[AuditAuthentication]') IS NOT NULL 
			dROP TABLE [ETL_Local_PROD].[dbo].[AuditAuthentication]

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
				  from auditloginsequence next
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

			into [ETL_Local_PROD].[dbo].[AuditAuthentication]

			from auditloginsequence ls


  
		IF OBJECT_ID('[ETL_Local_PROD].[dbo].[AuditPatientAuthentication]') IS NOT NULL 
					dROP TABLE [ETL_Local_PROD].[dbo].[AuditPatientAuthentication]


		;with patientaudit as
		(
			select 
			* 
			from AuditPatient 
			where LoginReference is not null

			union all

			select 
			CMC_ID,
			FromPatientSummary,
			ToPatientSummary,
			Audit,
			ActionTime,
			ActionType,
			StaffRegistryId,
			DeptPDRegistryId,
			Role,
			aa.loginitemid as loginreference
			from 
			AuditAuthentication aa 
			join AuditPatient ap  with (nolock)
			on ap.StaffRegistryId=aa.UserRegistryId 
			and ap.DeptPDRegistryId=aa.OrganizationRegistryId 
			and ap.LoginReference is null 
			and ap.ActionTime between aa.LoginTime and aa.LogoutTime
		)

		select 
		UserId, 
		UserRegistryID, 
		OrganizationRegistryID, 
		LoginTime, 
		LogoutTime, 
		UserAgent, 
		Browser, 
		BrowserVersion, 
		isMobile, 
		BrowserMode,
		CspSessionId, 
		Domain, 
		LoginId, 
		cast(LoginTime as date) as LoginDate,
		isnull(Audit,'NoAudit'+LoginItemId) as Audit, 
		LoginItemId, 
		replace(ActionType,'custom-','') as ActionType 

		into [ETL_Local_PROD].[dbo].[AuditPatientAuthentication]

		from
			(
				select
				aa.[UserId],
				aa.[UserRegistryId],
				aa.[OrganizationRegistryId],
				aa.[UserAgent],
				aa.[Browser],
				aa.[BrowserMode],
				aa.[BrowserVersion],
				-- add isMobile flag MS 21.1.17
				isMobile,
				aa.[CspSessionId],
				aa.[Domain],
				aa.[LoginId],
				aa.AuthenticationTime as LoginTime,
				aa.[LogoutTime],
				ap.Audit,
				aa.LoginItemId,
				ActionType,
				ROW_NUMBER() over (partition by audit,domain order by logintime) as rn
				from AuditAuthentication aa 
				join patientaudit ap  with (nolock) on ap.LoginReference = aa.LoginItemId
		
				union all

				select
				a.[UserId],
				a.[UserRegistryId],
				case 
					when Success=1 or domain <> '%HS_EMIS' then a.[OrganizationRegistryId]
					else 
						case 
							when charindex('^',a.loginid)=0 then NULL 
								else LEFT(a.loginid,charindex('^',loginid)-1) 
						end
				  end as OrganizationRegistryId,
				a.[UserAgent],
				a.[Browser],
				a.[BrowserMode],
				a.[BrowserVersion],
				isMobile,
				a.[CspSessionId],
				a.[Domain],
				a.[LoginId],
				a.AuthenticationTime as LoginTime,
				a.[LogoutTime],
				a.[LoginAuditId],
				a.LoginItemId,
				case
				-- 16.3.17 MS success/failure now goes in here
				-- need to look up EMIS organisation on failures [also wherever there is a single ind/org / no activity log entry, EMIS / auto-flagging]
				  when Success = 0 then 'failed'
				-- this works for now, we'll have to think of something later .....
				  when domain = '%HS_EMIS' and (browser is null or loginstatus = 'direct') then 'direct'
				  when domain = '%HS_Smartcard' and userid is not null then 'direct'
				  else 'login' end as ActionType,1 as rn
				-- 30.6.16 MS new direct handling
				  from AuditAuthentication a  with (nolock)
				  join AuditIdentifyDirect d on a.CspSessionId = d.cspsessionid
				) sel1
				where rn=1


 

		exec dbo.PivotCareLandscape


 
 


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
			from StaffLoginIds) lo on s.StaffUserId = lo.StaffUserId and lo.rn=1
	left join (select StaffUserId, Smartcard,
			ROW_NUMBER() over (partition by StaffUserId order by Smartcard) as rn
			from StaffSmartcards) sc on s.StaffUserId = sc.StaffUserId and sc.rn=1
	left join
		   (select StaffUserId, EMIS,
			ROW_NUMBER() over (partition by StaffUserId order by EMIS) as rn
			from StaffEMIS) es on s.StaffUserId = es.StaffUserId and es.rn=1
	-- MS 4.11.16 IndOrg emails
	left join (select StaffEnterpriseId, DeptEnterpriseId, Email,
			ROW_NUMBER() over (partition by StaffEnterpriseId,deptenterpriseid order by StaffEnterpriseId,deptenterpriseid) as rn
			from IndOrgEmails) em on s.StaffEnterpriseId = em.StaffEnterpriseId and d.deptenterpriseid = em.deptenterpriseid and em.rn=1
	-- MS 22.1.17 add AUP confirmation information
	left join ETL_PROD.dbo.CMC_UserTermsOfUseAcceptance au  with (nolock) on au.UserRegID = s.StaffRegistryId and au.UserLogonOrg = d.DeptPDRegistryID
	where (LoginId is not null or Smartcard is not null)


	WAITFOR DELAY '00:05:00' -- WAIT SEVEN before new set of incremental builds



		PRINT 'End Section SEVEN'
		PRINT GETDATE()

		


		end
 