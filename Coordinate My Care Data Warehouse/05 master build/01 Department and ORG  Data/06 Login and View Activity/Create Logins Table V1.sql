USE [ETL_Local_PROD]
GO
/****** Object:  StoredProcedure [dbo].[AT_Loging_Data]    Script Date: 18/11/2020 11:48:37 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




Create PROCEDURE [dbo].[AT_Logins_Table_Creation] 
-- Amended for PD Upgrade
AS
BEGIN



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

	--select * from AT_Staff
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

	End