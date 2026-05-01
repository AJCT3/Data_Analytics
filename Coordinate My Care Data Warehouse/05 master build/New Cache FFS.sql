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

alter PROCEDURE [dbo].[AT_NewCacheUpdated]

AS

BEGIN



	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[CacheOrganization]') is not null
	drop table [ETL_Local_PROD].[dbo].[CacheOrganization]
 
	Select 
	* 
	into [ETL_Local_PROD].[dbo].[CacheOrganization] 
	from [ETL_Local_PROD].[dbo].[AT_CacheOrg] with (nolock)

	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[Department]') is not null
	drop table [ETL_Local_PROD].[dbo].[Department] 
 
	select 
	* 
	into [ETL_Local_PROD].[dbo].[Department]  
	from [ETL_Local_PROD].[dbo].[AT_Dept] with (nolock)


		 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[DeptHierarchy]') is not null
	drop table [ETL_Local_PROD].[dbo].[DeptHierarchy] 
	select 
	* 
	into [ETL_Local_PROD].[dbo].[DeptHierarchy] 
	from [ETL_Local_PROD].[dbo].[AT_Dept_Heirarchy] with (nolock)

	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[Staff]') is not null
	drop table [ETL_Local_PROD].[dbo].[Staff] 
	select 
	* 
	into Staff 
	from [ETL_Local_PROD].[dbo].[AT_Staff] with (nolock)

		 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[StaffDeptContext]') is not null
	drop table [ETL_Local_PROD].[dbo].[StaffDeptContext] 

	select poc.ItemId as ProviderOrgContext, s.*, d.*
	into [ETL_Local_PROD].[dbo].[StaffDeptContext] 
	from ETL_PROD.dbo.CMC_ProviderOrgContext poc
	left join ETL_PROD.dbo.CMC_IndividualProvider ip on poc.Provider = ip.ItemId
	left join ETL_PROD.dbo.CMC_Individual i on i.PDRegistryID = ip.RegistryID
	left join AT_Staff s on s.Individual = i.ItemID
	left join AT_Dept d on d.Organization = poc.Organization
	-- Exclude LastClinicalApprover rows introduced in 15.1 release 
	where ip.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
	AND ip.ItemId not like 'PS|%|%|%|%|%|LCA'
	AND poc.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
	AND poc.ItemId not like 'PS|%|%|%|%|%|LCA'


	
	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[IndOrgAddresses]') is not null
	drop table [ETL_Local_PROD].[dbo].[IndOrgAddresses] 


	SELECT 
	distinct 
	StaffEnterpriseId,
	DeptEnterpriseId,
	a.StreetLine, 
	a.Line2, 
	a.City, 
	a.County, 
	a.PostalCode,
	ISNULL(a.StreetLine,'') +
	  case when a.StreetLine is not null and rtrim(a.StreetLine) <> '' 
		   and a.Line2 is not null and rtrim(a.Line2) <> ''
		   then ', ' else '' end + 
	ISNULL(a.Line2,'') + case when a.City is not null and rtrim(a.City) <> '' then ', ' else '' end +
	ISNULL(a.City,'') + case when a.County is not null and rtrim(a.County) <> '' then ', ' else '' end +
	ISNULL(a.County,'') as CombinedAddress,
	-- add address location MS 5.2.17
	lo.EnterpriseID as AddressLocation

	into [ETL_Local_PROD].[dbo].[IndOrgAddresses] 

	FROM [ETL_PROD].[dbo].[CMC_Location] lo  with (nolock)
	join [ETL_Local_PROD].[dbo].[AT_IndToOrg] io  with (nolock) on lo.OrganizationEID = io.deptenterpriseid and lo.IndividualEID = io.StaffEnterpriseID
	join ETL_PROD.dbo.CMC_Address a  with (nolock) on lo.AddressEID = a.PDEnterpriseID
	-- Active [ETL_Local_PROD].[dbo].[AT_IndToOrg]s only MS 21.2.17
	  where (StartDate is null or CAST(startdate as date) <= CAST(getdate() as date))
	  and (endDate is null or CAST(enddate as date) > CAST(getdate() as date))



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
 
   	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[IndOrgPhones]') is not null
	drop table [ETL_Local_PROD].[dbo].[IndOrgPhones] 

	 SELECT 
	 distinct 
	 Staffenterpriseid,
	 DeptEnterpriseId, 
	 isnull(ph.FullNumber,ph.TelephoneNumber) as Telephone,
	-- Add phone location MS 5.2.17
	lo.EnterpriseID as PhoneLocation

	into [ETL_Local_PROD].[dbo].[IndOrgPhones] 

	  FROM [ETL_PROD].[dbo].[CMC_Location] lo  with (nolock)
	  join [ETL_Local_PROD].[dbo].[AT_IndToOrg] io  with (nolock) on lo.OrganizationEID = io.deptenterpriseid and lo.IndividualEID = io.StaffEnterpriseID
	  join etl_PROD.dbo.cmc_location_phones loc  with (nolock) on lo.itemid = loc.location
	  join ETL_PROD.dbo.CMC_Telecom ph  with (nolock) on loc.Phone = ph.ItemId
	-- Active [ETL_Local_PROD].[dbo].[AT_IndToOrg]s only MS 21.2.17
	  where (StartDate is null or CAST(startdate as date) <= CAST(getdate() as date))
	  and (endDate is null or CAST(enddate as date) > CAST(getdate() as date))

 
    	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[PatientHSCContacts]') is not null
		drop table [ETL_Local_PROD].[dbo].[PatientHSCContacts] 
		select 
		sel1.*,
		np1.Description as StaffTitleDescription,
		np2.Description as NamePrefixDescription,
		dpr.Description as RoleDescription,
		dor.Description as OrgRoleDescription

		into [ETL_Local_PROD].[dbo].[PatientHSCContacts] 

		from
			(
				SELECT
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

				from ETL_PROD.dbo.CMC_PatientSummary ps  with (nolock)
				join ETL_PROD.dbo.CMC_Patient_PatientNumbers po  with (nolock) on po.Patient = ps.ItemID
				join ETL_PROD.dbo.CMC_PatientNumber no  with (nolock) on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
				join ETL_PROD.dbo.CMC_PatientSummary_Providers pa  with (nolock) on ps.itemid = pa.PatientSummary
				join ETL_PROD.dbo.CMC_DocumentProvider b  with (nolock) on pa.Provider = b.Provider
				left join [ETL_Local_PROD].[dbo].[StaffDeptContext] a  with (nolock) on pa.Provider = a.ProviderOrgContext
				left join ETL_PROD.dbo.CMC_IndividualProvider ip  with (nolock) on ip.ItemId = b.Provider

 			) sel1
		-- Add title and role descriptions MS 19.2.16
		left join ETL_PROD.dbo.Coded_NamePrefix np1  with (nolock)on StaffTitle = np1.code
		left join ETL_PROD.dbo.Coded_NamePrefix np2  with (nolock) on NamePrefix = np2.code
		left join ETL_PROD.dbo.Coded_DocumentProviderRole dpr  with (nolock) on Role = dpr.code
		left join ETL_PROD.dbo.Coded_DocumentOrganizationRole dor  with (nolock) on OrgRole = dor.code




    	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[PatientRegisteredGP]') is not null
		drop table [ETL_Local_PROD].[dbo].[PatientRegisteredGP] 
 
		select 
		* 
		into [ETL_Local_PROD].[dbo].[PatientRegisteredGP]  
		from [ETL_Local_PROD].[dbo].[AT_PatientRegistered_GP] with (nolock)

		PRINT 'End Section One'
		PRINT GETDATE()

		WAITFOR DELAY '00:02:00' -- WAIT ONE before first incremental builds

		PRINT 'Start Section Two'
		PRINT GETDATE()


		
		insert into [ETL_Local_PROD].[dbo].[AuditPatient]

		select
		CMC_ID,
		FromPatientSummary,
		ToPatientSummary,
		PatAuditID as Audit,
		ActionTime,
		ActionType,
		cast(StaffRegistryId as varchar(255)) as StaffRegistryId,
		DeptPDRegistryId,
		Role,
		LoginReference

		from [ETL_Local_PROD].[dbo].[AuditPatient_New] 
		where not exists (select audit from [ETL_Local_PROD].[dbo].[AuditPatient]where audit = PatAuditID)


		insert into [ETL_Local_PROD].[dbo].[AuditPatient-CarePlan]

		select
		CMC_ID,
		FromCarePlan,
		ToCarePlan,
		CPAuditID as Audit,
		ActionTime,
		ActionType,
		StaffRegistryId,
		DeptPDRegistryId,
		Role 

		from [ETL_Local_PROD].[dbo].[AuditPatient_Careplan_New] with (nolock)
		where not exists (select audit from [ETL_Local_PROD].[dbo].[AuditPatient-CarePlan] where audit = CPAuditID)
 

		PRINT 'End Section Two'
		PRINT GETDATE()

		WAITFOR DELAY '00:02:00' -- WAIT TWO after first incremental builds

		PRINT 'Start Section Three'
		PRINT GETDATE()
		-------------------------------------------------------------------------------------------------------------------------------------------------------------------


		 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AccurateEnteredBy]') is not null
		drop table [ETL_Local_PROD].[dbo].[AccurateEnteredBy] 
 
		select 
		* 
		into AccurateEnteredBy 
		from [ETL_Local_PROD].[dbo].[AT_AccuratelyEnteredBy] with (nolock)


		drop table Load.PDS
		select * into Load.PDS from Load.[Cache-PDS] with (nolock)
-- correct position for this one as it's used by PatientDetail MS 12.8.16

	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[PatientAddresses]') is not null
		drop table [ETL_Local_PROD].[dbo].[PatientAddresses] 
 
		select 
		* into 
		PatientAddresses 
		from [ETL_Local_PROD].[dbo].[AT_PatAddress] with (nolock)



		-- correct position for this one as it's used by PatientDetail MS 28.9.16
			drop table PatientContactInfo
			select * into PatientContactInfo from [Cache-PatientContactInfo] with (nolock)
		---------------------------------------------------------------------
		-- Moved to above Patient Detail build as this table is required for Patient Detail
		-- Issues found during second data reconciliation on Data Quality reports
		-- GW 23/07/2019
		drop table PatientDiagnoses
		select * into PatientDiagnoses from [Cache-PatientDiagnoses] with (nolock)
		CREATE NONCLUSTERED INDEX [Main] ON [dbo].[PatientDiagnoses] 
		(
			[MainDiagnosis] ASC
		)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
		----------------------------------------------------------------------
		drop table PatientDetail
		select * into PatientDetail from [Cache-PatientDetail] with (nolock)


		insert into CDA.PatientUUID
		select cmc_id,newid() from PatientDetail pd with (nolock)
		where cmc_id not in (select cmc_id from CDA.PatientUUID)
		insert into CDA.PatientTemplateUUID
		select pd.cmc_id,t.[Template ID],newid()
		from PatientDetail pd with (nolock)
		join Reference.Templates t on 1=1
		left join CDA.PatientTemplateUUID u
		on pd.cmc_id=u.cmc_id and t.[Template ID] = u.[Template ID]
		where u.cmc_id is null
		drop table PatientAlerts
		select * into PatientAlerts from [Cache-PatientAlerts] with (nolock)
		drop table PatientAliases
		select * into PatientAliases from [Cache-PatientAliases] with (nolock)
		drop table PatientAllergies
		select * into PatientAllergies from [Cache-PatientAllergies] with (nolock)
		drop table PatientDisabilities
		select * into PatientDisabilities from [Cache-PatientDisabilities] with (nolock)
		drop table PatientHSCContactInfo
		select * into PatientHSCContactInfo from [Cache-PatientHSCContactInfo] with (nolock)
		drop table PatientMedications
		select * into PatientMedications from [Cache-PatientMedications] with (nolock)
		-- cache PatientDiagnoses MS 19.10.16
		---------------------------------------------------------------------
		-- Moved to above Patient Detail build as this table is required for Patient Detail
		-- Issues found during second data reconciliation on Data Quality reports
		-- GW 23/07/2019
		/*drop table PatientDiagnoses
		select * into PatientDiagnoses from [Cache-PatientDiagnoses] with (nolock)
		CREATE NONCLUSTERED INDEX [Main] ON [dbo].[PatientDiagnoses] 
		(
			[MainDiagnosis] ASC
		)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
		*/
		----------------------------------------------------------------------
		drop table CarePlanSymptoms
		select * into CarePlanSymptoms from [Cache-CarePlanSymptoms] with (nolock)
		-- move to before next entry which now uses it MS 18.3.17
		DROP TABLE [dbo].[AssessorDQInfo]
		select * into [dbo].[AssessorDQInfo] from [dbo].[Cache-AssessorDQInfo] with (nolock)
		-- Correct modified local cmc ids for migrated staff MS 19.2.17
		update AssessorDQInfo
		set StaffLocalCMCId =
		ISNULL(
		(select distinct primarystaffid from Protocol.MigratedStaffIdLookup l where l.staffuserid=AssessorDQInfo.staffuserid),
		stafflocalcmcid) 

		PRINT 'End Section Three'
		PRINT GETDATE()

	
		WAITFOR DELAY '00:02:00' -- WAIT THREE lowered from 2 - 5 minutes.
						 -- before Protocol tables


		PRINT 'Start Section Four'
		PRINT GETDATE()


				update protocol.accessdatadetail2 set ActivityDepartmentId =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=ActivityDepartmentId)
		where activitydepartmentid in (select localcmcid from protocol.MigratedDeptChanges) 

		update protocol.accuratelogins set LogonDepartmentId =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=LogonDepartmentId)
		where Logondepartmentid in (select localcmcid from protocol.MigratedDeptChanges) 

		update protocol.episodeteams set ActivityDeptId =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=ActivityDeptId)
		where activitydeptid in (select localcmcid from protocol.MigratedDeptChanges) 

		update protocol.extraaccessdetail set ActivityDepartmentId =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=ActivityDepartmentId)
		where activitydepartmentid in (select localcmcid from protocol.MigratedDeptChanges) 

		update protocol.migratedprofcontacts set CMCDeptIdchar =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=CMCDeptIdChar)
		where CMCdeptidchar in (select localcmcid from protocol.MigratedDeptChanges) 

		update protocol.ODSReconciliationCandidates set Dept_Id =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=Dept_Id)
		where Dept_ID in (select localcmcid from protocol.MigratedDeptChanges) 

		update protocol.oldsystemcareplans set PracticeExternalID =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=PracticeExternalID)
		where PracticeExternalID in (select localcmcid from protocol.MigratedDeptChanges) 

		update protocol.oldsystemcareplans set OriginalWorkbaseId =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=OriginalWorkbaseId)
		where OriginalWorkbaseId in (select localcmcid from protocol.MigratedDeptChanges) 

		update protocol.oldsystemcareplans set LatestWorkbaseId =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=LatestWorkbaseId)
		where LatestWorkbaseId in (select localcmcid from protocol.MigratedDeptChanges) 

		update protocol.oldsystemcareplans set OriginalApproverWorkbaseId =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=OriginalApproverWorkbaseId)
		where OriginalApproverWorkbaseId in (select localcmcid from protocol.MigratedDeptChanges) 

		update protocol.oldsystemcareplans set LatestApproverWorkbaseId =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=LatestApproverWorkbaseId)
		where LatestApproverWorkbaseId in (select localcmcid from protocol.MigratedDeptChanges) 

		update protocol.OldSystemCarePlansMigratedDuplicates set PracticeExternalID =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=PracticeExternalID)
		where PracticeExternalID in (select localcmcid from protocol.MigratedDeptChanges) 
		update protocol.oldsystemcareplansMigratedDuplicates set OriginalApproverWorkbaseId =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=OriginalApproverWorkbaseId)
		where OriginalApproverWorkbaseId in (select localcmcid from protocol.MigratedDeptChanges) 
		update protocol.oldsystemcareplansMigratedDuplicates set LatestApproverWorkbaseId =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=LatestApproverWorkbaseId)
		where LatestApproverWorkbaseId in (select localcmcid from protocol.MigratedDeptChanges) 
		update protocol.palliativeclinicians set dept_external_id =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=dept_external_id)
		where dept_external_id in (select localcmcid from protocol.MigratedDeptChanges) 
		update protocol.patientdemographics set PracticeExternalID =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=PracticeExternalID)
		where PracticeExternalID in (select localcmcid from protocol.MigratedDeptChanges) 
		update protocol.patientdemographicsmigratedduplicates set PracticeExternalID =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=PracticeExternalID)
		where PracticeExternalID in (select localcmcid from protocol.MigratedDeptChanges) 
		update protocol.worktrayallocations set workbase_id =
		(select deptlocalcmcid from protocol.MigratedDeptChanges where localcmcid=workbase_id)
		where workbase_id in (select localcmcid from protocol.MigratedDeptChanges) 
		-- end MS 28.5.17

		PRINT 'End Section Four'
		PRINT GETDATE()

		WAITFOR DELAY '00:07:00' -- WAIT FOUR after Protocol tables

		PRINT 'Start Section Five'
		PRINT GETDATE()
		--select * from PatientHSCContactsHistoric
		drop table PatientHSCContactsHistoric
		select * into [PatientHSCContactsHistoric] from [Cache-PatientHSCContactsHistoric] with (nolock)
		drop table AllDataMedications
		select * into AllDataMedications from [Cache-AllDataMedications] with (nolock)
		drop table AllDataDisabilities
		select * into AllDataDisabilities from [Cache-AllDataDisabilities] with (nolock)
		drop table AllDataAddresses
		select * into AllDataAddresses from [Cache-AllDataAddresses] with (nolock)
		drop table AllDataContacts
		select * into AllDataContacts from [Cache-AllDataContacts] with (nolock)
		drop table AllDataContactInfo
		select * into AllDataContactInfo from [Cache-AllDataContactInfo] with (nolock)
		drop table AllDataAllergies
		select * into AllDataAllergies from [Cache-AllDataAllergies] with (nolock)
		drop table AllDataAliases
		select * into AllDataAliases from [Cache-AllDataAliases] with (nolock)
		drop table AllDataAlerts
		select * into AllDataAlerts from [Cache-AllDataAlerts] with (nolock)

		PRINT 'End Section Five added 15/02/2019'
		PRINT GETDATE()

		WAITFOR DELAY '00:02:00' -- WAIT FIVE Pivot Symptoms is where
								 -- New Cache Job had been crashing
		exec dbo.PivotSymptoms

		PRINT 'End Section Five Original'
		PRINT GETDATE()

		WAITFOR DELAY '00:02:00' -- WAIT FIVE Pivot Symptoms is where
								 -- New Cache Job had been crashing

		PRINT 'Start Section Six'
		PRINT GETDATE()


		drop table AllDataSymptoms
		select * into AllDataSymptoms from [Cache-AllDataSymptoms] with (nolock)
		drop table AllDataContactsHistoric
		select * into AllDataContactsHistoric from [Cache-AllDataContactsHistoric] with (nolock)
		drop table AllDataLPAHistoric
		select * into AllDataLPAHistoric from [Cache-AllDataLPAHistoric] with (nolock)
		DROP TABLE [dbo].[AllDataPersonalHistoric]
		select * into [dbo].[AllDataPersonalHistoric] from [dbo].[Cache-AllDataPersonalHistoric] with (nolock)
		DROP TABLE [dbo].[StaffDeptActor]
		select * into [dbo].[StaffDeptActor] from [dbo].[Cache-StaffDeptActor] with (nolock)
		DROP TABLE [dbo].[WorkbaseDQInfo]
		select * into [dbo].[WorkbaseDQInfo] from [dbo].[Cache-WorkbaseDQInfo] with (nolock)
		drop table Protocol.metadatadepteids
		select * into Protocol.MetadatadeptEids from Protocol.[Cache-MetadatadeptEids]
		drop table Protocol.metadataStaffeids
		select * into Protocol.MetadataStaffEids from Protocol.[Cache-MetadataStaffEids]



		if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AuditApprovals]') is not null
		drop table [ETL_Local_PROD].[dbo].[AuditApprovals] 
 
		select 
		* into 
		AuditApprovals from [ETL_Local_PROD].[dbo].[AT_AuditApprovals] with (nolock)
		-- add APV version 22.3.17


		drop table [AuditApprovals-AllPublishedVersions] 
		select * into [AuditApprovals-AllPublishedVersions] from [Cache-AuditApprovals-AllPublishedVersions]
		DROP TABLE [dbo].[PatientDetailFull]
		select * into [dbo].[PatientDetailFull] from [dbo].[Cache-PatientDetailFull] with (nolock)

	 
	 
		DROP TABLE [Protocol].[PatientPractice]
		select * into [Protocol].[PatientPractice] from [Protocol].[Cache-PatientPractice] with (nolock)
		DROP TABLE [dbo].[PatientDetailHistoric]
		select * into [dbo].[PatientDetailHistoric] from [dbo].[Cache-PatientDetailHistoric] with (nolock)
	 
		DROP TABLE [dbo].[PatientDetailSpan]
		select * into [dbo].[PatientDetailSpan] from [dbo].[Cache-PatientDetailSpan] with (nolock)
	 
		DROP TABLE [dbo].[PatientDetailSpanAll]
		select * into [dbo].[PatientDetailSpanAll] from [dbo].[Cache-PatientDetailSpanAll] with (nolock)


		PRINT 'End Section Six'
		PRINT GETDATE()

		WAITFOR DELAY '00:01:00' -- WAIT NINE after PatientDetailSpanAll     .... WHy?

		PRINT 'Start Section Six A'
		PRINT GETDATE()

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

WAITFOR DELAY '00:01:00' -- WAIT SIX after new code added in January 2018



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






		PRINT 'End Section SEVEN'
		PRINT GETDATE()

		WAITFOR DELAY '00:05:00' -- WAIT SEVEN before new set of incremental builds

		PRINT 'Start Section EIGHT'
		PRINT GETDATE()


		--this is a waste of time
	 --if OBJECT_ID ('Tempdb..#AuditMig') is not null
		--drop table #AuditMig
		--select
		--REPLACE(ad.GenusId,'PatientSummary||','') as CMC_ID,
		--REPLACE(FromVersionId,'PatientSummary','PS') as FromPatientSummary,
		--REPLACE(ToVersionId,'PatientSummary','PS') as ToPatientSummary,
		--ItemId as Audit,
		--ad.ActionTime,
		--ad.ActionType

		--into #AuditMig

		-- from ETL_PROD.dbo.CMC_AuditData ad  with (nolock) 
		-- left join [ETL_Local_PROD].[dbo].[AuditMigration]am on am.Audit = ad.ItemId
		-- where ad.Actor = 'System' 
		-- and ad.ActionType not in ('Login','Logout') 
		--and am.Audit is null
		--and ad.RecordName = 'PatientSummary'
 
  
 --select * from [ETL_Local_PROD].[dbo].[AuditMigration] order by actiontime desc


 
	--insert into AuditMigration ([CMC_ID], [FromPatientSummary], [ToPatientSummary], [Audit], [ActionTime], [ActionType])
	--select * from #AuditMig with (nolock)

 
	DROP TABLE [dbo].[OrgArea]
	select * into [dbo].[OrgArea] from [dbo].[Cache-OrgArea] with (nolock)
	-- cache GPInformation MS 18.10.16
	DROP TABLE [Reporting].[GPInformation]
	select * into [Reporting].[GPInformation] from [Reporting].[Cache-GPInformation] with (nolock)
	-- GW April 2019
	DROP TABLE [reporting].[GPInformation-No-GP-Check]
	select * into [reporting].[GPInformation-No-GP-Check] from [reporting].[Cache-GPInformation-No-GP-Check] with (nolock)

	--alter TABLE [ETL_Local_PROD].[dbo].[ServiceSearch]
 --  ADD CONSTRAINT PK_ItemID PRIMARY KEY CLUSTERED (ItemID);

 	 if OBJECT_ID ('Tempdb..#MaxID') is not null
		drop table #MaxID
		select 
		RowItemID 
		into #MaxID 
		from [ETL_Local_PROD].[dbo].[ServiceSearch] where OverallOrder = 
																			(
																			select max(OverAllOrder) from [ETL_Local_PROD].[dbo].[ServiceSearch]
																			)




--CREATE NONCLUSTERED INDEX [TimeSearchIndex] ON [dbo].[ServiceSearch]
--(
--	[ActionTime] ASC,
--	[OverallOrder] ASC
--)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
--GO 


WAITFOR DELAY '00:01:00' -- WAIT EIGHT after incremental builds



	
		insert into [ETL_Local_PROD].[dbo].[ServiceSearch]

		select 
		CAST(SUBSTRING(a.ItemId,9,Len(a.ItemId)) as numeric) as RowItemID,
		b.OverAllOrder,
		a.ItemId,
		ActionTime,
		MRNs,
		Actor,
		REGISTRYRoles,
		---- add stripped actor for performance MS 21.5.16
		 rtrim(REPLACE(Actor,'EMISAvail','')) as ODS
 
		from etl_PROD.dbo.cmc_auditdata a with (nolock)
		 inner join [ETL_Local_PROD].[dbo].[AT_CMC_AuditData_RowID]b with (nolock) on b.Itemid = a.ItemId
		where ActionType = 'SearchPatient'
		and REGISTRYRoles like '%HSCC_Service_Search%'
		and CAST(SUBSTRING(a.ItemId,9,Len(a.ItemId)) as numeric) > (
																		select RowItemID from #MaxID
																		)
		and a.actionTime >= dateadd(week,-3,getdate())																	 

			IF OBJECT_ID('[ETL_Local_PROD].[dbo].[ServiceSearchGeneric]') IS NOT NULL 
			 dROP TABLE [ETL_Local_PROD].[dbo].[ServiceSearchGeneric]
			  select*,
			rtrim(REPLACE(Actor,'AvailGen','')) as ODS2

			 into  [ETL_Local_PROD].[dbo].[ServiceSearchGeneric]
			 from ServiceSearch 
			where Actor like 'AvailGen%'




		 




			if OBJECT_ID ('[ETL_Local_PROD].[reporting].[TeamAudit]') is not null
		drop table [ETL_Local_PROD].[reporting].[TeamAudit]
		select
		*
		into [ETL_Local_PROD].[reporting].[TeamAudit]
		 from [Reporting].[Cache-TeamAudit]


 

		
	 if OBJECT_ID ('[ETL_Local_PROD].[reporting].[DisambiguatedActivityTeams]') is not null
		drop table [ETL_Local_PROD].[reporting].[DisambiguatedActivityTeams]

		select distinct 
		* 
		into [ETL_Local_PROD].[reporting].[DisambiguatedActivityTeams]
		from [ETL_Local_PROD].[reporting].[Cache-DisambiguatedActivityTeams]



	PRINT 'End Section EIGHT'
	PRINT GETDATE()

	WAITFOR DELAY '00:03:00' -- WAIT EIGHT after incremental builds

	PRINT 'Start Section NINE'
	PRINT GETDATE()

	--//////////up to here

	 


	insert into AvailabilityLog

	select 
	ItemId as id,
cast(actiontime as datetime) as EventDateTime,
cast(actiontime as date) as EventDate,
case when MRNs='0' then 'N' else 'Y' end as match,
'EMIS' as Service, 
DeptEnterpriseID, 
Team,
a.RowItemID
from ServiceSearch a
-- logic here heeds to take close dates into account and also to follow Gareth's instructions re filtering
join (select deptodscode,deptenterpriseid,ROW_NUMBER() over (PARTITION by deptodscode order by deptodscode) as rn from PDDepartment  where deptodscode is not null) d on a.ODS = DeptODSCode and d.rn=1
-- enforce uniqueness MS 29.5.16
join (select *,
row_number() over (partition by activitydepartmentid order by team) rn 
from Reporting.DisambiguatedActivityTeams) at on at.ActivityDepartmentID = d.DeptEnterpriseID and at.rn=1

where a.RowItemID > (
																		select RowItemID from #MaxID
																			 
																	)

--select top 5* from ServiceSearch
--select top 5* from AvailabilityLog

	WAITFOR DELAY '00:03:00' 

	--drop table AvailabilityLog
	--select distinct * into AvailabilityLog from [Cache-AvailabilityLog] with (nolock)




--CAST(SUBSTRING(a.ItemId,9,Len(a.ItemId)) as numeric) as RowItemID,

--ALTER TABLE select top 5* from AvailabilityLog
--ADD RowItemID numeric(18,0)null;
--ALTER TABLE AvailabilityLog
--   ADD CONSTRAINT PK_AvailabilityID PRIMARY KEY CLUSTERED (ID); 
  /** table Work...


  ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_CarePlan_New]
   ADD CONSTRAINT PK_AudCarePlan PRIMARY KEY CLUSTERED (CPAuditID);


     CREATE INDEX CMCID_OverAllOrder
ON [ETL_Local_PROD].[dbo].[AT_AccessDataDetail]  (CMC_ID, OverAllOrder);


**/





	--RowItemID, -- NEed to make this table incrimental load like service search...
	
	drop table AvailabilityLogGeneric
	select distinct * into AvailabilityLogGeneric from [Cache-AvailabilityLogGeneric] with (nolock)
	DROP TABLE [Reporting].[PatientListReport]
	select * into [Reporting].[PatientListReport] from [Reporting].[Cache-PatientListReport] with (nolock)


	DROP TABLE [Reporting].[DisambiguatedPractices]
	select * into [Reporting].[DisambiguatedPractices] from [Reporting].[Cache-DisambiguatedPractices] with (nolock)
	DROP TABLE [dbo].[PatientDQInfo]
	select * into [dbo].[PatientDQInfo] from [dbo].[Cache-PatientDQInfo] with (nolock)
	DROP TABLE [dbo].[DemographicVersions]
	select * into [dbo].[DemographicVersions] from [dbo].[Cache-DemographicVersions] with (nolock)


	exec dbo.DePivotDemographicVersions


	IF OBJECT_ID('tempdb..#Templatest') IS NOT NULL 
	dROP TABLE #Templatest
 select 
 * 
 into #Templatest
 from (
 select* from 
	(
	select 
	*,
	ROW_NUMBER() over (PARTITION by cmc_id,[Column] order by VersionNumber desc) as rn 
	from DemographicVersionsAnswers
	) sel1 where rn=1
	)y

		  IF OBJECT_ID('tempdb..#DemographicVersionsDifferences') IS NOT NULL 
			dROP TABLE #DemographicVersionsDifferences

		 ;with metadata as
		(select c.name from sys.all_columns c join sys.all_objects o
		   on c.object_id=o.object_id
		   where o.name='DemographicVersions' and c.name not in ('CMC_ID','VersionNumber','PublishedTime'))

		select
		d.cmc_id,
		d.[Column],
		i.Question_Desc as Description,
		d.[value] as CurrentValue, h.[value] as HistoricalValue,
		d.PublishedTime as CurrentPublishedTime, h.PublishedTime as HistoricalPublishedTime,
		d.VersionNumber as CurrentVersionNumber, h.VersionNumber as HistoricalVersionNumber

		into #DemographicVersionsDifferences

		from #Templatest d with (nolock)
		join metadata m with (nolock)
		on d.[Column] = m.name
		join Reference.AllDataInfo i on i.ShortCode = m.name
		join DemographicVersionsAnswers h with (nolock)
		on d.[Column] = h.[Column]
		and d.cmc_id = h.cmc_id
		and d.value <> h.value

		
	 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[DemographicsChanges]') is not null
		drop table [ETL_Local_PROD].[dbo].[DemographicsChanges]
		;with
		a1 as
		(
		select cmc_id,min(currentpublishedtime) as since,
		'*SEP*On ' + convert(varchar(11),cast(max(CurrentPublishedTime) as DATE),106) + ': ' + [description] + ' changed to: ' + CurrentValue + ' from: ' + historicalvalue as ValueChange
		from #DemographicVersionsDifferences
		where CurrentPublishedTime > DATEADD(week,-2,GETDATE()) 
		group by cmc_id,[column],[description],CurrentValue,historicalvalue
		),

		a2 as (select cmc_id,MAX(since) as Since from a1 group by cmc_id)

		select a2.cmc_id,Since,
		'Record changes:' + cast(replace(replace(replace(stuff((
		select ValueChange from a1
		where a1.cmc_id = a2.cmc_id
		for xml path('')
		),1,0,''),'<ValueChange>',''),'</ValueChange>',''),'*SEP*',CHAR(0x0D)+CHAR(0x0A))
		as varchar(1000)) as [Changes]

		into [ETL_Local_PROD].[dbo].[DemographicsChanges]

		from a2
		group by a2.cmc_id,Since

		
		DROP TABLE [Extracts].[UrgentCareExtract]
		select * into [Extracts].[UrgentCareExtract] from [Extracts].[Cache-UrgentCareExtract] with (nolock)
		DROP TABLE [Extracts].[SECAmbExtract]
		select * into [Extracts].[SECAmbExtract] from [Extracts].[Cache-SECAmbExtract] with (nolock)

		DROP TABLE [Extracts].[LASExtract]
		select * into [Extracts].[LASExtract] from [Extracts].[Cache-LASExtract] with (nolock)
		DROP TABLE [Extracts].[emisrollout]
		select * into [Extracts].[emisrollout] from [Extracts].[cache-emisrollout] with (nolock)



		-- MS 181016 cache IndividualPatientExceptions
		DROP TABLE [DataQuality].[IndividualPatientExceptions]
		select * into [DataQuality].[IndividualPatientExceptions] from [DataQuality].[Cache-IndividualPatientExceptions] with (nolock)
		DROP TABLE [DataQuality].[ExceptionWorkbaseLabels]
		select * into [DataQuality].[ExceptionWorkbaseLabels] from [DataQuality].[Cache-ExceptionWorkbaseLabels] with (nolock)
		EXEC [Reporting].[CacheMonthlyAdditionsToCMC]
		DROP TABLE [Reporting].[V_Deaths_Statistics_Practice]
		select * into [Reporting].[V_Deaths_Statistics_Practice] from [Reporting].[Cache-V_Deaths_Statistics_Practice] with (nolock)
		DROP TABLE [Reporting].[ActivityCareplans]
		select * into [Reporting].[ActivityCareplans] from [Reporting].[Cache-ActivityCareplans] with (nolock)


		WAITFOR DELAY '00:00:30' -- WAIT EIGHT after incremental builds

		DROP TABLE [Reporting].[CCGActivity]
		select * into [Reporting].[CCGActivity] from [Reporting].[Cache-CCGActivity] with (nolock)
		DROP TABLE [Reporting].[CCGActivity24]
		select * into [Reporting].[CCGActivity24] from [Reporting].[Cache-CCGActivity24] with (nolock)

		WAITFOR DELAY '00:00:30' -- WAIT EIGHT after incremental builds

		-- Added 10/08/2018 - table used for the new Data Overview report
		DROP TABLE [Reporting].[CCGActivity24_ForDataOverview]
		select * into [Reporting].[CCGActivity24_ForDataOverview] from [Reporting].[Cache-CCGActivity24_ForDataOverview] with (nolock)
		-- Added 18/04/2018 - table used by new Activity Report
		DROP TABLE [Reporting].[CCGActivityAllData]
		select * into [Reporting].[CCGActivityAllData] from [Reporting].[Cache-CCGActivityAllData] with (nolock)

		 
		 WAITFOR DELAY '00:00:30' -- WAIT EIGHT after incremental builds

		 DROP TABLE [Reporting].[CCGActivityAllData_ActivityByMonthReport]
		 select * into [Reporting].[CCGActivityAllData_ActivityByMonthReport] from [Reporting].[Cache-CCGActivityAllData_ActivityByMonthReport] with (nolock)
		--

		WAITFOR DELAY '00:00:30' -- WAIT EIGHT after incremental builds

		drop table CareHomes
		select * into CareHomes from [Cache-CareHomes] with (nolock)
		drop table PatientPostcodePhoneNumber
		select * into PatientPostcodePhoneNumber from [Cache-PatientPostcodePhoneNumber] with (nolock)
		-- added MS 6.4.16 (whoops)
		drop table PatientPrimaryAddressIsCareHome
		select * into PatientPrimaryAddressIsCareHome from [Cache-PatientPrimaryAddressIsCareHome] with (nolock)
		-- Added MS 18.3.17
		drop table PatientCurrentAddressIsCareHome
		select * into PatientCurrentAddressIsCareHome from [Cache-PatientCurrentAddressIsCareHome] with (nolock)
		drop table PatientsInCareHomes
		select * into PatientsInCareHomes from [Cache-PatientsInCareHomes] with (nolock)
		drop table [PatientDetail-AllPublishedVersions]
		select * into [PatientDetail-AllPublishedVersions] from [Cache-PatientDetail-AllPublishedVersions] with (nolock)



		drop table [AllDataAddresses-APV]
		select * into [AllDataAddresses-APV] from [Cache-AllDataAddresses-APV] with (nolock)

		drop table [AllDataAlerts-APV]
		select * into [AllDataAlerts-APV] from [Cache-AllDataAlerts-APV] with (nolock)

		drop table [AllDataAliases-APV]
		select * into [AllDataAliases-APV] from [Cache-AllDataAliases-APV] with (nolock)
		drop table [AllDataAllergies-APV]
		select * into [AllDataAllergies-APV] from [Cache-AllDataAllergies-APV] with (nolock)
		drop table [AllDataContactInfo-APV]
		select * into [AllDataContactInfo-APV] from [Cache-AllDataContactInfo-APV] with (nolock)
		drop table [AllDataContacts-APV]
		select * into [AllDataContacts-APV] from [Cache-AllDataContacts-APV] with (nolock)


		drop table [AllDataDisabilities-APV]
		select * into [AllDataDisabilities-APV] from [Cache-AllDataDisabilities-APV] with (nolock)
		drop table [AllDataMedications-APV]
		select * into [AllDataMedications-APV] from [Cache-AllDataMedications-APV] with (nolock)
		drop table [AllDataSymptoms-APV]
		select * into [AllDataSymptoms-APV] from [Cache-AllDataSymptoms-APV] with (nolock)


		exec [dbo].[PivotSymptoms-APV]
		drop table [PatientDetailFull-AllPublishedVersions]
		select * into [PatientDetailFull-AllPublishedVersions] from [Cache-PatientDetailFull-AllPublishedVersions] with (nolock)
		exec [dbo].[DePivotPatientDetail-APV]


		-- cache AccessLog for performance MS 26.8.16
		drop table Reporting.AccessLog
		select * into Reporting.AccessLog from Reporting.[Cache-AccessLog] with (nolock)
		drop table JobTitleToRole
		select * into JobTitleToRole from [Cache-JobTitleToRole] with (nolock)
		drop table DataQuality.ScoresPersonalContacts
		select * into DataQuality.ScoresPersonalContacts from DataQuality.[Cache-ScoresPersonalContacts]
		drop table DataQuality.Scores
		select * into DataQuality.Scores from DataQuality.[Cache-Scores]


		drop table PatientLatestApproverContactInfo
		select * into PatientLatestApproverContactInfo from [Cache-PatientLatestApproverContactInfo]
		exec DePivotDateRanges

		PRINT 'End Section NINE'
		PRINT GETDATE()






END
