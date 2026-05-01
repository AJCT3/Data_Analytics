

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

alter PROCEDURE [dbo].[AT_NewCacheUpdated_Pt_1d]

AS

BEGIN



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


		WAITFOR DELAY '00:03:00' -- WAIT EIGHT after incremental builds

		DROP TABLE [Reporting].[CCGActivity]
		select * into [Reporting].[CCGActivity] from [Reporting].[Cache-CCGActivity] with (nolock)
		DROP TABLE [Reporting].[CCGActivity24]
		select * into [Reporting].[CCGActivity24] from [Reporting].[Cache-CCGActivity24] with (nolock)

		WAITFOR DELAY '00:03:00' -- WAIT EIGHT after incremental builds

		-- Added 10/08/2018 - table used for the new Data Overview report
		DROP TABLE [Reporting].[CCGActivity24_ForDataOverview]
		select * into [Reporting].[CCGActivity24_ForDataOverview] from [Reporting].[Cache-CCGActivity24_ForDataOverview] with (nolock)
		-- Added 18/04/2018 - table used by new Activity Report
		DROP TABLE [Reporting].[CCGActivityAllData]
		select * into [Reporting].[CCGActivityAllData] from [Reporting].[Cache-CCGActivityAllData] with (nolock)

		 
		 WAITFOR DELAY '00:03:30' -- WAIT EIGHT after incremental builds

		 DROP TABLE [Reporting].[CCGActivityAllData_ActivityByMonthReport]
		 select * into [Reporting].[CCGActivityAllData_ActivityByMonthReport] from [Reporting].[Cache-CCGActivityAllData_ActivityByMonthReport] with (nolock)
		--

		WAITFOR DELAY '00:03:30' -- WAIT EIGHT after incremental builds

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