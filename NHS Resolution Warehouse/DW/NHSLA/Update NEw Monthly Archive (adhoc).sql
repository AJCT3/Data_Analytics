 

 
 

	
	
	ALTER INDEX ClaimsArchiveIndex ON [NHR Data Mart Test].[dbo].[ClaimGeneral_Monthly_Snapshot] DISABLE
	go
	
	declare @MonthEnd  date
 
	set @MonthEnd =  DATEADD(ss, -1, DATEADD(month, DATEDIFF(month, 0, getdate()), 0))
 
	--print @MonthEnd



	 if OBJECT_ID('tempdb..#temp1') is not null
		drop table #temp1
		select 
		*
		into #Temp1
		from [ldndw1\reporting].[Informatics_Reporting].[etl].[vw_ClaimGeneral_Archive2] with (nolock) 

    if OBJECT_ID('tempdb..#temp2') is not null
		drop table #temp2
		select 
		*
		into #Temp2
		from [ldndw1\reporting].[Informatics_Reporting].[etl].[vw_ClaimRiskCategory_Archive2] with (nolock) 

 

 		--DROP INDEX ClaimsArchiveIndex   
  --  ON [NHR Data Mart Test].[dbo].[ClaimGeneral_Monthly_Snapshot] 

-- CREATE INDEX ClaimsArchiveIndex   
--		ON [NHR Data Mart Test].[dbo].[ClaimGeneral_Monthly_Snapshot] (Claimid, SnapshotMonth)   
--		INCLUDE (CMSTimeStamp)
		
--		CREATE NONCLUSTERED COLUMNSTORE INDEX ClaimsArchiveIndex ON [NHR Data Mart Test].[dbo].[ClaimGeneral_Monthly_Snapshot]
--(
--	[SettlementDate],
--	[IncidentDate],
--	[CreationDate],
--	[CloseDate],
--	SnapshotMonth,
--	[DamagesPaid],
--	[DefenceCostsPaid],
--	[ClaimantCostsPaid],
--	[TotalPaid],
--	[MemberCode] 
--)WITH (DROP_EXISTING = ON) ON [PRIMARY]

  
-- update mytable --  

 IF EXISTS (select top 1 SnapshotMonth from [NHR Data Mart Test].[dbo].[ClaimGeneral_Monthly_Snapshot] where SnapshotMonth = @MonthEnd)
	begin 

 
		Delete from [NHR Data Mart Test].[dbo].[ClaimGeneral_Monthly_Snapshot] where SnapshotMonth = @MonthEnd
 
 
	 
	insert into [NHR Data Mart Test].[dbo].[ClaimGeneral_Monthly_Snapshot]
		select   
 
	  a.[ClaimId]
    ,a.[ClaimRef]
    ,[OldRef]
    ,[ClaimantFirstName]
    ,[ClaimantSurname]
    ,[PatientFirstName]
    ,[PatientSurname]
    ,[ScheduleName]
    ,[ClaimStatusTypeName]
    ,[CurrentStatusType]
    ,[StatusDateChange]
    ,[SettlementDate]
    ,[IncidentDate]
    ,[NotificationDate]
    ,[DateOfEntry]
    ,[CreationDate]
    ,[OpenDate]
    ,[CloseDate]
    ,[Void Claim Flag]
    ,[SchemeAbbrev]
    ,[SchemeAbbrev adj]
    ,[Clinical / Non Clinical]
    ,[Successful/Unsuccessful]
    ,[Closure Date (Settlement Year for PPOs)] as [Closure Date (Settlement Date for PPOs)]
    ,[MemberCode]
    ,[MemberName]
    ,[MemberRef]
    ,[GroupedClaimsCode]
    ,[GroupedClaimsDescription]
    ,[InternalGroupedClaimsCode]
    ,[InternalGroupedClaimsDescription]
    ,[SiteCode]
    ,[SiteName]
    ,[EstSettlementDate]
    ,[IncidentDescription]
    ,[UserWhoEnteredFirstName]
    ,[UserWhoEnteredSurname]
    ,[HandlerFirstName]
    ,[HandlerSurname]
    ,[TotalClaim]
    ,[TotalOSEstimate]
    ,[OSDamages]
    ,[OSDefenceCosts]
    ,[OSPlaintifCosts]
    ,[TotalPaid]
    ,[DamagesPaid]
    ,[DefenceCostsPaid]
    ,[ClaimantCostsPaid]
    ,[NHSLAPayments]
    ,[MemberPayments]
    ,[NHSLADefenceCostsPaid]
    ,[ApplicableExcess]
    ,[Actual Excess (RPST Only)]
    ,[NHSLAFunded]
    ,[InquestCosts]
    ,[Mediation]
    ,[Probability]
    ,[PercentageShare]
    ,[DefenceSolicitorId]
    ,[SolicitorDescription]
    ,[SolicitorRef]
    ,[DefenceSolicitorName]
    ,[ClaimantSolicitorOrganisationId]
    ,[ClaimantSolicitor]
    ,[PendingFirstReserve]
    ,[PortalClaim]
    ,[ExitReason]
    ,[ExitComments]
    ,[ExitedBy]
    ,[ClaimType]
	,[Cause1L1]
	,[Cause1L2]
	,[Cause1L3]
	,[Cause2L1]
	,[Cause2L2]
	,[Cause2L3]
	,[Cause3L1]
	,[Cause3L2]
	,[Cause3L3]
	,[Injury1L1]
	,[Injury1L2]
	,[Injury1L3]
	,[Injury2L1]
	,[Injury2L2]
	,[Injury2L3]
	,[Injury3L1]
	,[Injury3L2]
	,[Injury3L3]
	,[Location1L1]
	,[Location1L2]
	,[Location1L3]
	,[Location2L1]
	,[Location2L2]
	,[Location2L3]
	,[Location3L1]
	,[Location3L2]
	,[Location3L3]
	,[RootCause1L1]
	,[RootCause1L2]
	,[RootCause1L3]
	,[RootCause2L1]
	,[RootCause2L2]
	,[RootCause2L3]
	,[RootCause3L1]
	,[RootCause3L2]
	,[RootCause3L3]
	,[Specialty]
	,[Speciality1L1]
	,[Speciality1L2]
	,[Speciality1L3]
	,[Speciality2L1]
	,[Speciality2L2]
	,[Speciality2L3]
	,[Speciality3L1]
	,[Speciality3L2]
	,[Speciality3L3]
	,[Standard1L1]
	,[Standard1L2]
	,[Standard1L3]
	,[Standard2L1]
	,[Standard2L2]
	,[Standard2L3]
	,[Standard3L1]
	,[Standard3L2]
	,[Standard3L3]
	,[DefaultRiskCategoryOrdering]
	,@MonthEnd  as SnapshotMonth

	,a.CMSTimestamp + b.CMSTimestamp  as CMSTimeStamp
 
	

from 
[#Temp1]a
left join[#Temp2]b on b.claimid = a.claimid



End
	Else
	Begin

	 

	insert into [NHR Data Mart Test].[dbo].[ClaimGeneral_Monthly_Snapshot]
	select
	 a.[ClaimId]
    ,a.[ClaimRef]
    ,[OldRef]
    ,[ClaimantFirstName]
    ,[ClaimantSurname]
    ,[PatientFirstName]
    ,[PatientSurname]
    ,[ScheduleName]
    ,[ClaimStatusTypeName]
    ,[CurrentStatusType]
    ,[StatusDateChange]
    ,[SettlementDate]
    ,[IncidentDate]
    ,[NotificationDate]
    ,[DateOfEntry]
    ,[CreationDate]
    ,[OpenDate]
    ,[CloseDate]
    ,[Void Claim Flag]
    ,[SchemeAbbrev]
    ,[SchemeAbbrev adj]
    ,[Clinical / Non Clinical]
    ,[Successful/Unsuccessful]
    ,[Closure Date (Settlement Year for PPOs)] as [Closure Date (Settlement Date for PPOs)]
    ,[MemberCode]
    ,[MemberName]
    ,[MemberRef]
    ,[GroupedClaimsCode]
    ,[GroupedClaimsDescription]
    ,[InternalGroupedClaimsCode]
    ,[InternalGroupedClaimsDescription]
    ,[SiteCode]
    ,[SiteName]
    ,[EstSettlementDate]
    ,[IncidentDescription]
    ,[UserWhoEnteredFirstName]
    ,[UserWhoEnteredSurname]
    ,[HandlerFirstName]
    ,[HandlerSurname]
    ,[TotalClaim]
    ,[TotalOSEstimate]
    ,[OSDamages]
    ,[OSDefenceCosts]
    ,[OSPlaintifCosts]
    ,[TotalPaid]
    ,[DamagesPaid]
    ,[DefenceCostsPaid]
    ,[ClaimantCostsPaid]
    ,[NHSLAPayments]
    ,[MemberPayments]
    ,[NHSLADefenceCostsPaid]
    ,[ApplicableExcess]
    ,[Actual Excess (RPST Only)]
    ,[NHSLAFunded]
    ,[InquestCosts]
    ,[Mediation]
    ,[Probability]
    ,[PercentageShare]
    ,[DefenceSolicitorId]
    ,[SolicitorDescription]
    ,[SolicitorRef]
    ,[DefenceSolicitorName]
    ,[ClaimantSolicitorOrganisationId]
    ,[ClaimantSolicitor]
    ,[PendingFirstReserve]
    ,[PortalClaim]
    ,[ExitReason]
    ,[ExitComments]
    ,[ExitedBy]
    ,[ClaimType]
	,[Cause1L1]
	,[Cause1L2]
	,[Cause1L3]
	,[Cause2L1]
	,[Cause2L2]
	,[Cause2L3]
	,[Cause3L1]
	,[Cause3L2]
	,[Cause3L3]
	,[Injury1L1]
	,[Injury1L2]
	,[Injury1L3]
	,[Injury2L1]
	,[Injury2L2]
	,[Injury2L3]
	,[Injury3L1]
	,[Injury3L2]
	,[Injury3L3]
	,[Location1L1]
	,[Location1L2]
	,[Location1L3]
	,[Location2L1]
	,[Location2L2]
	,[Location2L3]
	,[Location3L1]
	,[Location3L2]
	,[Location3L3]
	,[RootCause1L1]
	,[RootCause1L2]
	,[RootCause1L3]
	,[RootCause2L1]
	,[RootCause2L2]
	,[RootCause2L3]
	,[RootCause3L1]
	,[RootCause3L2]
	,[RootCause3L3]
	,[Specialty]
	,[Speciality1L1]
	,[Speciality1L2]
	,[Speciality1L3]
	,[Speciality2L1]
	,[Speciality2L2]
	,[Speciality2L3]
	,[Speciality3L1]
	,[Speciality3L2]
	,[Speciality3L3]
	,[Standard1L1]
	,[Standard1L2]
	,[Standard1L3]
	,[Standard2L1]
	,[Standard2L2]
	,[Standard2L3]
	,[Standard3L1]
	,[Standard3L2]
	,[Standard3L3]
	,[DefaultRiskCategoryOrdering]
	,@MonthEnd  as SnapshotMonth

	,a.CMSTimestamp + b.CMSTimestamp  as CMSTimeStamp

	

from 
[#Temp1]a
left join[#Temp2]b on b.claimid = a.claimid

 
end

	ALTER INDEX ClaimsArchiveIndex ON [NHR Data Mart Test].[dbo].[ClaimGeneral_Monthly_Snapshot] REBUILD

	go