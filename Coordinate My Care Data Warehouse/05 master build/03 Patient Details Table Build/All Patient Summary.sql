
USE [ETL_Local_PROD]
GO
/****** Object:  StoredProcedure [dbo].[AT__Activity_Data]    Script Date: 25/10/2021 14:06:02 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




create PROCEDURE [dbo].[AT__All_Patient_Summary] 
-- Amended for PD Upgrade
AS
BEGIN


			IF OBJECT_ID('tempdb..#CPR') IS NOT NULL 
			dROP TABLE #CPR
			SELECT  
			[ItemId]
			,b.GenusId as  CMC_ID
			,b.VersionNumber
			,ROW_NUMBER() over (order by b.GenusId, b.VersionNumber ) as id
			,dense_rank() over (partition by b.GenusId order by b.VersionNumber ) as PRoRn
			,null as CPR_VersionNumber
			,null as FirstVersionNumber
			,null as LastVErsionFlag
			,c.[Description] as [CPR Decision]
			,[DecisionTime]
 
			 into #CPR

			  FROM [ETL_PROD].[dbo].[CMC_CPR]a
			  inner join [ETL_PROD].[dbo].[CMC_PatientSummary_CarePlan] b on b.PatientSummary = a.ItemId
			  left join [ETL_PROD].[dbo].[Coded_CPRDecision]c on c.Code = a.Decision
 

				update r

				set r.CPR_VersionNumber = s.CPR_VersionNumber

				from #CPR r
				left join
				(
				select
				[CMC_ID],
				[PRoRn],
				dense_rank() over (partition by [CMC_ID] order by [PRoRn]) as CPR_VersionNumber
				from #CPR
				)s on s.CMC_ID = r.CMC_ID and s.PRoRn = r.PRoRn


	 

				update f
				set f.firstVErsionNumber = 1
				from #CPR f
				where f.CPR_VersionNumber = 1
	 
	 
				update g

				set g.LastVErsionFlag = 1

				from #CPR g
				inner join
				(
				select 
				CMC_ID,
				max(VersionNumber) as LastVersion
				from #CPR
				group by
				CMC_ID
				)h on h.CMC_ID = g.CMC_ID
				and h.LastVersion = g.VersionNumber
 



  --ceiling of treatment

			IF OBJECT_ID('tempdb..#CoT') IS NOT NULL 
			dROP TABLE #CoT

			SELECT 
			[ItemId]
			,[ADRTExists]
			,bb.GenusId as  CMC_ID
			,bb.VersionNumber
			,ROW_NUMBER() over (order by bb.GenusId, bb.VersionNumber ) as id
			,dense_rank() over (partition by bb.GenusId order by bb.VersionNumber ) as PRoRn
			,null as CPR_VersionNumber
			,null as FirstVersionNumber
			,null as LastVErsionFlag
			,b.Description as  [LevelOfTrtmnt]
			,[WHOPerf]
			,[WHOPerfTime]
			,[SensitiveDetails]

			into #CoT

			FROM [ETL_PROD].[dbo].[CMC_MedicalBackground]a
			left join [ETL_PROD].[dbo].[Coded_LevelOfTrtmnt]b on b.Code = a.[LevelOfTrtmnt]
			inner join [ETL_PROD].[dbo].[CMC_PatientSummary_CarePlan] bb on bb.PatientSummary = a.ItemId
	 

				update r

				set r.CPR_VersionNumber = s.CPR_VersionNumber

				from #CoT r
				left join
				(
				select
				[CMC_ID],
				[PRoRn],
				dense_rank() over (partition by [CMC_ID] order by [PRoRn]) as CPR_VersionNumber
				from #CoT
				)s on s.CMC_ID = r.CMC_ID and s.PRoRn = r.PRoRn


	 

				update f
				set f.firstVErsionNumber = 1
				from #CoT f
				where f.CPR_VersionNumber = 1
	 
	 
				update g

				set g.LastVErsionFlag = 1

				from #CoT g
				inner join
				(
				select 
				CMC_ID,
				max(VersionNumber) as LastVersion
				from #CoT
				group by
				CMC_ID
				)h on h.CMC_ID = g.CMC_ID
				and h.LastVersion = g.VersionNumber
 

 ------------------------------------------------------------------------------------------------------
 --medical background
 			IF OBJECT_ID('tempdb..#MB') IS NOT NULL 
			dROP TABLE #MB
 
 
 select
		d.CMC_ID
		,y.Description as[ADTR Exists]
		--,[ADRTExists]
      ,[ADRTDetails]
      ,z.description as [HaveDisability]
      ,[DisabilityDetails]
      ,az.description as [FamilyAwareDiagnosis]
      ,[FamilyAwareDiagDetails]
      ,[LevelOfTrtmnt]
      ,[OtherSignifHx]
      ,bz.description as [PatientAwareDiagnosis]
      ,[PatientAwareDiagDetails]
      ,[WHOPerf]
      ,[WHOPerfTime]
      ,[LevelOfTrtmntDetails]
      --,[CCItemID]
      ,[SensitiveDetails]
	  into #MB
		from
		(
		
		SELECT  
		distinct
		cast(LEFT(REPLACE(a.[ItemId],'PS||',''), CHARINDEX('||', REPLACE(a.[ItemId],'PS||','')) - 1)as Nvarchar(75)) as CMC_ID
		,ROW_NUMBER() over ( partition by cast(LEFT(REPLACE(a.[ItemId],'PS||',''), CHARINDEX('||', REPLACE(a.[ItemId],'PS||','')) - 1)as Nvarchar(75)) order by a.[ItemId]  ) as id
      ,[ADRTExists]
      ,[ADRTDetails]
      ,[HaveDisability]
      ,[DisabilityDetails]
      ,[FamilyAwareDiagnosis]
      ,[FamilyAwareDiagDetails]
      ,[LevelOfTrtmnt]
      ,[OtherSignifHx]
      ,[PatientAwareDiagnosis]
      ,[PatientAwareDiagDetails]
      ,[WHOPerf]
      ,[WHOPerfTime]
      ,[LevelOfTrtmntDetails]
      --,[CCItemID]
      ,[SensitiveDetails]
	  --,c.Description as Disability
  FROM [ETL_PROD].[dbo].[CMC_MedicalBackground]a

  )d


  inner join 
			(
			select
			cmc_id,
			max(id) as LastID
			from
			(
			SELECT  
		distinct
		cast(LEFT(REPLACE(a.[ItemId],'PS||',''), CHARINDEX('||', REPLACE(a.[ItemId],'PS||','')) - 1)as Nvarchar(75)) as CMC_ID
		,ROW_NUMBER() over ( partition by cast(LEFT(REPLACE(a.[ItemId],'PS||',''), CHARINDEX('||', REPLACE(a.[ItemId],'PS||','')) - 1)as Nvarchar(75)) order by a.[ItemId]  ) as id
		  FROM [ETL_PROD].[dbo].[CMC_MedicalBackground]a
		  )d group by cmc_id
			)x on x.CMC_ID = d.CMC_ID
			and x.LastID = d.id 
	left join [ETL_PROD].[dbo].[Coded_ADRTExists]y on y.Code = d.ADRTExists	 
	left join [ETL_PROD].[dbo].[Coded_HaveDisability]z on z.code = d.HaveDisability
    left join [ETL_PROD].[dbo].[Coded_FamilyAwareDiagnosis]az on az.Code = d.FamilyAwareDiagnosis
	left join [ETL_PROD].[dbo].[Coded_PatientAwareDiagnosis]bz on bz.Code = d.PatientAwareDiagnosis
 ------------------------------------------------------------------------------------------------------------
 --care package
		IF OBJECT_ID('tempdb..#CarePkg') IS NOT NULL 
			dROP TABLE #CarePkg

SELECT  
cast(LEFT(REPLACE(a.[ItemId],'PS||',''), CHARINDEX('||', REPLACE(a.[ItemId],'PS||','')) - 1)as Nvarchar(75)) as CMC_ID
 ,[ItemId]
 , convert(int,replace( right(a.[ItemId], charindex('||', a.[ItemId]) - 1),'|','')) as CPNo
 ,null as LastRecordFlag
      ,b.description as [DS1500]
      ,c.description as [Patient Received Careplan Package]
      ,[PatientReceiptNotes]
      ,d.description as [Equipment]
      ,[EquipmentNotes] as [Notes on Equipment]
      ,e.description as [Homecare Help Recieved]
      ,[HomecareHelpNotes] as [Details of Homecare]
      ,f.description as [Patient has Family Support]
      ,[FamilySupportNotes] as  [Family Support Notes]
 into #CarePkg
  FROM [ETL_PROD].[dbo].[CMC_CarePackage]a
  left join [ETL_PROD].[dbo].[Coded_DS1500]b on b.Code = a.DS1500
  left join [ETL_PROD].[dbo].[Coded_PatientReceipt]c on c.Code = a.PatientReceipt
  left join [ETL_PROD].[dbo].[Coded_Equipment]d on d.Code = a.Equipment
  left join [ETL_PROD].[dbo].[Coded_HomecareHelp]e on e.code = a.HomecareHelp
  left join [ETL_PROD].[dbo].[Coded_FamilySupport]f on f.code = a.[FamilySupport]

  update c
  
		set c.LastRecordFlag = 1
  
  from #CarePkg c
  inner join
			(
				select 
				cmc_id,
				Max(CPno) as LastCP
				 from #CarePkg
				 group by
				 cmc_id
				 )d on d.CMC_ID = c.CMC_ID
				 and d.LastCP = c.CPNo

				 --select * from #CarePkg order by CMC_ID,CPNo

				 delete from #CarePkg where LastRecordFlag is null
 ------------------------------------------------------------------------------------------------------------------------------------------------
			IF OBJECT_ID('tempdb..#PPD') IS NOT NULL 
			dROP TABLE #PPD

			SELECT 

			bb.GenusId as  CMC_ID
			,bb.VersionNumber
 
			,ROW_NUMBER() over (order by bb.GenusId, bb.VersionNumber ) as id
			,dense_rank() over (partition by bb.GenusId order by bb.VersionNumber ) as PRoRn
			,null as CPR_VersionNumber
			,null as FirstVersionNumber
			,null as LastVErsionFlag
			,[PatientWishes]
			,[FamilyAwarePref]
			,d.description as [OrganDonat]
			,[OrganDonatDet]
			,[CulturalRelNeeds]

			into #PPD

			FROM [ETL_PROD].[dbo].[CMC_PatientPreference]a
			inner join [ETL_PROD].[dbo].[CMC_PatientSummary_CarePlan] bb on bb.PatientSummary = a.ItemId
 
			left join [ETL_PROD].[dbo].[Coded_OrganDonat]d on d.Code = a.OrganDonat


					update r

					set r.CPR_VersionNumber = s.CPR_VersionNumber

					from #PPD r
					left join
					(
					select
					[CMC_ID],
					[PRoRn],
					dense_rank() over (partition by [CMC_ID] order by [PRoRn]) as CPR_VersionNumber
					from #PPD
					)s on s.CMC_ID = r.CMC_ID and s.PRoRn = r.PRoRn


	 

					update f
					set f.firstVErsionNumber = 1
					from #PPD f
					where f.CPR_VersionNumber = 1
	 
	 
					update g

					set g.LastVErsionFlag = 1

					from #PPD g
					inner join
					(
					select 
					CMC_ID,
					max(VersionNumber) as LastVersion
					from #PPD
					group by
					CMC_ID
					)h on h.CMC_ID = g.CMC_ID
					and h.LastVersion = g.VersionNumber
 

  
 

	
	

		IF OBJECT_ID('tempdb..#TempOrg2') IS NOT NULL 
		dROP TABLE #TempOrg2
		SELECT   
		distinct

	   cast(null as varchar(50)) as ODS
	   ,cast(null as varchar(100)) as [Team or GP]
	    ,cast(null as varchar(max)) as [CMC_Team_Name]
	 ,null as EnterpriseID
	   ,cast(null as varchar(32))as Type
	   ,cast(null as varchar(255)) as NEwCCG
	   ,cast(null as varchar(255)) as OriginalCCG
	,CMC_ID
	,convert(date,coalesce(dod,dod_pds))  as DateOfDeath
	,convert(date,Date_Original_Approval) as Date_Original_Approval
	  ,b.CURRENT_ADDRESS
	  ,b.CURRENT_POSTCODE
		,[PRIMARY_POSTCODE] 
		,b.PRIMARY_ADDRESS
      ,b.GP_Practice
	  ,b.GPODSCode
	  ,b.GP_EnterpriseID 
	 --,c.PCNName as GPPCNName
	 --,c.PCNCode as GPPCNCODE
	 ,cast(null as varchar(255)) as GPPCNName
	 ,cast(null as varchar(255)) as GPPCNCODE
	  ,cast(null as varchar(255)) as STP
	  ,cast(null as varchar(55)) as PCN
      ,cast(null as varchar(max)) as [Ward]
 
	  ,cast(null as varchar(50)) as PostCode
	   ,cast(null as varchar(max)) as Region

	  into #tempORG2
  FROM [ETL_Local_PROD].[dbo].[AT_Patient_General]b  
  
  --left join [ETL_Local_PROD].[dbo].[AT_Commissioners_Report_Primary_Care_Network_Core_Partners]c on c.PartnerOrganisationCode = b.GPODSCode and ([Practice to PCNRelationshipEnd Date] is null or [Practice to PCNRelationshipEnd Date] < convert(date,Date_Original_Approval))
  --inner join  [ETL_Local_PROD].[dbo].[AT_PD_Dept]c on c.DeptEnterpriseID = f.deptenterpriseid
	 




	update b

		set b.GPPCNName = c.PCNName,
			b.GPPCNCODE = c.PCNCode


	from #tempORG2 b
	left join [ETL_Local_PROD].[dbo].[AT_Commissioners_Report_Primary_Care_Network_Core_Partners]c on c.PartnerOrganisationCode = b.GPODSCode 
																									and ([Practice to PCNRelationshipEnd Date] is null or [Practice to PCNRelationshipEnd Date] <  Date_Original_Approval)
																									and [Practice to PCNRelationshipStart Date] <= Date_Original_Approval

 
	
	update a
			set a.NEwCCG = coalesce(t.[GP CCG],y.[GP CCG]),
				--a.OriginalCCG = coalesce(z.ccg,s.ccg,x.ccg),
				a.ods = coalesce(t.ODS,y.ods),
				a.[Team or GP] = coalesce(t.[Team or GP],y.[Team or GP]),
				a.Type = coalesce(t.Type,y.type),
				a.PostCode =   coalesce(t.postcode,y.postcode,null) 
				
				--,
				--a.Ward = coalesce(z.[ward name],s.[ward name],x.[ward name])

	from #tempORG2 a
	left join [ETL_Local_PROD].[ODSData].[searchods]t on t.Postcode = a.[PRIMARY_POSTCODE] and ( t.Type = 'Care Home'OR t.Type = 'Care Home HQ' ) and t.[GP CCG] is not null and t.[Close Date] is  null
	left join [ETL_Local_PROD].[ODSData].[searchods]y on y.Postcode = a.CURRENT_POSTCODE and ( y.Type = 'Care Home'OR y.Type = 'Care Home HQ' ) and y.[GP CCG] is not null and y.[Close Date] is  null
	--left join [ETL_Local_PROD].[dbo].[AT_ENGLISH_POSTCODE_DATA]z on z.PCDS = coalesce(t.postcode,y.postcode)
	--left join [ETL_Local_PROD].[dbo].[AT_ENGLISH_POSTCODE_DATA]s on s.PCDS = a.PRIMARY_POSTCODE 
	--left join [ETL_Local_PROD].[dbo].[AT_ENGLISH_POSTCODE_DATA]x on x.PCDS = a.CURRENT_POSTCODE

	--select * from [ETL_Local_PROD].[ODSData].[searchods]
	
	update a
			set  a.OriginalCCG = coalesce(z.ccg,s.ccg,x.ccg),
			    a.Ward = coalesce(z.[ward name],s.[ward name],x.[ward name]),
				a.Region = coalesce(z.[NHS England REgion],s.[NHS England REgion],x.[NHS England REgion])

	from #tempORG2 a
 
	left join [ETL_Local_PROD].[dbo].[AT_ENGLISH_POSTCODE_DATA]z on z.PCDS = a.PostCode
	left join [ETL_Local_PROD].[dbo].[AT_ENGLISH_POSTCODE_DATA]s on s.PCDS = a.PRIMARY_POSTCODE 
	left join [ETL_Local_PROD].[dbo].[AT_ENGLISH_POSTCODE_DATA]x on x.PCDS = a.CURRENT_POSTCODE



	

	update r

		set r.[Team or GP] = 'ELMWOOD NURSING HOME',
		r.Type = 'Care Home',
		r.ods = 'VM028',
		r.PostCode = 'CR0 2SG'

	from #tempORG2 r
	inner join [ETL_Local_PROD].[dbo].[AT_Patient_General]b on b.CMC_ID = r.CMC_ID
	where charindex('Elmwood Nursing Home',r.PRIMARY_ADDRESS)>0
	or charindex('Elmwood Nursing Home',b.current_ADDRESS)>0
	 
 
 
 update x
		set x.NEwCCG = s.NEW_CCG_Pseudo
 from #tempORG2 x
 left join [ETL_Local_PROD].[Reference].[STP]s on s.CCGLONGNAME = x.OriginalCCG
 
 

  update x
		set x.NEwCCG = ltrim(rtrim(REplace( x.NEwCCG, ' CCG', ''))) ,
			x.OriginalCCG = ltrim(rtrim(REplace( x.OriginalCCG, ' CCG', ''))) 
 from #tempORG2 x





update a

	set a.[Team or GP] = b.[Provider Name],
		a.Type = 'Care Home',
		a.Region = b.[Provider Region],
		a.ODS = b.[Location ODS Code],
		a.PostCode = b.[Postal Code]
	
  from #tempORG2 a
  left join [ETL_Local_PROD].[Load].[CQCDirectory]b on b.[Postal Code] = coalescE(a.Primary_POSTCODE,a.CURRENT_POSTCODE)
  where [Team or GP] is null
  and b.[Care home?] = 'Y'
  and b.[Location ODS Code] is not null

  update e
		set e.EnterpriseID = coalesce(c.EnterpriseID,d.EnterpriseID),
			e.CMC_Team_Name = coalesce(c.Team,d.team)
 
 from #tempORG2 e
left join [ETL_Local_PROD].[dbo].[AT_Organisation_to_Department_Provider_Directory]c on c.DeptODSCode = e.ODS
left join [ETL_Local_PROD].[dbo].[AT_Organisation_to_Department_Provider_Directory]d on d.PostCode = e.PostCode and d.OrgType <> 'General Practice'


delete  from #tempORG2 where type is null


--select * from #tempORG2
	 
 --Planned REviewer
 ---------------------------------------------------------------------------------------------------------------------------------------
 		
		
			IF OBJECT_ID('tempdb..#PlannedReviewer') IS NOT NULL 
			dROP TABLE #PlannedReviewer
			SELECT  
			cast(LEFT(REPLACE(a.[ItemId],'CarePlan||',''), CHARINDEX('||', REPLACE(a.[ItemId],'CarePlan||','')) - 1)as Nvarchar(75)) as CMC_ID
			,ROW_NUMBER() over ( partition by cast(LEFT(REPLACE(a.[ItemId],'CarePlan||',''), CHARINDEX('||', REPLACE(a.[ItemId],'CarePlan||','')) - 1)as Nvarchar(75)) order by cast(LEFT(REPLACE(a.[ItemId],'CarePlan||',''), CHARINDEX('||', REPLACE(a.[ItemId],'CarePlan||','')) - 1)as Nvarchar(75))  ,  [PlannedReviewTime]) as id
			,null as LastRowFlag
			,a.[ItemId]
			,[Identifier]
			,[Type]
    
			,[PlannedReviewer]
			,[PlannedReviewTime]
			,[Comments]
			,[DateLastSaved]
			,a.[CCItemID]
			,[ROWKeyfield]
			,[LastClinicalApprovalTime]
			,[LastClinicalApprovalUser]
			,coalesce(C.StaffTitleDescription,'') +' '+ coalesce(StaffForename,'') +' '+ coalesce(StaffSurname,'') AS sTAFFnAME
			,c.StaffProviderTypeDescription
			into #PlannedReviewer
			FROM [ETL_PROD].[dbo].[CMC_CarePlan]a
			inner join [ETL_PROD].[dbo].[CMC_IndividualProvider]b on b.[ItemId] = a.PlannedReviewer
			left join [ETL_Local_PROD].[dbo].[AT_Staff]c on c.StaffRegistryId = b.RegistryID
 


			update c

			set c.LastRowFlag = 1

			from #PlannedReviewer  c
			inner join 
			(
			select
			cmc_id,
			max(id) as LastID
			from #PlannedReviewer
			group by
			CMC_ID
			)D on d.CMC_ID = c.CMC_ID and d.LastID = c.id
 
 
 ---------------------------------------------------------------------------------------------------------------------------------- 
 --address details
 		IF OBJECT_ID('tempdb..#DT') IS NOT NULL 
		dROP TABLE #DT
 SELECT  [ItemId]
   ,LEFT([ItemId], len([ItemId]) -3)as ItemID_Shaved
      ,[PostalCode]
	,Line2
      ,[PDDeleted]
      ,c.Description as   [Address Used]
      ,b.Description as	  [Dwelling Type]
 
      ,d.description as	  [CCLivingConditions]
      ,[CCKeySafeDetails]
      ,[CCResidenceNotes]
	into #DT
  FROM [ETL_PROD].[dbo].[CMC_Address]a
  left join [ETL_PROD].[dbo].[Coded_DwellingType]b on b.Code = a.CCDwellingType
  left join [ETL_PROD].[dbo].[Coded_AddressUse]c on c.Code = a.CCAddressUse
  left join [ETL_PROD].[dbo].[Coded_LivingConditions]d on d.Code = a.CCLivingConditions


----------------------------------------------------------------------------------------------------------------------------------------
		IF OBJECT_ID('tempdb..#Consent') IS NOT NULL 
			dROP TABLE #Consent


SELECT   

	cast(LEFT(REPLACE(a.[ItemId],'PS||',''), CHARINDEX('||', REPLACE(a.[ItemId],'PS||','')) - 1)as Nvarchar(75)) as CMC_ID,
a.[ItemId]
 , convert(int,replace( right(a.[ItemId], charindex('||', a.[ItemId]) - 1),'|','')) as CPNo
 ,null as LastRecordFlag
      ,Translation
	  ,null as TestPat
	  ,c.Description
	  ,d.Report
      ,cast(null as date) as DateOfBirth
	  ,cast(null as date) as DateOfDeath
      ,[DateObtained]
      ,[FromTime]
      ,[ToTime]
      --,[POADocLocation]
      ,[Clinician]
      --,[Comments]
	  ,null as currentAge
	  ,null as ageatDateAdded
	  ,cast(null as varchar(20)) as AgeGroupAtAdd
	   ,cast(null as varchar(20)) as AgeGroupCurrent
	 --,e.GP_Practice
	  ,[ProfessionalSuffix]+' '+[GivenName]+' '+[FamilyName]as ConsentProviderName
 into #Consent
  FROM [ETL_PROD].[dbo].[CMC_Consent]a
  inner join [ETL_PROD].[dbo].[CMC_IndividualProvider]b on b.ItemId = a.Clinician
  inner join [ETL_PROD].[dbo].[Coded_ConsentType]c on c.Code = a.Type
  left join [ETL_Local_PROD].[dbo].[AT_Consent_Lookup]d on d.Code = a.Type
  --left join [ETL_Local_PROD].[dbo].[AT_Patient_General]e on e.CMC_ID = cast(LEFT(REPLACE(a.[ItemId],'PS||',''), CHARINDEX('||', REPLACE(a.[ItemId],'PS||','')) - 1)as Nvarchar(75))
  --left join [ETL_Local_PROD].[dbo].[AT_Patient_General]e on e.PatientSummary = a.ItemId
  --where charindex('test',e.GP_Practice) = 0
 
  update c
  
		set c.LastRecordFlag = 1
  
  from #Consent c
  inner join
			(
				select 
				cmc_id,
				Max(CPno) as LastCP
				 from #Consent
				 group by
				 cmc_id
				 )d on d.CMC_ID = c.CMC_ID
				 and d.LastCP = c.CPNo



 


update x
		set  currentAge = DATEDIFF(YY,dob,getdate())-
							  case
								when dateadd(YY,datediff(YY,DOB,getdate()),DOB)
								> getdate() then 1
								ELSE 0
							end,
			ageatDateAdded = DATEDIFF(YY,dob,[Add_Date])-
		  case
			when dateadd(YY,datediff(YY,DOB,[Add_Date]),DOB)
			> [Add_Date] then 1
			ELSE 0
		end,
		DateOfBirth = dod,
		DateOfDeath = coalesce(DoD_PDS,dod)

from #Consent x
inner join [ETL_Local_PROD].[dbo].[AT_Patient_General]e on e.CMC_ID = x.CMC_ID




update x
	
	set x.AgeGroupAtAdd = CASE 
							   WHEN  ageatDateAdded < 18 THEN '0-17'
							   WHEN ageatDateAdded >= 18 AND ageatDateAdded < 40 THEN '18-39'
							   WHEN ageatDateAdded >= 40 AND ageatDateAdded < 50 THEN '40-49'
							   WHEN ageatDateAdded >= 50 AND ageatDateAdded < 60 THEN '50-59'
							   WHEN ageatDateAdded >= 60 AND ageatDateAdded < 70 THEN '60-69'
							   WHEN ageatDateAdded >= 70 AND ageatDateAdded < 80 THEN '70-79'
								ELSE '80+' 
						  END,
		x.AgeGroupCurrent = CASE 
							   WHEN  currentAge < 18 THEN '0-17'
							   WHEN currentAge >= 18 AND currentAge < 40 THEN '18-39'
							   WHEN currentAge >= 40 AND currentAge < 50 THEN '40-49'
							   WHEN currentAge >= 50 AND currentAge < 60 THEN '50-59'
							   WHEN currentAge >= 60 AND currentAge < 70 THEN '60-69'
							   WHEN currentAge >= 70 AND currentAge < 80 THEN '70-79'
								ELSE '80+' 
						  END


from #Consent x
inner join [ETL_Local_PROD].[dbo].[AT_Patient_General]e on e.CMC_ID = x.CMC_ID


update x
		set TestPat = 1
from #Consent x
inner join [ETL_Local_PROD].[dbo].[AT_Patient_General]e on e.CMC_ID = x.CMC_ID
where charindex('test',e.GP_Practice) > 0


				 --select * from #Consent where TestPat = 1

				 delete from #Consent where LastRecordFlag is null
				 delete from #Consent where TestPat = 1
				 delete x from #Consent x where not exists (select distinct cmc_id from [ETL_Local_PROD].[dbo].[AT_Patient_General] a where a.CMC_ID = x.CMC_ID)

 ---------------------------------------------------------------------------------------------------------------------------------------
		IF OBJECT_ID('tempdb..#TempIt1') IS NOT NULL 
		dROP TABLE #TempIt1
 
		SELECT
		pds.cmc_id,
		pds.PatientSummary,
		pr.DateLastSaved,
		pds.Restricted,
		pds.NHS_Number,
		--t.[AlertType],
		--t.[Alert Description],
		--t.[Comments] as [Alert Comments],
		case when (ch.[Team or GP] is not null) then 1 else null end as [Care home Flag],
		ch.[Team or GP] as [Care Home Name] ,


 
		--[MAIN_ADDRESS] as [Patient Main Address],
		--[MAIN_POSTCODE] as [Patient Main Postcode],
		pds.[PRIMARY_ADDRESS] as [Patient Primary Address],
		pds.[PRIMARY_POSTCODE]  as [Patient Primary Postcode],
cast(null as nvarchar(254))as [IMD Rank],
cast(null as nvarchar(254))as [IMD Decile],
		[Home_Phone],
		[Mobile_Phone],
		[Work_Phone],
		[Email],
		[Other_Phone],
		pds.[GP_Practice],
		pds.[GPODSCode],
 
 
		CAST(pds.Add_Date as Date) as [CMC Record Creation Date], 
		cc.[StaffProviderTypeDescription] as [Creator Type],
		cc.StaffFullName as [Name of Care Plan Creator],
		cc.TeamType as  [Creator Team Type],
		cc.team as [Creator Team],
		r.parent as [Creator Parent ORganisation],

 
		cast(pds.Date_Original_Approval as date) as [CMC record First Published Date],
		[OriginalApprover] as [Original Approver Name],
		OriginalApproverJobTitle as [First Publisher Type], 
		--Original_Approver_Prof_Group as  FirstPublisherTeamType,
		s.DErivedTEamType as [Approver Team Type],
		s.team as [Approver Team],
		s.Parent as [Approver Parent Organisation],
		cast(pds.DoB AS date) as DOB,

 
		diag.[MainDiagnosisCode] as [Classified Primary Diagnosis],
		diag.[Main Classified Diagnosis for Report] as [Classified Primary Diagnosis Category],
		diag.MainDiagnosisCategoryDescription as [Primary Diagnosis Category],
		diag.MainDiagnosis as [Primary Diagnosis],

		diag.[SecondDiagnosisCode] as [Classified Secondary Diagnosis],
		diag.[Second Classified Diagnosis for Report] as [Classified Secondary Diagnosis Category],
		diag.SecondDiagnosisCategoryDescription as [Secondary Diagnosis Category],
		diag.SecondDiagnosis as [Secondary Diagnosis],
 
		gen.description as Gender,
 
		--- Modified 11/10/2019
		---
		CASE WHEN pds.Religion  in ('Buddhist','Centra Asian Buddhist','Chinese Buddhist','Japanese Buddhist','Mahayana Buddhist','Sinhalese Buddhist','South East Asia Buddhist','Theravada Buddhist','Tibetan Buddhist','Western Buddhist') then 'Buddhist'
		WHEN pds.Religion  in ('Anglican','Arminianist','Baptist','Calvinist','Catholic','Christadelphian','Christian','Christian Scientist','Church of England','Church of Ireland','Church of Scotland','Evangelist','Greek Orthodox','Jehovahs Witness','Liberal Protestant','Methodist','Mormon','Nonconformist','Orthodox Christian','Pentecostalist','Pietist','Plymouth Brethren','Presbyterian','Protestant','Quaker','Rastafarian','Roman Catholic','Russian Orthodox','Scottish Protestant','Seventh Day Adventist','Society of Friends','Unitarian','United Reform Church') then 'Christian'
		WHEN pds.Religion  in ('Conservative Jewish','Jewish','Liberal Jewish','Reconstructionist Jewish','Safardi Jewish') then 'Jewish'
		WHEN pds.Religion  in ('Druze','Ismailis','Muslim','Shiite Muslim','Sufi Muslim','Sunni Muslim') then 'Muslim'
		WHEN pds.Religion in ('Atheist','None') then 'None'
		WHEN pds.Religion in ('Agnostic','Arcane School','Babis','Confucian','Eckankar','Eminist','Humanist','Jedi Knight','Mixed Religion','Other','Primal Society','Spiritualist') then 'Other'
		WHEN pds.Religion='Religion Withheld' then	'Declines to Disclose'
		WHEN pds.Religion='Hindu' then 'Hindu'
		WHEN pds.Religion='Sikh' then 'Sikh'
		END as Religion,
 
		CASE WHEN pds.ETHNICITY in ('A','B','C','Y') then 'White'
		WHEN pds.ETHNICITY IN ('D','E','F','G') then 'Mixed'
		WHEN pds.ETHNICITY IN ('H','J','K','L') then 'Asian or Asian British'
		WHEN pds.ETHNICITY IN ('M','N','P') then 'Black or Black British'
		WHEN pds.ETHNICITY IN ('C5','E2','R') then 'Other Ethnic Groups'
		WHEN pds.ETHNICITY in ('Z','Z2','Z3') then 'not stated / not divulged / not known'
		END as Ethnicity,	
  
  

		----
		--dt.CCLivingConditions as Living_Condition,
		--dt.[Dwelling Type] as Dwelling_Type,
		--dt.CCKeySafeDetails AS [Key Safe Details],
		--dt.CCResidenceNotes as [Residential Notes],
 		cast(null as varchar(max))  as Living_Condition,
		cast(null as varchar(max))  as Dwelling_Type,
		cast(null as varchar(max))  AS [Key Safe Details],
		cast(null as varchar(max)) as [Residential Notes],

		ms.description as [Marital Status],
		--pds.CCG as CCG,
		pds.DoD_PDS as   [Date of death on Spine],
		pds.PDS_Status as [Spine Status],
		pds.DoD as   [Date of death on CMC],
		pds.DeathSourceInfo as [Death Source Info],
		dv.Description as [Death Variance],
		pds.DeathLocationOther as [Death Variance Description],
		--select * from  [ETL_Local_PROD].[dbo].[AT_PPD_Data]
		n.Description as [Preferred Place Of Care],
		ppd.[PPD Expressed] as [Preferred Place of Death Expressed],
		ppd.DODPLACE_RECORDED as [Place of Death Recorded],
		ppd.Derived_PPD1 as [Preferred Place Of Death 1],
		ppd.Derived_PPD2 as [Preferred Place Of Death 2],

		ppd.Derived_DODPLACE as [Actual Place Of Death],
		ws.PatientWishes as [Patient Wishes],
		ws.FamilyAwarePref as [Family Awareness], 
		ws.OrganDonat as [Organ Donor],
		ws.OrganDonatDet as [Organ Donor Details],
		ws.CulturalRelNeeds as [Cultural/Religious Need],
		pr.sTAFFnAME as [Planned Reviewer],
		pr.StaffProviderTypeDescription as [Planned Reviewer Title],
		convert(date,pr.PlannedReviewTime) as [Planned Review Date],
		 [POADocLocation] as [POA Doc Location],

	 
 
		pds.DATE_PROGNOSIS as [Prognosis Date],
		h.Description as [Surprise Question],
		    [PrognosisClinicianName] as [Prognosis Clinician Name],
		pds.PrognosisClinicianWorkbase as [Prognosis Organisation Name],
		pds.PrognosisClinicianWorkbaseODS as [Prognosis Organisation ODS Code],
		fap.Description as [Family aware of Prognosis],
		pds.PA_FAMPRODDETAILS as [Family Aware_Prognosis Details],
		pap.Description as [Patient Aware of Prognosis],
		pds.PA_PRODDETAILS as [Patient Aware Prognosis Details],
		--pds.TimeFrame as Timeframe,
		pds.ALT_PROGNOSIS as [Timeframe Description],
		adt.Description as [Presence Of Advanced Decision To Refuse Treatment],

		pds.DNACPRFormUploaded as [DNACPR Form Attached],
		rd.Description as [CPR Resus Discussion with Patient],
		pds.PatientDiscussionTime as [CPR Resus Discussion Date],
		pds.RESUS_PATIENTDET as [Summary of CPR Resus Discussion],
		wa.Description as [Welfare Attorney assigned],
		i.Description as [CPR Decision],
		pds.CPRDECDATE as [CPR Decision Date],
 
		pds.REVIEW_DATE as [CPR Review Date],
		k.Description  as [Presence of Discussion of DNACPR With Family],
		pds.RESUS_FAMDET as [Details of Resus Family Discussion],
		pds.HasBeenAgreed as [Resus Agreed],
		pds.FamilyDiscussionTime as [Date discussed with Family],
		l.description as [Patient Have The Capacity to Make and Communicate Decisions About CPR],
		pds.CLINPROB as [Clinical Reson Patient Cannot Discuss],	
		b.description as [Clinical Recommendations], 
		u.Description as [WHO Performance],
		convert(date,pds.WHP_DATE) as [Date of WHO Performance Status] ,
		gg.description as [Patient Aware of Diagnosis],
		g.Description as [Carers Aware of Diagnosis],
		

       
		
		m.Description as [Presence of Organ Donation Discussion],
		e.description as [Has a DS1500 form been completed],
		d.Description as [Is Patient In Receipt Of a Care_Package for Personal Care],
	
		c.Description as [Consent Type],
		c.report as [Concent Capacity],
		c.translation as [Consent Reason],
		convert(Date,c.DateObtained) as [Consent Date],
		
		 

		
		--select * from #MB
		mb.HaveDisability as [Patient Disabilities],
		mb.DisabilityDetails as [Further Details],
		mb.OtherSignifHx as [Other Significant History],

	 	q.Description as [Family Support],
		pds.FAM_SUPPORT_Y as [Family Support DEtails],
		--convert(Date,cot2.WHOPerfTime) as	[WhoPerftime Final]
				hc.Description as [Homecare Required],
		pds.HOMECARE_DET as [Details of Homecare]
 
		into #TempIt1

		FROM [ETL_Local_PROD].[dbo].[AT_Patient_General] pds with (nolock)
		left join [ETL_Local_PROD].[dbo].[DIM_Date]ad on ad.[Calendar Day] = convert(date,pds.Add_Date) 
		left join [ETL_Local_PROD].[dbo].[DIM_Date]doa on doa.[Calendar Day] = convert(date,pds.Date_Original_Approval)
		left join [ETL_Local_PROD].[dbo].[AT_CarePlanCreationData]cc with (nolock) on cc.CMC_ID = pds.CMC_ID
		left join  [ETL_Local_PROD].[dbo].[AT_PPD_Data]ppd with (nolock) on ppd.CMC_ID = pds.CMC_ID
		left join #PPD ws on ws.CMC_ID = pds.CMC_ID and ws.LastVErsionFlag is not null
		left join  [#tempORG2]ch with (nolock) on ch.CMC_ID = pds.CMC_ID
		left join  [ETL_Local_PROD].[dbo].[AT_DistinctDiagnosis]diag with (nolock) on diag.CMC_ID = pds.CMC_ID

		left join [ETL_PROD].[dbo].[Coded_LevelOfTrtmnt] b on b.code = replace(pds.CEILTREAT,',',' ')

		left join [#Consent]c on c.CMC_ID = pds.CMC_ID
		left join  [ETL_PROD].[dbo].[Coded_PatientReceipt]d on d.Code = pds.CAREPLAN
		left join [ETL_PROD].[dbo].[Coded_DS1500]e on e.Code = pds.DS1500 
		left join [ETL_PROD].[dbo].[Coded_Gender]gen on gen.Code = pds.Gender
		left join [ETL_PROD].[dbo].[Coded_PatientAwareDiagnosis]g on g.Code = pds.C_AWARE
		left join [ETL_PROD].[dbo].[Coded_PatientAwareDiagnosis]gg on gg.Code = pds.P_AWARE
		left join [ETL_PROD].[dbo].[Coded_Surprise]h on h.code = pds.Surprise
		left join [ETL_PROD].[dbo].[Coded_CPRDecision]i on i.Code = pds.CARDIO_YN
		left join [ETL_PROD].[dbo].[Coded_CPRDecision]rd on rd.Code = pds.RESUS_PATIENT
		left join [ETL_PROD].[dbo].[Coded_PtAbleToDecide]l on l.Code = pds.HAVECAP
		left join [ETL_PROD].[dbo].[Coded_OrganDonat]m on m.code = pds.WISHES
		left join [ETL_PROD].[dbo].[Coded_PreferPlace]n on n.Code = pds.PPC
		left join [ETL_PROD].[dbo].[Coded_ADRTExists]o on o.Code = pds.ADRTExists
		left join [ETL_PROD].[dbo].[Coded_FamilySupport]q on q.Code = pds.FAM_SUPPORT
		left join [ETL_PROD].[dbo].[Coded_MaritalStatus]ms on ms.Code = pds.[MARITALSTATUS]
		--left join [ETL_PROD].[dbo].[Coded_ConsentType]cs on cs.Code = replace(pds.Consent,',',' ')
		left join [ETL_PROD].[dbo].[Coded_PatientDiscussion]j on j.Code = pds.RESUS_PATIENT
		left join [ETL_PROD].[dbo].[Coded_FamilyDiscussion]k on k.Code = pds.RESUS_FAMILY
		left join [#CPR]cpr1 on cpr1.CMC_ID = pds.CMC_ID and cpr1.FirstVersionNumber = 1
		left join [#CPR]cpr2 on cpr2.CMC_ID = pds.CMC_ID and cpr2.LastVersionFlag = 1
		left join [#CoT]cot1 on cot1.CMC_ID = pds.CMC_ID and cot1.FirstVersionNumber = 1
		left join [#CoT]cot2 on cot2.CMC_ID = pds.CMC_ID and cot2.LastVersionFlag = 1
 
		left join  [ETL_Local_PROD].[dbo].[AT_Organisation_to_Department_Provider_Directory]r on r.EnterpriseID = cc.enterpriseid
		left join  [ETL_Local_PROD].[dbo].[AT_Organisation_to_Department_Provider_Directory]s on s.EnterpriseID = pds.[OriginalApproverWorkbaseEId]  
		left join [ETL_PROD].[dbo].[Coded_WHOPerf]u on u.code = replace(pds.WHPERF,',',' ')
		left join #MB mb on mb.CMC_ID = pds.CMC_ID
		left join #PlannedReviewer pr on pr.CMC_ID = pds.CMC_ID and LastRowFlag = 1
		left join [ETL_PROD].[dbo].[Coded_WelfareAttorney]wa on wa.code = pds.APPOINTWA
		left join [ETL_PROD].[dbo].[Coded_HomecareHelp]hc on hc.Code = pds.HOMECARE
		left join [ETL_PROD].[dbo].[Coded_DeathVariance]dv on dv.code = pds.DeathVariance
		left join [ETL_PROD].[dbo].[Coded_ADRTExists]adt on adt.code = PDS.ADRTExists
		left join [ETL_PROD].[dbo].[Coded_FamilyAwarePrognosis]fap on fap.code = pds.PA_FAMPROD
		left join [ETL_PROD].[dbo].[Coded_PatientAwarePrognosis]pap on pap.code = pds.PA_PROD
	 
	 	 delete x from #TempIt1 x where   charindex('test', GP_Practice) > 0


		update t
			set	t.Living_Condition = CCLivingConditions,
				t.Dwelling_Type = [Dwelling Type],
				t.[Key Safe Details] = CCKeySafeDetails,
				t.[Residential Notes] = CCResidenceNotes,
				t.[Care home FLag] = case when  (charindex('care home',dwelling_type)>0 and t.[Care home FLag] is null )then 1 else t.[Care home FLag] end,
				t.[Care Home Name] = case when  (charindex('care home',dwelling_type)>0 and [Care Home Name] is null )then dt.Line2 else t.[Care Home Name] end
		from #TempIt1 t 
		left join #dt dt on coalesce(dt.ItemID_Shaved,dt.itemid) = t.PatientSummary and t.[Patient Primary Postcode] = dt.PostalCode

		--where pds.cmc_id in (100016973,
		--100016974)
		--where NHS_Number = 9990569126

		
					if object_id('tempdb..#Temp3') is not null
					drop table #Temp3

					select 
					distinct  
					ltrim(rtrim(REPLACE(CCG, ' CCG', ''))) as CCG, 
					STP,
					[NHS England REgion],
					[Local Authority],
					[PCDS],
					REPLACE([PCDS] , ' ', '') as [PCDS_NoGaps] ,
					left(PCDS,7) as [PCDS_7],
					left(PCDS,6) as [PCDS_6],
					left(PCDS,5) as [PCDS_5],
					left(PCDS,4) as [PCDS_4],
					LSOA11CD
					into #Temp3
					from [ETL_Local_PROD].[dbo].[AT_ENGLISH_POSTCODE_DATA_NEW]










		update s

	set s.[IMD Rank] =  md.[IMD_Rank], 
		s.[IMD Decile] = md.[IMD_Decile] 

from [#TempIt1] s
inner join  [#Temp3]pc on pc.PCDS = s.[Patient Primary Postcode]
inner join [ETL_Local_PROD].[dbo].[AT_Indices_of_Multiple_Deprivation_(IMD)_2019]md on md.lsoa11cd = pc.LSOA11CD
 



update s

	set s.[IMD Rank] =  md1.[IMD_Rank],
		s.[IMD Decile] = md1.[IMD_Decile]

from [#TempIt1] s
 
inner join  [#Temp3]pc2 on   pc2.[PCDS_NoGaps]  = s.[Patient Primary Postcode]
inner join [ETL_Local_PROD].[dbo].[AT_Indices_of_Multiple_Deprivation_(IMD)_2019]md1 on md1.lsoa11cd = pc2.LSOA11CD


IF OBJECT_ID('[ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]') IS NOT NULL 
drop table [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]
select * into
[ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]
from #TempIt1 

  --select * from #TempIt1    where NHS_Number in (4147000894)
  --where NHS_Number = 9990569126


 


  --select * from #TempIt1   where ([Classified Primary Diagnosis] = 'C71' OR [Classified Secondary Diagnosis] = 'C71') 

/**

   select *   FROM [ETL_Local_PROD].[dbo].[AT_Patient_General]  where NHS_Number in (4147000894)
 
    select *   FROM [ETL_Local_PROD].[dbo].[AT_Patient_General] where cmc_id in (100010227)
  --select * from [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]
 --PS||100010227||2||1

select [Consent REason],count(*) as Total from [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]group by [Consent REason]
select [Consent REason],count(*) as Total from [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]where CMC_record_First_Published_Date <'2020-03-01'group by [Consent REason]
select [Consent REason],count(*) as Total  from [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]where CMC_record_First_Published_Date >'2020-03-01' and CMC_record_First_Published_Date <'2020-10-01'group by [Consent REason]
select [Consent REason],count(*) as Total  from [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]where CMC_record_First_Published_Date >'2020-09-30' and CMC_record_First_Published_Date <'2021-04-01'group by [Consent REason]
select [Consent REason],count(*) as Total  from [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]where CMC_record_First_Published_Date >'2021-03-31' and CMC_record_First_Published_Date <'2021-10-01'group by [Consent REason]

select [CPR_Decision],count(*) as Total from [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]group by [CPR_Decision]
select [CPR_Decision],count(*) as Total from [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]where CMC_record_First_Published_Date <'2020-03-01'group by [CPR_Decision]
select [CPR_Decision],count(*) as Total  from [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]where CMC_record_First_Published_Date >'2020-03-01' and CMC_record_First_Published_Date <'2020-10-01'group by [CPR_Decision]
select [CPR_Decision],count(*) as Total  from [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]where CMC_record_First_Published_Date >'2020-09-30' and CMC_record_First_Published_Date <'2021-04-01'group by [CPR_Decision]
select [CPR_Decision],count(*) as Total  from [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]where CMC_record_First_Published_Date >'2021-03-31' and CMC_record_First_Published_Date <'2021-10-01'group by [CPR_Decision]

select Patient_Have_The_Capacity_To_Make_And_Communicate_Decisions_About_CPR,count(*) as Total from [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary] where [CPR Resus Discussion with Patient] = 'Yes' group by Patient_Have_The_Capacity_To_Make_And_Communicate_Decisions_About_CPR
select Patient_Have_The_Capacity_To_Make_And_Communicate_Decisions_About_CPR,count(*) as Total from [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]where CMC_record_First_Published_Date <'2020-03-01'group by Patient_Have_The_Capacity_To_Make_And_Communicate_Decisions_About_CPR
select Patient_Have_The_Capacity_To_Make_And_Communicate_Decisions_About_CPR,count(*) as Total  from [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]where CMC_record_First_Published_Date >'2020-03-01' and CMC_record_First_Published_Date <'2020-10-01'group by Patient_Have_The_Capacity_To_Make_And_Communicate_Decisions_About_CPR
select Patient_Have_The_Capacity_To_Make_And_Communicate_Decisions_About_CPR,count(*) as Total  from [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]where CMC_record_First_Published_Date >'2020-09-30' and CMC_record_First_Published_Date <'2021-04-01'group by Patient_Have_The_Capacity_To_Make_And_Communicate_Decisions_About_CPR
select Patient_Have_The_Capacity_To_Make_And_Communicate_Decisions_About_CPR,count(*) as Total  from [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary]where CMC_record_First_Published_Date >'2021-03-31' and CMC_record_First_Published_Date <'2021-10-01'group by Patient_Have_The_Capacity_To_Make_And_Communicate_Decisions_About_CPR

  select 
  * 
  from [ETL_Local_PROD].[dbo].[AT_Patient_Detailed_Summary] 
  where coalesce([Date of death on Spine],[Date of death on CMC]) is null
  and ([Family aware of Prognosis] = 'Yes' or [Presence of Discussion of DNACPR With Family] = 'Yes')



**/



			IF OBJECT_ID('tempdb..#TempViewsLong') IS NOT NULL 
			dROP TABLE #TempViewsLong
			SELECT a.[CMC_ID]
			,[ActionTime] as ActivityDate
			,ROW_NUMBER() OVER(partition by a.cmc_id, [actionTime],[ActionType],stafffullname,a.team,[parent],b.[DerivedTeamType],a.postcode  ORDER BY OverAllOrder)  as RNumber
			--,convert(date,DATEADD(month, DATEDIFF(month, 0, convert(date,[ActionTime])), 0)) as ActivityMonth
			,[ActionType]
			,[DerivedActionType]
    
			,x.[Date Of Death]
			,datediff(day,[ActionTime],x.[Date Of Death]) as TimeInDays
			, [DerivedActionType]   as ActivityType
			,  [DerivedTeamType]   as OrganizationTypeDescription
  
   
   
			,b.[Team]
			,b.Parent
 
			into #TempViewsLong
			FROM [ETL_Local_PROD].[dbo].[AT_CarePlanData]a
			inner join [ETL_Local_PROD].[dbo].[AT_Organisation_to_Department_Provider_Directory]b on b.EnterpriseID = a.DEptEnterpriseID
			left join [ETL_Local_PROD].[Reference].[STP] cc on cc.CCGLONG_TRUNC =  b.CCG
			inner join (select distinct c.cmc_ID,coalesce([date of death on Spine],[date of death on CMC]) as [Date Of Death] from #TempIt1 c where coalesce([date of death on Spine],[date of death on CMC])  is not null)x on x.CMC_ID = a.CMC_ID
 
  
			Where  (DerivedActionType like 'view%' )
 
			and b.Team is not null
			and b.DerivedTeamType <>  'EXCLUDE'
 
	

			--select * from #TempViewsLong where CMC_ID = 100000013 order by ActivityDate
			--select * from #TempViewsLong where RNumber >1 
			delete from #TempViewsLong where RNumber >1 
			delete from #TempViewsLong where TimeInDays > 30 
			delete from #TempViewsLong where TimeInDays < 0




 		IF OBJECT_ID('tempdb..#TempIt2') IS NOT NULL 
		dROP TABLE #TempIt2
		select
		pds.*,
--,
(
  (select count(*) from #TempViewsLong x where x.CMC_ID = pds.cmc_id   )
 
) as [Total record views in 30 days before death]
,
(
  (select count(*) from #TempViewsLong x where x.CMC_ID = pds.cmc_id  and TimeInDays <= 10 )
 
) as [Total record views in 10 days before death]

into #TempIt2

from #TempIt1 pds
--where pds.[Date Of Death] is not null


delete from #TempViewsLong where ( OrganizationTypeDescription NOT IN ('111 PROVIDER','AMBULANCE TRUST') OR OrganizationTypeDescription is null)



if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_Droney_BrainCancer]') is not null
drop table [ETL_Local_PROD].[dbo].[AT_Droney_BrainCancer]
		select
		pds.*,
 
(
 
 (select count(*) from #TempViewsLong x where x.CMC_ID = pds.cmc_id and OrganizationTypeDescription = '111 PROVIDER'   )
 
) as [Total 111 record views in 30 days before death],
(
 (select count(*) from #TempViewsLong x where x.CMC_ID = pds.cmc_id  and TimeInDays <= 10 and OrganizationTypeDescription = '111 PROVIDER'  )
 
) as [Total 111 record views in 10 days before death],
(
 
 (select count(*) from #TempViewsLong x where x.CMC_ID = pds.cmc_id and OrganizationTypeDescription = 'AMBULANCE TRUST'   )
 
) as [Total 999 record views in 30 days before death],
(
 (select count(*) from #TempViewsLong x where x.CMC_ID = pds.cmc_id  and TimeInDays <= 10 and OrganizationTypeDescription = 'AMBULANCE TRUST'  )
 
) as [Total 999 record views in 10 days before death]

into [ETL_Local_PROD].[dbo].[AT_Droney_BrainCancer]

from #TempIt2 pds
 where ([Classified Primary Diagnosis] = 'C71' OR [Classified Secondary Diagnosis] = 'C71') 

 end
 --select * from [ETL_Local_PROD].[dbo].[AT_Droney_BrainCancer]