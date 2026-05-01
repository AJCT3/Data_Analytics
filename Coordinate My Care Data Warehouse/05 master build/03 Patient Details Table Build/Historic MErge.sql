/****** Script for SelectTopNRows command from SSMS  ******/



			if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_Patient_General_Historic_Inc]') is not null
			drop table [ETL_Local_PROD].[dbo].[AT_Patient_General_Historic_Inc]

			select
			*
			into [ETL_Local_PROD].[dbo].[AT_Patient_General_Historic_Inc]
			from
			(

		SELECT  
		[CMC_ID],
		[NHS_Number],
		[OnNewSystem],
		[VersionNumber],
		[GenusId],
		[FirstVersionNumber],
		[#PatientSummary] as [PatientSummary],
		[#Patient] as [Patient],
		[#Care_Plan] as [Care_Plan],
		--[DataLoadDate], -- date
		[#IsSoftDeleted] as [IsSoftDeleted],
		[DateLastSaved], -- date
		[#OriginalEnteredBy] as [OriginalEnteredBy],
		null as [OriginalWorkbaseEId],
		convert(Date,[Add_Date]) as [Add_Date], -- date
		[#OriginalApprovedBy] as [OriginalApprovedBy],
		convert(Date,[Date_Original_Approval]) as [Date_Original_Approval], -- date
		[OriginalApprover],
		[OriginalApproverJobTitle],
		[OriginalApproverWorkbase],
		[OriginalApproverWorkbaseODS],
		[Original_Approver_Prof_Group],
		[OriginalApproverWorkbaseEId],
		[OriginalApproverODS],



		[Original_Approver_Role_Description],
		[OriginalAssessmentStatus],
		[Date_Original_Assessment],
		[#LatestEnteredBy] as [LatestEnteredBy],
		[Date_Latest_Assessment],
		[#LatestApprovedBy] as [LatestApprovedBy],
		[Date_Latest_Approval],
		--[#TITLE] as [TITLE],
		[TITLE],
		[FORENAME],
		[MiddleName],
		[SURNAME],
		[PreferredName],
		convert(date,[DoB]) as [DoB], -- date
		[Age],
		[#GENDER] as [GENDER],
		[#MARITALSTATUS] as [MARITALSTATUS],
		[#ETHNICITY] as [ETHNICITY],
		[LivingCondDetails],
		[#RELIGION] as [RELIGION],
		[#PrimaryLanguage] as [PrimaryLanguage],
		[PrimaryLangDetails],
		convert(date,[DoD_Demographics]) AS [DoD], -- date
		[#DODPLACE] as [DeathLocation],
		[INF_DEATH] as [DeathSourceInfo],
		[OTHERPS] as [DeathLocationOther],
		[#VARIANCE] as [DeathVariance],
		[OTHERPSA] as [DeathVarianceOther],
		[#Restricted] as [Restricted],
		[PDSOverride],
		convert(date,[DOD_PDS]) as [DOD_PDS], -- date
		[#DeceasedPDS] as [DeceasedPDS],
		[PDS_NHS_Number],
		[PDS_Status],
		convert(date,[PDS_Reconciliation_Date]) as [PDS_Reconciliation_Date], -- date
		[#MAIN_ADDRESS] as [MAIN_ADDRESS],
		[#MAIN_POSTCODE] as [MAIN_POSTCODE],
		[#PRIMARY_ADDRESS] as [PRIMARY_ADDRESS],
		[#PRIMARY_POSTCODE] as [PRIMARY_POSTCODE],
		[#CURRENT_ADDRESS] as [CURRENT_ADDRESS],
		[#CURRENT_POSTCODE] as [CURRENT_POSTCODE],
		[#SECONDARY_ADDRESS] as [SECONDARY_ADDRESS],
		[#SECONDARY_POSTCODE] as [SECONDARY_POSTCODE],
		[#Home_Phone] as [Home_Phone],
		[#Mobile_Phone] as [Mobile_Phone],
		[#Work_Phone] as [Work_Phone],
		[#Email] as [Email],
		[#Other_Phone] as [Other_Phone],
		[#RegisteredGP] as [GP_Practice],
		[CCG],
		[CommissioningArea],
		[#London_CCG_ODS] as [London_CCG_ODS],
		[CCG_ODS]
		,
		[#ConsentedBy] as [ConsentedBy],
		[ConsentedOn] as [ConsentedDate],
		[#CONSENT] as [CONSENT],
		[MC_DET],
		[#REQ_COPY] as [REQ_COPY],
		[#PlannedReviewer] as [PlannedReviewer],
		Convert(Date,[REVIEW]) as [PlannedReviewDate], -- date
		[POADocLocation],
		[#PrognosisBy] as [PrognosisByRef],
		[PrognosisClinician] as [PrognosisClinicianName],
		[PrognosisClinicianWorkbase],
		[PrognosisClinicianWorkbaseODS],
		[PA_FAMPRODDETAILS],
		[#PA_FAMPROD] as [PA_FAMPROD],
		[PA_PRODDETAILS],
		[#PA_PROD] as [PA_PROD],
		[TimeFrame],
		[#ALT_PROGNOSIS] as [ALT_PROGNOSIS],
		[#Surprise] as [Surprise],
		convert(date,[DATE_PROGNOSIS]) as [DATE_PROGNOSIS], -- date
		[ADRTDetails],
		[#ADRTExists] as [ADRTExists],
		[COMM_DIFF_DETAIL],
		[C_AWAREDETAILS],
		[#C_AWARE] as [C_AWARE],
		[#COMMDIFF] as [COMMDIFF],
		[#CEILTREAT] as [CEILTREAT],
		[CT_DET],
		[SIGNIFICANT_MEDICAL],
		[P_AWAREDETAILS],
		[#P_AWARE] as [P_AWARE],
		[#WHPERF] as [WHPERF],
		convert(Date,[WHP_DATE]) as [WHP_DATE], -- date
		[#Classified_Diagnosis] as [Classified_Diagnosis],
		[#DiagnosisCode] as [DiagnosisCode],
		convert(Date,[REVIEW_DATE]) as [REVIEW_DATE], -- date
		[ChildInvolv],
		[ChildParentConsult],
		[#CPRBy] as [CPRBy],
		[#VALIDAD] as [VALIDAD],
		[POSITION],
		convert(Date,[DNARDATE1]) as [DNARDATE1], -- date

		[CourtOrder],
		[ORDER_YES],
		[DNACPRFormUploaded],
		[#CARDIO_YN] as [CARDIO_YN],
		convert(Date,[CPRDECDATE]) as [CPRDECDATE], -- date
		[#RESUS_FAMILY] as [RESUS_FAMILY],
		[RESUS_FAMDET],
		convert(Date,[FamilyDiscussionTime]) as [FamilyDiscussionTime] , -- date
		[HasBeenAgreed],
		[JudgeCourt],
		[JudgeCourtLocation],
		Convert(Date,[JudgeCourtTime]) as [JudgeCourtTime],
		[NAMEMEM],
		[#RESUS_PATIENT] as [RESUS_PATIENT],
		[RESUS_PATIENTDET],
		convert(Date,[PatientDiscussionTime]) as [PatientDiscussionTime], -- date
		[#HAVECAP] as [HAVECAP],
		[#APPOINTWA] as [APPOINTWA],
		[CLINPROB],
		[DNARNAME1],
		[DNARNAME2],
		[DNARNAME3],
		convert(Date,[DNARDATE2]) as [DNARDATE2], -- date
		convert(Date,[DNARDATE3]) as [DNARDATE3], -- date
		convert(Date,[DNARDATE4]) as [DNARDATE4], -- date
		[#DS1500] as [DS1500],
		[#EQUIP] as [EQUIP],
		[EQUIP_DETAIL],
		[#FAM_SUPPORT] as [FAM_SUPPORT],
		[FAM_SUPPORT_Y],
		[#HOMECARE] as [HOMECARE],
		[HOMECARE_DET],
		[#CAREPLAN] as [CAREPLAN],
		[CAREPLAN_DETAIL],
		[#Anticoags] as [Anticoags],
		[#Insulin] as [Insulin],
		[MedListLocation],
		[#OPIOID] as [OPIOID],
		[MED_OTH],
		[#Steroids] as [Steroids],
		[CULTURAL],
		[FAMILY_AWAR],
		[#WISHES] as [WISHES],
		[WISHES_YES],
		[PERCARE_PLAN],
		[#PPC] as [PPC],
		[PPDDiscuss],
		[PlaceCareDet],
		[#PPD1] as [PPD1],
		[PPCDiscuss],
		[PlaceDeath1Det],
		[#PPD2] as [PPD2],
		[PlaceDeath2Det]



		FROM [ETL_Local_PROD].[dbo].[PatientDetailHistoric]

		union 

		 select 
		 [CMC_ID]
			  ,[NHS_Number]
			  ,[OnNewSystem]
			  ,[VersionNumber]
			  ,[GenusId]
			  ,[FirstVersionNumber]
			  ,[PatientSummary]
			  ,[Patient]
			  ,[Care_Plan]
			  ----,[DataLoadDate]
			  ,[IsSoftDeleted]
			  ,[DateLastSaved]
			  ,[OriginalEnteredBy]
			  ,[OriginalWorkbaseEId]
			  ,[Add_Date] -- date
			  ,[OriginalApprovedBy]
			  ,[Date_Original_Approval] -- date
			  ,[OriginalApprover]
			  ,[OriginalApproverJobTitle]
			  ,[OriginalApproverWorkbase]
			  ,[OriginalApproverWorkbaseODS]
			  ,[Original_Approver_Prof_Group]
			  ,[OriginalApproverWorkbaseEId]
			  ,[OriginalApproverODS]
			  ,[Original_Approver_Role_Description]
			  ,[OriginalAssessmentStatus]
			  ,[Date_Original_Assessment] -- date
			  ,[LatestEnteredBy]
			  ,[Date_Latest_Assessment] -- date
			  ,[LatestApprovedBy]
			  ,[Date_Latest_Approval] -- date
			  --,[PatTitle]
			  ,[TITLE]
			  ,[FORENAME]
			  ,[MiddleName]
			  ,[SURNAME]
			  ,[PreferredName]
			  ,[DoB] -- date
			  ,[Age]
			  ,[GENDER]
			  ,[MARITALSTATUS]
			  ,[ETHNICITY]
			  ,[LivingCondDetails]
			  ,[RELIGION]
			  ,[PrimaryLanguage]
			  ,[PrimaryLangDetails]
			  ,[DoD] -- date
			  ,[DeathLocation]
			  ,[DeathSourceInfo]
			  ,[DeathLocationOther]
			  ,[DeathVariance]
			  ,[DeathVarianceOther]
			  ,[Restricted]
			  ,[PDSOverride]
			  ,[DoD_PDS] -- date
			  ,[DeceasedPDS]
			  ,[PDS_NHS_Number]
			  ,[PDS_Status]
			  ,[PDS_Reconciliation_Date] -- date
			  ,[MAIN_ADDRESS]
			  ,[MAIN_POSTCODE]
			  ,[PRIMARY_ADDRESS]
			  ,[PRIMARY_POSTCODE]
			  ,[CURRENT_ADDRESS]
			  ,[CURRENT_POSTCODE]
			  ,[SECONDARY_ADDRESS]
			  ,[SECONDARY_POSTCODE]
			  ,[Home_Phone]
			  ,[Mobile_Phone]
			  ,[Work_Phone]
			  ,[Email]
			  ,[Other_Phone]
			  ,[GP_Practice]
			  ,[CCG]
			  ,[CommissioningArea]
			  ,[London_CCG_ODS]
			  ,[CCG_ODS]
			  ,[ConsentedBy]
			  ,[ConsentedDate] -- date
			  ,[CONSENT]
			  ,[MC_DET]
			  ,[REQ_COPY]
			  ,[PlannedReviewer]
			  ,[PlannedReviewDate] -- date
			  ,[POADocLocation]
			  ,[PrognosisByRef]
			  ,[PrognosisClinicianName]
			  ,[PrognosisClinicianWorkbase]
			  ,[PrognosisClinicianWorkbaseODS]
			  ,[PA_FAMPRODDETAILS]
			  ,[PA_FAMPROD]
			  ,[PA_PRODDETAILS]
			  ,[PA_PROD]
			  ,[TimeFrame]
			  ,[ALT_PROGNOSIS]
			  ,[Surprise]
			  ,[DATE_PROGNOSIS] -- date
			  ,[ADRTDetails]
			  ,[ADRTExists]
			  ,[COMM_DIFF_DETAIL]
			  ,[C_AWAREDETAILS]
			  ,[C_AWARE]
			  ,[COMMDIFF]
			  ,[CEILTREAT]
			  ,[CT_DET]
			  ,[SIGNIFICANT_MEDICAL]
			  ,[P_AWAREDETAILS]
			  ,[P_AWARE]
			  ,[WHPERF]
			  ,[WHP_DATE] -- date
			  ,[Classified_Diagnosis]
			  ,[DiagnosisCode]
			  ,[REVIEW_DATE]
			  ,[ChildInvolv]
			  ,[ChildParentConsult]
			  ,[CPRBy]
			  ,[VALIDAD]
			  ,[POSITION]
			  ,[DNARDATE1] -- date
			  ,[CourtOrder]
			  ,[ORDER_YES]
			  ,[DNACPRFormUploaded]
			  ,[CARDIO_YN]
			  ,[CPRDECDATE]
			  ,[RESUS_FAMILY]
			  ,[RESUS_FAMDET]
			  ,[FamilyDiscussionTime]
			  ,[HasBeenAgreed]
			  ,[JudgeCourt]
			  ,[JudgeCourtLocation]
			  ,[JudgeCourtTime]
			  ,[NAMEMEM]
			  ,[RESUS_PATIENT]
			  ,[RESUS_PATIENTDET]
			  ,[PatientDiscussionTime]
			  ,[HAVECAP]
			  ,[APPOINTWA]
			  ,[CLINPROB]
			  ,[DNARNAME1]
			  ,[DNARNAME2]
			  ,[DNARNAME3]
			  ,[DNARDATE2]
			  ,[DNARDATE3]
			  ,[DNARDATE4]
			  ,[DS1500]
			  ,[EQUIP]
			  ,[EQUIP_DETAIL]
			  ,[FAM_SUPPORT]
			  ,[FAM_SUPPORT_Y]
			  ,[HOMECARE]
			  ,[HOMECARE_DET]
			  ,[CAREPLAN]
			  ,[CAREPLAN_DETAIL]
			  ,[Anticoags]
			  ,[Insulin]
			  ,[MedListLocation]
			  ,[OPIOID]
			  ,[MED_OTH]
			  ,[Steroids]
			  ,[CULTURAL]
			  ,[FAMILY_AWAR]
			  ,[WISHES]
			  ,[WISHES_YES]
			  ,[PERCARE_PLAN]
			  ,[PPC]
			  ,[PPDDiscuss]
			  ,[PlaceCareDet]
			  ,[PPD1]
			  ,[PPCDiscuss]
			  ,[PlaceDeath1Det]
			  ,[PPD2]
			  ,[PlaceDeath2Det]


		 FROM [ETL_Local_PROD].[dbo].[AT_Patient_General]

		 )d


		 	update r
		set CCG  = REPLACE(CCG, ' CCG', '')

		from [ETL_Local_PROD].[dbo].[AT_Patient_General_Historic_Inc] r

 
		update r
		set CCG  = REPLACE(CCG, 'Cross Border: ', '')

		from [ETL_Local_PROD].[dbo].[AT_Patient_General_Historic_Inc]r
		where charindex('Cross Border: ',CCG)>0