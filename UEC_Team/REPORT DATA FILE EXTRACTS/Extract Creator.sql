 
 
 IF OBJECT_ID('[PATLondon].[MH_ECDS_Report_Data_2_Feed]') IS NOT NULL 
 dROP TABLE [PATLondon].[MH_ECDS_Report_Data_2_Feed]

SELECT  [Gender]
      ,[Age Group]
      ,[Known to MH Services Flag]
	  ,[KnownInLast24Months]
	  ,[PreviouslyKnown]
      ,[ED Presentation within 28 days of Completed IP SPell]
      ,[Der_Provider_Name]
      ,[Arrival Mode]
      ,[Provider ICB]
      ,b.[Month Start Date] as  [Arrival Month]
      ,[ArrivalDate FY]
      ,[Time Grouper]
      ,[Local MH Trust]
	  ,case 
	  when [Days between Completed IP Spell and ED Presentation] >14 and [Days between Completed IP Spell and ED Presentation] <=28 then 'Within 28'
	  when [Days between Completed IP Spell and ED Presentation] <=14 and [Days between Completed IP Spell and ED Presentation] >7 then 'Within 14'
	  when [Days between Completed IP Spell and ED Presentation] <8 and [Days between Completed IP Spell and ED Presentation] >= 0 then 'Within 7'
	  Else null
      end as[IP Spell Time Grouper]
      ,[Reduction in Inappropriate Flag]
      ,[Chief Complaint Flag]
      ,[Injury Flag]
      ,[Injury Intent Flag]
      ,[Diagnosis Flag]
      ,[mh primary diagnosis description]
      ,case
	  when [Mental Health Presentation Flag] = 0 then 'Physical Presentation'
	  else 'MH Presentation'
	  end as [Presentation Type]
      ,[Self Harm Flag]
      ,count([Unique Record ID]) as [Total]

	  into [PATLondon].[MH_ECDS_Report_Data_2_Feed]

  FROM [PATLondon].[ECDS_All_Presentations_London]a
  inner join [PATLondon].[DIM_Date]b on b.[Calendar Day] = a.Arrival_Date

  where Arrival_Date >= '2023-04-01'

group by

[Gender]
      ,[Age Group]
      ,[Known to MH Services Flag]
	  ,[KnownInLast24Months]
	  ,[PreviouslyKnown]
      ,[ED Presentation within 28 days of Completed IP SPell]
      ,[Der_Provider_Name]
      ,[Arrival Mode]
      ,[Provider ICB]
      ,b.[Month Start Date]  
      ,[ArrivalDate FY]
      ,[Time Grouper]
      ,[Local MH Trust]
	  ,case 
	  when [Days between Completed IP Spell and ED Presentation] >14 and [Days between Completed IP Spell and ED Presentation] <=28 then 'Within 28'
	  when [Days between Completed IP Spell and ED Presentation] <=14 and [Days between Completed IP Spell and ED Presentation] >7 then 'Within 14'
	  when [Days between Completed IP Spell and ED Presentation] <8 and [Days between Completed IP Spell and ED Presentation] >= 0 then 'Within 7'
	  Else null
      end  
      ,[Reduction in Inappropriate Flag]
      ,[Chief Complaint Flag]
      ,[Injury Flag]
      ,[Injury Intent Flag]
      ,[Diagnosis Flag]
      ,[mh primary diagnosis description]
      ,case
	  when [Mental Health Presentation Flag] = 0  then 'Physical Presentation'
	  else 'MH Presentation'
	  end  
      ,[Self Harm Flag]

	   --select *  FROM [PATLondon].[MH_ECDS_Report_Data_2_Feed] order by [Arrival Month]
	   --select *  FROM  [PATLondon].[ECDS_All_Presentations_London]


  ---IP
      
SELECT  [UniqMonthID]
      ,[UniqHospProvSpellID]
      ,[UniqSubmissionID]
      ,[Person_ID]
      ,[Der_Person_ID]
      ,[Der_Pseudo_NHS_Number]
      ,[ODS_GPPrac_OrgCode]
      ,[GP_Practice_Name]
      ,[ODS_GPPrac_PostCode]
       ,[Local Authority Name] as [GP Local Authority]
      ,[2019 GP CCG NAME]
      ,[Patient GP Practice Region]
      ,[Gender]
      ,[Ethnic Category]
      ,[Broad Ethnic Category]
      ,[Derived Broad Ethnic Category]
      ,[patients postcode ccg]
      ,[CCG name by PatPostcode]
      ,[STP name by PatPostcode]
      ,[Region name by PatPostcode]
      ,[LSOA2011]
      ,[Pat Postcode Lan Name]
      ,[Res MH Trust by PatPostcode]
      ,[ICB of Res MH Trust by PatPostcode]
      ,[ Borough Res MH Trust by PatPostcode]
      ,[OrgIDProv]
      ,[Provider_Name]
      ,[Provider_PostCode]
      ,[Provider ICS Full Name]
      ,[Provider ICS Abbrev]
      ,[Provider Region Name]
      ,[Admission Site Name]
      ,[Provider_Type]
      ,[Region_Code]
      ,[Region_Name]
      ,[CCGCode]
      ,[CCG name]
      ,[STPCode]
      ,[STP name]
      ,[AgeBand]
      ,[AgeServReferRecDate]
      ,[AgeCat]
      ,[UniqServReqID]
      ,[ReferralRequestReceivedDate]
      ,[RefMonth]
      ,[Referring Organisation]
      ,[Referring Org Type]
      ,[Referring Care Professional Staff Group]
      ,[Referral Source]
      ,[Primary Reason for Referral]
      ,[Clinical Priority]
      ,[Ethnic proportion per 100000 of London Borough 2020]
      ,[Ethnic proportion per 100000 of England 2020]
      ,[RecordNumber]
      ,[Provisional Diag Code]
      ,[Prov. Diag Desc]
      ,[Prov. Diag Chapter]
      ,[Primary Diag Code]
      ,[Prim. Diag Desc]
      ,[Prim. Diag Chapter]
      ,[Secondary Diag Code]
      ,[Sec. Diag Desc]
      ,[Sec. Diag Chapter]
      ,[StartDateHospProvSpell]
      ,[StartTimeHospProvSpell]
      ,[Adm_MonthYear]
      ,[SourceAdmCodeHospProvSpell]
      ,[SourceOfAdmission]
      ,[Der_AdmissionMethod]
      ,[HospitalBedTypeMH]
      ,[Specialised Service Code for Initial Ward Admission]
      ,[BedType_Category]
      ,[BedType]
      ,[SpecCommCode]
      ,[EstimatedDischDateHospProvSpell]
      ,[PlannedDischDateHospProvSpell]
      ,[Planned Discharge Destination]
      ,[DischDateHospProvSpell]
      ,[DischTimeHospProvSpell]
      ,[Discharge Destination]
      ,[RN]
      ,[loS Tranche]
      ,[Stranded_Status]
      ,[HOSP_LOS]
      ,[HOSP_LOS at Last Update for Incomplete Spells]
      ,[Der_HospSpellStatus]
      ,[Male Psychosis 18-44 Flag]
      ,[Male Personality Disorder 18-44 Flag]
      ,[BiPolar Flag]
      ,[UniqMHActEpisodeID]
      ,[SectionType]
      ,[NHS LEgal Status Description]
      ,[Legal Status Start Date]
      ,[Legal Status Start Time]
      ,[Legal Status End Date]
      ,[Legal Status End Time]
      ,[Linked S136 Prior to Adm]
      ,[Known to MH Services Flag]
      ,[AWOL FLag]
      ,[AWOL Wardstay ID]
      ,[Admission Type]
      ,[NewLOS]
      ,[KnownInLast24Months]
      ,[PreviouslyKnown]
  FROM [PATLondon].[MH_Spells]
  where [Provider ICS Abbrev] is not null