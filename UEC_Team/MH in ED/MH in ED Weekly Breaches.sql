 

 
	  exec [PATLondon].[SP_Update GP Details]

		go 

		IF OBJECT_ID('Tempdb..#Prov') IS NOT NULL 
		dROP TABLE  #Prov
		select
		Distinct
		Parent_Organisation_Code,
		[Parent Organisation Name],
		[Parent Organisation Postcode],
		REPLACE([Parent Organisation Postcode], ' ', '') as [Parent Organisation Postcode No Gaps],
		[Parent Organisation Postcode District],
		[Parent Organisation yr2011 LSOA],
		[MH Trust Flag],
		[MH Provider Abbrev]

		into #Prov

		from [PATLondon].[Ref_Trusts_and_Sites]  a  with (nolock) 
					
 IF OBJECT_ID('Tempdb..#SNOMED') IS NOT NULL 
dROP TABLE #SNOMED	

select *
into #SNOMED
from
(

SELECT  [Sheet_Name]
 
      ,[ECDS_Group1]
 
 
      ,[SNOMED_Code]
      ,[SNOMED_Description]
	    ,		ROW_NUMBER() OVER (
		PARTITION BY  [SNOMED_Code]
		ORDER BY  [Created_Date]desc) as RowOrder
      ,[SNOMED_TERM]
       
      ,[Valid_From]
      ,[Valid_To]
      
  FROM [UKHD_ECDS_TOS].[Code_Sets]
  --where [SNOMED_Code]= '422400008'

 where [SNOMED_Description] is not null

 
 )d where RowOrder = 1				




		Declare 
			@StartDate date,  
			@EndDate date 

			--set @StartDate = DATEADD(wk,-2,  GETDATE()) 
			 
			set @StartDate ='2024-05-06'
			
			set @EndDate ='2024-05-12'
		





			IF OBJECT_ID('Tempdb..#Temp11') IS NOT NULL 
			drop table #Temp11

			SELECT 
	
			a.Generated_Record_ID
			,a.Der_Pseudo_NHS_Number
			,EC_Ident
			,Unique_CDS_identifier
			,sex
			,Ethnic_Category
			,Index_Of_Multiple_Deprivation_Decile
			,Index_Of_Multiple_Deprivation_Decile_Description
			,Rural_Urban_Indicator
			,gp.GP_Name as [Practice Name]
			,gp.PCDS_NoGaps as [GP Practice PostCode No Gaps] 
			,gp.[2019_CCG_Name] as [Patient GP Practice 2019 CCG Code]
			,GP.[Local_Authority] as [Patient GP Local Authority Name]
	 
			,GP.GP_Region_Name as [Patient GP Practice Region]
			,gp.Lower_Super_Output_Area_Code as [Patient GP 2011_LSOA]
			,gp.Middle_Super_Output_Area_Code as [Patient GP 2011_MS0A]
			,Accommodation_Status_SNOMED_CT
			,Attendance_LSOA_Provider_Distance  --The distance, in miles, between the LSOA centroid of the patient's submitted postcode and the LSOA centroid of the provider.
			,Attendance_LSOA_Treatment_Site_Distance  --The distance between the LSOA centroid of the patient's submitted postcode and the LSOA centroid of the site of treatment.
			,convert(varchar(50),a.Local_Patient_ID) +'-'+ 
			a.Der_Provider_Site_Code+'-'+ 
			convert(varchar(10), CONVERT(DATE,a.Arrival_Date,101))+':'+ 
			CONVERT(VARCHAR(8),a.Der_EC_Arrival_Date_Time,108)
			as Der_EC_Ident
			,Patient_Type
			,a.Der_Provider_Code 
			--local patient ID, provider code and activity date/time.
			,COALESCE(o1.Organisation_Name,'Missing/Invalid') AS Der_Provider_Name
			,a.Der_Provider_Site_Code 
			,COALESCE(o2.Organisation_Name,'Missing/Invalid') AS Der_Provider_Site_Name
			,COALESCE(o3.Region_Code,'Missing/Invalid') AS Region_Code --- regions taken from CCG of provider rather than CCG of residence
			,COALESCE(o3.Region_Name,'Missing/Invalid') AS Region_Name
			,COALESCE(cc.New_Code,a.Attendance_HES_CCG_From_Treatment_Site_Code,'Missing/Invalid') AS CCGCode
			,COALESCE(o3.Organisation_Name,'Missing/Invalid') AS [CCG name]
			,COALESCE(o3.STP_Code,'Missing/Invalid') AS STPCode
			,COALESCE(o3.STP_Name,'Missing/Invalid') AS [STP name]
			,DATEADD(MONTH, DATEDIFF(MONTH, 0, Arrival_Date), 0) MonthYear
			,a.Arrival_Date 
			,DATEPART(HOUR, a.Arrival_Time) as Arrival_Hour 
			,CAST(ISNULL(a.Arrival_Time,'00:00:00') AS datetime) + CAST(a.Arrival_Date AS datetime) AS ArrivalDateTime
			,a.EC_Departure_Date 
			,a.EC_Departure_Time
			,CAST(ISNULL(a.EC_Departure_Time,'00:00:00') AS datetime) + CAST(a.EC_Departure_Date AS datetime) AS DepartureDateTime
			,a.EC_Departure_Time_Since_Arrival
			,a.EC_Initial_Assessment_Time_Since_Arrival
			,a.EC_Chief_Complaint_SNOMED_CT
			,cp.SNOMED_Description  as ChiefComplaintDescription
			,a.EC_Injury_Intent_SNOMED_CT
			,ii.SNOMED_Description  as InjuryIntentDescription
			,a.Der_EC_Diagnosis_All
			,COALESCE(LEFT(a.Der_EC_Diagnosis_All, NULLIF(CHARINDEX(',',a.Der_EC_Diagnosis_All),0)-1),a.Der_EC_Diagnosis_All) AS PrimaryDiagnosis
			,pd.SNOMED_Description  as DiagnosisDescription
			,a.Age_At_Arrival 
			,case when a.Age_At_Arrival <= 18 then 'CYP' else 'Adult' end as [Age Group]
			,CASE 
			WHEN a.Age_At_Arrival BETWEEN 0 AND 11 THEN '0-11'  
			WHEN a.Age_At_Arrival BETWEEN 12 AND 17 THEN '12-17'
			WHEN a.Age_At_Arrival BETWEEN 18 AND 25 THEN '18-25'
			WHEN a.Age_At_Arrival BETWEEN 26 AND 64 THEN '26-64' 
			WHEN a.Age_At_Arrival >= 65 THEN '65+' 
			ELSE 'Missing/Invalid' 
			END as AgeCat 
			,a.Discharge_Destination_SNOMED_CT
	
			,CASE WHEN cp.SNOMED_Description  IS NOT NULL THEN 1 ELSE 0 END as Val_ChiefComplaint
			,CASE WHEN a.EC_Injury_Date IS NOT NULL THEN 1 ELSE 0 END as InjuryFlag
			,CASE WHEN a.EC_Injury_Date IS NOT NULL AND ii.SNOMED_Description  IS NOT NULL THEN 1 ELSE 0 END as Val_InjuryIntent
			,CASE WHEN pd.SNOMED_Description IS NOT NULL THEN 1 ELSE 0 END as Val_Diagnosis
			,CASE 
			WHEN EC_Chief_Complaint_SNOMED_CT IN ('248062006' --- self harm
			,'272022009' --- depressive feelings 
			,'48694002' --- feeling anxious 
			,'248020004' --- behaviour: unsual 
			,'6471006' -- feeling suicidal
			,'7011001') THEN 1  --- hallucinations/delusions 
			WHEN EC_Injury_Intent_SNOMED_CT = '276853009' THEN 1 --- self inflicted injury 
			WHEN COALESCE(LEFT(Der_EC_Diagnosis_All, NULLIF(CHARINDEX(',',Der_EC_Diagnosis_All),0)-1),Der_EC_Diagnosis_All) 
			IN ( 
			'52448006' --- dementia
			,'2776000' --- delirium 
			,'33449004' --- personality disorder
			,'72366004' --- eating disorder
			,'197480006' --- anxiety disorder
			,'35489007' --- depressive disorder
			,'13746004' --- bipolar affective disorder
			,'58214004' --- schizophrenia
			,'69322001' --- psychotic disorder
			,'397923000' --- somatisation disorder
			,'30077003' --- somatoform pain disorder
			,'44376007' --- dissociative disorder
			,'17226007' ---- adjustment disorder
			,'50705009'---- factitious disorder
			) 			THEN 1 
			ELSE 0 
			END as MH_Flag 
			,CASE 
			WHEN EC_Injury_Intent_SNOMED_CT = '276853009' THEN 1
			WHEN EC_Chief_Complaint_SNOMED_CT = '248062006' THEN 1
			ELSE 0 
			END as SelfHarm_Flag 
			--,a.Discharge_Destination_SNOMED_CT
			--,a.EC_Arrival_Mode_SNOMED_CT
			INTO #Temp11

			FROM  [MESH_ECDS].[EC_Core] a

			left join #SNOMED cp on cp.SNOMED_Code = a.EC_Chief_Complaint_SNOMED_CT	
						and a.EC_Chief_Complaint_SNOMED_CT IN
												(
												'248062006' --- self harm
												,'272022009' --- depressive feelings 
												,'48694002' --- feeling anxious 
												,'248020004' --- behaviour: unsual 
												,'6471006' -- feeling suicidal
												,'7011001'
												)  --- hallucinations/delusions 

			left join #SNOMED  ii  on ii.SNOMED_Code = a.EC_Injury_Intent_SNOMED_CT
						and a.EC_Injury_Intent_SNOMED_CT = '276853009' 
			left join #SNOMED  pd  on pd.SNOMED_Code =  COALESCE(LEFT(a.Der_EC_Diagnosis_All, NULLIF(CHARINDEX(',',a.Der_EC_Diagnosis_All),0)-1),a.Der_EC_Diagnosis_All) 
			and COALESCE(LEFT(a.Der_EC_Diagnosis_All, NULLIF(CHARINDEX(',',a.Der_EC_Diagnosis_All),0)-1),a.Der_EC_Diagnosis_All)  
			IN ('52448006' --- dementia
			,'2776000' --- delirium 
			,'33449004' --- personality disorder
			,'72366004' --- eating disorder
			,'197480006' --- anxiety disorder
			,'35489007' --- depressive disorder
			,'13746004' --- bipolar affective disorder
			,'58214004' --- schizophrenia
			,'69322001' --- psychotic disorder
			,'397923000' --- somatisation disorder
			,'30077003' --- somatoform pain disorder
			,'44376007' --- dissociative disorder
			,'17226007' ---- adjustment disorder
			,'50705009') 

			left join
			(
			SELECT  
			distinct 
			[Organisation_Code]
			,[Organisation_Name]
			FROM  [UKHD_ODS].[All_Providers_SCD_1]
			where   [Is_Latest] = 1
			)o1 ON a.Provider_Code = o1.Organisation_Code --- providers  
			left join
			(
			SELECT  
			distinct
			[Organisation_Code]
			,[Organisation_Name]
			FROM [UKHD_ODS].[NHS_Trust_Sites_Assets_And_Units_SCD_1]
			where [Is_Latest] = 1
			) o2 ON a.Site_Code_of_Treatment = o2.Organisation_Code --- sites
			LEFT JOIN  [Internal_Reference].[ComCodeChanges_1] cc ON a.Attendance_HES_CCG_From_Treatment_Site_Code = cc.Org_Code 
			LEFT JOIN [Reporting_UKHD_ODS].[Commissioner_Hierarchies] o3 ON COALESCE(cc.New_Code,a.Attendance_HES_CCG_From_Treatment_Site_Code) = o3.Organisation_Code --- CCG / STP / Region 
			LEft join [PATLondon].[Ref_GP_Data] gp on gp.GP_Practice_Code = coalesce(a.PDS_General_Practice_Code,a.GP_Practice_Code )
			WHERE a.EC_Department_Type = '01' --- Type 1 EDs only 
			AND a.Arrival_Date between @StartDate and @EndDate
			--a.Arrival_Date >= @StartDate
			AND (EC_Discharge_Status_SNOMED_CT IS NULL OR EC_Discharge_Status_SNOMED_CT  NOT IN ('1077031000000103','1077781000000101', '63238001')) --exclude streamed and Dead on arrival
			AND ([EC_AttendanceCategory] IS NULL OR [EC_AttendanceCategory] in ('1','2','3'))   --exclude follow ups and Dead on arrival
			and COALESCE(o3.Region_Name,'Missing/Invalid') = 'London'
			-- AND a.EC_Departure_Date < GetDate() -- remove attendances that depart in the future


 

		IF OBJECT_ID('Tempdb..#Final') IS NOT NULL 
		drop table #Final
		select 
		a.*,
		gen.Main_Description as Gender 
		,ec.[Category] as [Broad Ethnic Category] 
		,ec.Main_Description as [Ethnic Category Desc] 
		,case
		when h.Borough is null and a.[Patient GP Local Authority Name] is not null then 'Out of London Borough'
		when h.Borough is null and a.[Patient GP Local Authority Name] is null then 'GP Practice Unknown'
		end as [Borough Type] 
		,h.Borough  as [London GP Local Authority] 
		,h.ICS as [Patient ICS] 
		,h.Trust as [Local MH Trust]  
		,case 
		when a.EC_Departure_Time_Since_Arrival >= 0 AND a.EC_Departure_Time_Since_Arrival <= 240 THEN '0-4hrs'
		when a.EC_Departure_Time_Since_Arrival is null then '0-4'
		when a.EC_Departure_Time_Since_Arrival > 240 and a.EC_Departure_Time_Since_Arrival <= 720 THEN '5-12hrs' 
		when a.EC_Departure_Time_Since_Arrival > 720 and a.EC_Departure_Time_Since_Arrival <= 1440 then '12-24hrs'
		when a.EC_Departure_Time_Since_Arrival > 1440 then  '>24hrs' 
		else 'Not recorded'
		end as TimeGrouper 
		,CASE WHEN EC_Departure_Time_Since_Arrival < (24*60) THEN EC_Departure_Time_Since_Arrival ELSE 0 END as TotalTimeInED 
		,CASE WHEN EC_Departure_Time_Since_Arrival < (24*60) AND EC_Departure_Time_Since_Arrival > (60*6) THEN 1 ELSE 0 END as [6 Hour Breach]  
		,CASE WHEN EC_Departure_Time_Since_Arrival < (24*60) AND EC_Departure_Time_Since_Arrival > (60*6) THEN (EC_Departure_Time_Since_Arrival - (60*6)) ELSE 0 END AS [Time over 6 Hours] 
		,CASE WHEN EC_Departure_Time_Since_Arrival < (24*60) AND EC_Departure_Time_Since_Arrival > (60*12) THEN 1 ELSE 0 END as [12 Hour Breach]  
		,CASE WHEN EC_Departure_Time_Since_Arrival < (24*60) AND EC_Departure_Time_Since_Arrival > (60*12) THEN EC_Departure_Time_Since_Arrival - (60*12) ELSE 0 END AS [Time over 12 Hours] 
		,CASE WHEN EC_Departure_Time_Since_Arrival >= (24*60) THEN 1 ELSE 0 END as [24hrs_breach] 
		,CASE WHEN EC_Departure_Time_Since_Arrival > 720 THEN 1 ELSE 0 END as [12hrs_breach]  -- added to include 5-12 hrs breaches
		,CASE when EC_Departure_Time_Since_Arrival > 240 and a.EC_Departure_Time_Since_Arrival <= 720 THEN 1 else 0 end as'5-12hrs breach'  -- added to include 5-12 hrs breaches
		,cASE When EC_Departure_Time_Since_Arrival >= 0 AND EC_Departure_Time_Since_Arrival <= 240 THEN 1 else 0 end as '0-4hrs breach' -- added to include 0-4 hrs breaches
		into #Final

		from #Temp11 a
		left join [UKHD_Data_Dictionary].[Ethnic_Category_Code_SCD_1]ec on ec.[Main_Code_Text] = a.Ethnic_Category and ec.is_latest = 1
		left join [UKHD_Data_Dictionary].[Person_Gender_Code_SCD_1]gen  with (nolock)on gen.[Main_Code_Text]  = a.Sex
		left join #Prov pp on pp.Parent_Organisation_Code = a.Der_Provider_Code
		left join [PATLondon].[Ref_PostCode_to_Local_Authority]la on la.[PostCode No Gaps]= pp.[Parent Organisation Postcode No Gaps]
		left join [PATLondon].[Ref_Borough_Trust_Mapping]h on h.Borough = la.Name
	 
 

		where  Region_Name = 'London'
		and a.MH_Flag = 1
		and a.EC_Departure_Time_Since_Arrival > =0 
 

 --select * from #Final
		--select 
		--convert(date,Arrival_Date) as [Arrival Date],
		--Der_Provider_Name as  ProviderName,
		--Der_Provider_Site_Name as ProviderSite,
		--1 as [12hrBreach],
		--null as [Gap 1],
		--null as [Gap 2],
		--[Age Group],
		--'12 Plus Breach' as [Breach Flag],
		--[Local MH Trust] as MHTrust,
		--[Patient ICS] as MHICS,
		--coalesce([London GP Local Authority],[Borough Type]) as [Borough],
		--1 as [Diagnosis Level],
		--Unique_CDS_identifier,
		--EC_Ident,
		--case when ([Local MH Trust] is null) then 'Out of London CCG/GP Practice Unknown'
		--   --   when (coalesce([London GP Local Authority],[Borough Type])='GP Practice Unknown') then 'GP Practice Unknown'
		--	  else 'London CCG'
		--	  end as 'LondonCCG/OutofLondonCCG'

		
		--from temp.Final
		--order by 
		--ArrivalDateTime 
		 

		 

 
		select 
		convert(date,Arrival_Date) as [Arrival Date],
		Der_Provider_Name as  ProviderName,
		Der_Provider_Site_Name as ProviderSite,
		1 as [12hrBreach],
		null as [Gap 1],
		null as [Gap 2],
		[Age Group],
		'12 Plus Breach' as [Breach Flag],
		Case when ([Local MH Trust] is null ) then 'Others'
		else [Local MH Trust]
		end as MHTrust,

		case when ([Patient ICS] is null) then 'Out of London CCG/GP Practice Unknown'
		else [Patient ICS]
		end as  MHICS,
		coalesce([London GP Local Authority],[Borough Type]) as [Borough],
		1 as [Diagnosis Level],
		Unique_CDS_identifier,
		EC_Ident,
		case when ([Local MH Trust] is null) then 'Out of London CCG/GP Practice Unknown'
		   --   when (coalesce([London GP Local Authority],[Borough Type])='GP Practice Unknown') then 'GP Practice Unknown'
			  else 'London CCG'
			  end as 'LondonCCG/OutofLondonCCG'
			,[12hrs_breach]
			,[5-12hrs breach]
			,[0-4hrs breach]
--,Responsible_CCG_From_General_Practice
		
		from #Final
	where Der_Provider_Site_Name <> 'Missing/Invalid'
				order by ArrivalDateTime 
		 

--		 select sum([5-12hrs breach]) as [5-12hrs]
--		 ,sum([12hrs_breach]) as [12hr breaches]
--		 ,sum([5-12hrs breach])+sum([12hrs_breach]) as [over 5hrs breaches]
--		 ,SUM ([0-4hrs breach]) as [0-4hrs]
--		 ,COUNT(*) as Total
--		 from  temp.Final
--		 where Der_Provider_Site_Name <> 'Missing/Invalid'
	
	
--		select 
--		convert(date,Arrival_Date) as [Arrival Date],
--		Der_Provider_Name as  ProviderName,
--		Der_Provider_Site_Name as ProviderSite,
	
--		[Age Group],
	
--		Unique_CDS_identifier,
--			case when ([Local MH Trust] is null) then 'Out of London CCG/GP Practice Unknown'
--		   --   when (coalesce([London GP Local Authority],[Borough Type])='GP Practice Unknown') then 'GP Practice Unknown'
--			  else 'London CCG'
--			  end as 'LondonCCG/OutofLondonCCG'
--			,[12hrs_breach]
			
----,Responsible_CCG_From_General_PracticeSS
--		,[Patient ICS]
--		from temp.Final
--	-- where [5-12hrs breach]=1
--	where ([12hrs_breach])=1
--				order by ArrivalDateTime 
		 

		 
		 select SUM ([0-4hrs breach]) as [0-4hrs]
		 ,sum([5-12hrs breach]) as [5-12hrs]
		 ,sum([12hrs_breach]) as [12hr breaches]
		 ,COUNT(*) as Total
		 from  temp.Final
		 where Der_Provider_Site_Name <> 'Missing/Invalid'
	
	
		select
		ArrivalDateTime as [Arrival DateTime]
	--	convert(date,Arrival_Date) as [Arrival Date],
		,Der_Provider_Name as  ProviderName,
		Der_Provider_Site_Name as ProviderSite,
	
		[Age Group],
	
		Unique_CDS_identifier,
			case when ([Local MH Trust] is null) then 'Out of London CCG/GP Practice Unknown'
		   --   when (coalesce([London GP Local Authority],[Borough Type])='GP Practice Unknown') then 'GP Practice Unknown'
			  else 'London CCG'
			  end as 'LondonCCG/OutofLondonCCG'
			  ,EC_Departure_Time_Since_Arrival as [Total time in ED]
			  ,1 as [MH Breach]
			 ,[0-4hrs breach] as [0-4hrs]
			 ,[5-12hrs breach] as [5-12hrs]
			,[12hrs_breach]  as [Over 12hr]
			
--,Responsible_CCG_From_General_PracticeSS
		,ISNULL([Patient ICS],'Out Of London') as [Patient ICB]

		,Case when ([Local MH Trust] is null ) then 'Others'
		else [Local MH Trust]
		end as MHTrust,

		case when ([Patient ICS] is null) then 'Out of London CCG/GP Practice Unknown'
		else [Patient ICS]
		end as  MHICB,
		coalesce([London GP Local Authority],[Borough Type]) as [Patient Borough]
		
		from temp.Final
	-- where [5-12hrs breach]=1
	where Der_Provider_Site_Name <> 'Missing/Invalid'
				order by ArrivalDateTime 