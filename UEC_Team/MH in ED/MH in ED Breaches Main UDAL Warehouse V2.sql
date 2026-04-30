 

	
			
			if object_id('Tempdb..#TempGP1') is not null				
			drop table #TempGP1
			SELECT 
			DISTINCT
			B.[GP_Code] AS GP_CODE
			,B.[GP_PCN_Code] AS GP_PCN_Code
			,B.[GP_PCN_Name] AS GP_PCN_Name
			,B.[GP_STP_Code] AS [GP_STP_Code]
			,Replace(B.[GP_STP_Name],' INTEGRATED CARE BOARD','')   AS GP_STP_Name
			,B.[GP_Region_Code] AS GP_Region_Code
			,Replace(B.[GP_Region_Name],' COMMISSIONING REGION','') AS GP_Region_Name
			,C.PRACTICE AS Practice_code	
			,C.[CCG2019_20_Q4] AS CCG1920
			,Replace(D.Organisation_Name,' CCG','')  AS [New CCG]
				 
			into #TempGP1

			FROM  [Reporting_UKHD_ODS].[GP_Hierarchies_All] B 	
			LEFT JOIN  [Internal_Reference].[RightCare_practice_CCG_pcn_quarter_lookup] C ON B.[GP_Code] COLLATE DATABASE_DEFAULT = C.Practice COLLATE DATABASE_DEFAULT
			LEFT JOIN [Reporting_UKHD_ODS].[Commissioner_Hierarchies] D ON D.Organisation_Code  COLLATE DATABASE_DEFAULT  = C.[CCG2019_20_Q4]  COLLATE DATABASE_DEFAULT
			
 

			if object_id('Tempdb..#TempGP2') is not null				
			drop table #TempGP2
			SELECT 
			DISTINCT 
			GP_PCN_Code, 
			GP_PCN_Name, 
			[New CCG], 
			COUNT(GP_PCN_NAME) AS GPS 
			into #TempGP2
			FROM #TempGP1 x
			GROUP BY 
			GP_PCN_Code, 
			GP_PCN_Name, 
			[New CCG]

			if object_id('Tempdb..#TempGP3') is not null				
			drop table #TempGP3
			SELECT 
			GP_PCN_Code, 
			GP_PCN_Name, 
			[New CCG],
			ROW_NUMBER() OVER (PARTITION BY GP_PCN_Code, GP_PCN_Name ORDER BY GPS DESC) AS LA_ORDER
			into #TempGP3
			FROM  #TempGP2 


 

					
			if object_id('Tempdb..#TempGP4') is not null				
			drop table #TempGP4
			SELECT 
			DISTINCT
			B.[GP_Code] AS GP_CODE
			,b.GP_Name
			,B.[GP_PCN_Code] AS GP_PCN_Code
			,B.[GP_PCN_Name] AS GP_PCN_Name
			,B.[GP_STP_Code] AS [GP_STP_Code]
			,Replace(B.[GP_STP_Name],' INTEGRATED CARE BOARD','')   AS GP_STP_Name
			,B.[GP_Region_Code] AS GP_Region_Code
			,Replace(B.[GP_Region_Name],' COMMISSIONING REGION','') AS GP_Region_Name
			,C.PRACTICE AS Practice_code	
			,C.[CCG2019_20_Q4] AS CCG1920
			,REPLACE([GP_Postcode] , ' ', '') as [PCDS_NoGaps]   
			,REPLACE(left([GP_Postcode],7) , ' ', '') as [PCDS_7] 
			,REPLACE(left([GP_Postcode],6) , ' ', '') as [PCDS_6] 
			,REPLACE(left([GP_Postcode],5) , ' ', '')as [PCDS_5] 
			,REPLACE(left([GP_Postcode],4)  , ' ', '')as [PCDS_4] 
			,ltrim(rtrim(left( [GP_Postcode] ,3))) as [PCDS_3] 
					 
			,cast(null as varchar(255)) as [2019_CCG_Name]
			,Z.[New CCG] AS [New CCG]
			,ROW_NUMBER() OVER (PARTITION BY GP_CODE ORDER BY CASE WHEN GP_PCN_Rel_End_Date IS NULL THEN 1 ELSE 0 END DESC, GP_PCN_Rel_End_Date DESC) AS GP_ORDER,
			cast(null as varchar(9)) as [Lower_Super_Output_Area_Code],
			cast(null as varchar(80)) as [Lower_Super_Output_Area_Name],
			cast(null as varchar(9)) as [Middle_Super_Output_Area_Code],
			cast(null as varchar(80)) as [Middle_Super_Output_Area_Name],
			cast(null as varchar(9)) as [Longitude],
			cast(null as varchar(9)) as [Latitude],
			cast(null as varchar(40)) as [Spatial_Accuracy]
			into  #TempGP4
			FROM  [Reporting_UKHD_ODS].[GP_Hierarchies_All] B 
			LEFT JOIN  [Internal_Reference].[RightCare_practice_CCG_pcn_quarter_lookup] C ON B.[GP_Code] COLLATE DATABASE_DEFAULT = C.Practice COLLATE DATABASE_DEFAULT
			LEft Join #TempGP3 z on z.GP_PCN_CODE = B.GP_PCN_CODE
					 
			where z.LA_ORDER = 1
 



			update f

			set f.[Lower_Super_Output_Area_Code] = g.[Lower_Super_Output_Area_Code],
			f.[Lower_Super_Output_Area_Name] = g.[Lower_Super_Output_Area_Name],
			f.[Middle_Super_Output_Area_Code] = g.[Middle_Super_Output_Area_Code],
			f.[Middle_Super_Output_Area_Name] = g.[Middle_Super_Output_Area_Name],
			f.[Longitude] = g.[Longitude],
			f.[Latitude] = g.[Latitude],
			f.[Spatial_Accuracy] = g.[Spatial_Accuracy]

			from #TempGP4  f
			inner join [UKHD_Other].[National_Statistics_Postcode_Lookup_SCD] g on REPLACE([Postcode_1] , ' ', '') = f.[PCDS_NoGaps]

			 
			 --select * from #TempGP4
					if object_id('Tempdb..#TempGP6') is not null				
					drop table #TempGP6
					SELECT distinct
					REPLACE([Postcode_1] , ' ', '') as [PCDS_NoGaps]   
					,[Postcode_1]
					,[Postcode_2]
					,[Postcode_3]
      
					,[Local_Authority_Code]
					,[Local_Authority_Name]
					into #TempGP6
					FROM [UKHD_Other].[National_Statistics_Postcode_Lookup_SCD]
					where is_Latest = 1
					 
										
				if object_id('Tempdb..#TempGP7') is not null				
				drop table #TempGP7
				select
				GP_CODE
				,Practice_code as [GP_Practice_Code]	
				,GP_Name
				,GP_PCN_Code
				,GP_PCN_Name
				,[GP_STP_Code]
				,GP_STP_Name
				,GP_Region_Code
				,GP_Region_Name
							
				,CCG1920
				,b.[PCDS_NoGaps]   
				,[2019_CCG_Name]
				,la.[Local_Authority_Name] as  [Local_Authority]
				,[Lower_Super_Output_Area_Code]
				,[Lower_Super_Output_Area_Name]
				,[Middle_Super_Output_Area_Code]
				,[Middle_Super_Output_Area_Name]
				,[Longitude]
				,[Latitude]
				,[Spatial_Accuracy]
				into #TempGP7
			
				from #TempGP4 b
				left join #TempGP6 la on la.[PCDS_NoGaps] = b.[PCDS_NoGaps]

				where GP_ORDER = 1



			update gp
					set gp.[Local_Authority]	= coalesce(la2.[Local_Authority_Name],la3.[Local_Authority_Name],la4.[Local_Authority_Name],la5.[Local_Authority_Name])
			from #TempGP7 gp
			inner join #TempGP4 gp4 on gp4.Practice_code = gp.GP_Practice_Code
		left join #TempGP6 la2 on left(la2.[PCDS_NoGaps],7)  = gp4.PCDS_7
		left join #TempGP6 la3 on left(la3.[PCDS_NoGaps],6)  = gp4.PCDS_6
		left join #TempGP6 la4 on left(la4.[PCDS_NoGaps],5)  = gp4.PCDS_5
		left join #TempGP6 la5 on left(la5.[PCDS_NoGaps],4)  = gp4.PCDS_4
		where gp.[Local_Authority]is null

			--select top 500 * from [PATLondon].[Ref_PostCode_to_Local_Authority]
			--select * from [PATLondon].[Ref_GP_Data] where GP_Region_Name = 'London'and [Local_Authority] is null
			-- select * from  #TempGP7 where [PCDS_NoGaps]= 'SW59JA'
 


		IF OBJECT_ID('Tempdb..#TrustsandSItes') IS NOT NULL 
			dROP TABLE  #TrustsandSItes
			select 
			Distinct
			a.Parent_Organisation_Code,
			b.Organisation_Name as [Parent Organisation Name],
			b.Postcode as [Parent Organisation Postcode],
			left(b.Postcode,3) as [Parent Organisation Postcode District],
			c.[yr2011_LSOA] as  [Parent Organisation yr2011 LSOA],
			case when a.Parent_Organisation_Code in ('RAT','RKL','RPG','RQY','RRP','RV3','RV5','RWK','TAF')	then 1 else null end as [MH Trust Flag],
			cast(null as varchar(255)) as [MH Provider Abbrev],
			a.Organisation_Code as [Site Organisation Code],
			a.Organisation_Name as [Site Name],
			a.Postcode as [Site  Postcode],
			left(a.Postcode,3) as [Site Postcode District],
			d.[yr2011_LSOA] as  [Site yr2011 LSOA] 

			into #TrustsandSItes

			from [UKHD_ODS].[NHS_Trusts_SCD]b 
			left join [UKHD_ODS].[NHS_Trust_Sites_Assets_And_Units_SCD]a  on b.Organisation_Code = a.Parent_Organisation_Code and a.[Is_Latest] = 1
			left join [UKHD_ODS].[Postcode_Grid_Refs_Eng_Wal_Sco_And_NI_SCD]c on  REPLACE(c.[Postcode_8_chars] , ' ', '') =  REPLACE(b.Postcode , ' ', '') and c.[Is_Latest] = 1
			left join [UKHD_ODS].[Postcode_Grid_Refs_Eng_Wal_Sco_And_NI_SCD]d on  REPLACE(d.[Postcode_8_chars] , ' ', '') =  REPLACE(a.Postcode , ' ', '') and d.[Is_Latest] = 1
		
			where b.[Is_Latest] = 1
			
					update r
					    set r.[MH Provider Abbrev] = case
							when Parent_Organisation_Code = 'RAT' then 'NELFT'
							when Parent_Organisation_Code = 'RKL' then 'WLT'
							when Parent_Organisation_Code = 'RV3' then 'CNWL'
							when Parent_Organisation_Code = 'RPG' then 'OXLEAS'
							when Parent_Organisation_Code = 'RWK' then 'ELFT'
							when Parent_Organisation_Code = 'RRP' then 'BEH'
							when Parent_Organisation_Code = 'RQY' then 'SWLStG'
							when Parent_Organisation_Code = 'RV5' then 'SLAM'
							when Parent_Organisation_Code = 'TAF' then 'CANDI'
							else null end


					from #TrustsandSItes r

			 

						
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

		from #TrustsandSItes  a  with (nolock)

IF OBJECT_ID('Tempdb..#TrustBoroMap') IS NOT NULL 
dROP TABLE  #TrustBoroMap
 
select
716	as [Local Authority Code], 'NEL' as [ICS],	'NELFT'as [Trust], 	'Barking and Dagenham' as [Borough]
into #TrustBoroMap
union all
select 717,	'NCL',	'BEH',   	'Barnet'union all
select 718,	'SEL',	'Oxleas',	'Bexley'union all
select 719,	'NWL',	'CNWL',  	'Brent'union all
select 720,    'SEL',	'Oxleas',	'Bromley'union all
select 702,	'NCL',	'CANDI', 	'Camden'union all
select 714,	'NEL',	'ELFT',  	'City of London'union all
select 721,	'SEL',	'SLAM',  	'Croydon'union all
select 722,	'NWL',	'WLT',   	'Ealing'union all
select 723,	'NCL',	'BEH',  	'Enfield'union all
select 703,	'SEL',	'Oxleas',	'Greenwich'union all
select 704,	'NEL',  'ELFT',  	'Hackney'union all
select 705,	'NWL',	'WLT',   	'Hammersmith and Fulham'union all
select 724,	'NCL',	'BEH',   	'Haringey'union all
select 725,	'NWL',	'CNWL',  	'Harrow'union all
select 726,	'NEL',	'NELFT', 	'Havering'union all
select 727,	'NWL',	'CNWL',  	'Hillingdon'union all
select 728,	'NWL',	'WLT',   	'Hounslow'union all
select 706,	'NCL',	'CANDI', 	'Islington'union all
select 707,	'NWL',	'CNWL',  	'Kensington and Chelsea'union all
select 729,	'SWL',	'SWLStG',	'Kingston upon Thames'union all
select 708,	'SEL',	'SLAM',  	'Lambeth'union all
select 709,	'SEL',	'SLAM',  	'Lewisham'union all
select 730,	'SWL',	'SWLStG',	'Merton'union all
select 731,	'NEL',	'ELFT',  	'Newham'union all
select 732,	'NEL',	'NELFT', 	'Redbridge'union all
select 733,	'SWL',	'SWLStG',	'Richmond upon Thames'union all
select 710,	'SEL',	'SLAM',  	'Southwark'union all
select 734,	'SWL',	'SWLStG',	'Sutton'union all
select 711,	'NEL',	'ELFT',  	'Tower Hamlets'union all
select 735,	'NEL',	'NELFT', 	'Waltham Forest'union all
select 712,	'SWL',	'SWLStG',	'Wandsworth'

--select * from #TrustBoroMap


-----------------------------------------------------------------------------------------------------------------------


 
		Declare 
			@StartDate date,  
			@EndDate date 

			--set @StartDate = DATEADD(wk,-2,  GETDATE()) 
			 
			set @StartDate ='2025-01-10'
			
			set @EndDate ='2025-01-30'
		
			   

 IF OBJECT_ID('Tempdb..#tempED') IS NOT NULL 
dROP TABLE #tempED

	SELECT 

	convert(varchar(255),a.Generated_Record_ID)+'|'+ convert(varchar(255),Unique_CDS_identifier)+'|'+ convert(varchar(255),Attendance_Unique_Identifier) +'|'+convert(varchar(255),EC_Ident) as [Unique Record ID]
	,a.Der_Pseudo_NHS_Number
 

	,EC_Ident
	,a.Generated_Record_ID
	,Unique_CDS_identifier
	,Attendance_Unique_Identifier
 
	,ROW_NUMBER() OVER (
	PARTITION BY  Der_Pseudo_NHS_Number ,Attendance_Unique_Identifier, a.Arrival_Date
	ORDER BY  a.Arrival_Date,EC_Departure_Time desc 
	,case when DQ_Primary_Diagnosis_Valid = 'True' then 1 else 0 end desc
			,COALESCE(LEFT(a.Der_EC_Diagnosis_All, NULLIF(CHARINDEX(',',a.Der_EC_Diagnosis_All),0)-1),a.Der_EC_Diagnosis_All)   desc 
			,a.EC_Chief_Complaint_SNOMED_CT desc
			,a.EC_Injury_Intent_SNOMED_CT  desc
			,a.Der_EC_Diagnosis_All  desc
			) as RowOrder
  
	,case 
		when Sex = '0' then 'Unknown'
		when sex = '1' then 'Male'
		when sex = '2' then 'Female'
		when sex = '9' then 'Not specified'
	end as Gender
 
    ,a.Age_At_Arrival as [Age at Arrival]
	,case 
	when (a.Age_At_Arrival <= 18 and a.Age_At_Arrival is not null) then 'CYP' 
	when (a.Age_At_Arrival > 18 and a.Age_At_Arrival is not null) then 'Adult' 
	else 'Missing/Invalid' end as [Age Group]
	,CASE 
		WHEN a.Age_At_Arrival BETWEEN 0 AND 11 THEN '0-11'  
		WHEN a.Age_At_Arrival BETWEEN 12 AND 17 THEN '12-17'
		WHEN a.Age_At_Arrival BETWEEN 18 AND 25 THEN '18-25'
		WHEN a.Age_At_Arrival BETWEEN 26 AND 64 THEN '26-64' 
		WHEN a.Age_At_Arrival >= 65 THEN '65+' 
		ELSE 'Missing/Invalid' 
	END as AgeCat 
	,ec.[Category] as [Broad Ethnic Category] 
	,ec.Main_Description as [Ethnic Category] 
	,case 
	when (ec.Main_Description = '' OR ec.Main_Description = 'Not stated' OR ec.Main_Description = 'Not known' OR  ec.Main_Description is null) then 'Not Known / Not Stated / Incomplete'
	when ec.Category = 'Asian or Asian British' then 'Asian'
	when ec.Category = 'Black or Black British' then 'Black'
	when ec.Main_Description in ('mixed','Any other ethnic group','White & Black Caribbean','Any other mixed background','Chinese') then 'Mixed/ Other'
	ELSE ec.[Category]
	END as [Derived Broad Ethnic Category] 
	,Index_Of_Multiple_Deprivation_Decile
	,Index_Of_Multiple_Deprivation_Decile_Description
	,Rural_Urban_Indicator
	,cast(null as float) as [Ethnic proportion per 100000 of London Borough 2020] 

	,null as [Known to MH Services Flag] 
	--,null as [OLD Known to MH Services Flag] 
	,cast(null as date) as [Last Completed IP Spell] 
	,cast(null as varchar(255)) as [IP Spell Provider Name] 
	,cast(null as varchar(255)) as [UniqHospProvSpellID] 
	,cast(null as varchar(255)) as [IP Spell UniqServReqID]
	,null as [ED Presentation within 28 days of Completed IP SPell] 
	,null as [Days between Completed IP Spell and ED Presentation] 
 
	,coalesce(a.PDS_General_Practice_Code,a.GP_Practice_Code ) as  GP_Practice_Code
	,gp.GP_Name as [Practice Name]
	,gp.PCDS_NoGaps as [GP Practice PostCode No Gaps] 
	,gp.[2019_CCG_Name] as [Patient GP Practice 2019 CCG Code]
	,GP.[Local_Authority] as [Patient GP Local Authority Name]
	 
	,GP.GP_Region_Name as [Patient GP Practice Region]
	,case
		when gpTm.Borough is null and GP.[Local_Authority] is not null then 'Out of London Borough'
		when gpTm.Borough is null and GP.[Local_Authority] is null then 'GP Practice Unknown'
		when gpTm.Borough is not null then 'London patient'
		end as [Borough Type]
	,gpTm.ICS as [Patient ICS]
	,gpTm.Trust as [Local MH Trust]
	,gp.Lower_Super_Output_Area_Code as [Patient GP 2011_LSOA]
	,gp.Middle_Super_Output_Area_Code as [Patient GP 2011_MS0A]
	,ac.[SNOMED_Description] as  Accommodation_Status_SNOMED_CT


	,Attendance_Postcode_District
	,Attendance_HES_CCG_From_Treatment_Origin
	,Attendance_HES_CCG_From_Treatment_Site_Code
	,Attendance_LSOA_Provider_Distance  --The distance, in miles, between the LSOA centroid of the patient's submitted postcode and the LSOA centroid of the provider.
	,Attendance_LSOA_Treatment_Site_Distance  --The distance between the LSOA centroid of the patient's submitted postcode and the LSOA centroid of the site of treatment.
	,ats.[SNOMED_Description] as AttendanceSource
	,Patient_Type
	,a.Der_Provider_Code 
	--local patient ID, provider code and activity date/time.
	,COALESCE(o1.Organisation_Name,'Missing/Invalid') AS Der_Provider_Name
	,a.Der_Provider_Site_Code 
	,pp.[Parent Organisation Postcode] as   [Provider PostCode]  
	,pp.[Parent Organisation Postcode District] as [Provider Postcode District]
	,pp.[Parent Organisation yr2011 LSOA]  as [Provider 2011 LSOA]
	,COALESCE(o2.Organisation_Name,'Missing/Invalid') AS Der_Provider_Site_Name
	,COALESCE(o3.Region_Code,'Missing/Invalid') AS Provider_Region_Code --- regions taken from CCG of provider rather than CCG of residence
	,COALESCE(o3.Region_Name,'Missing/Invalid') AS Provider_Region_Name
	,COALESCE(cc.New_Code,a.Attendance_HES_CCG_From_Treatment_Site_Code,'Missing/Invalid') AS Provider_CCGCode
	,COALESCE(o3.Organisation_Name,'Missing/Invalid') AS [Provider_CCG name]
	,tm.ICS as [Provider ICB]
	,COALESCE(o3.STP_Code,'Missing/Invalid') AS Provider_STPCode
	,COALESCE(o3.STP_Name,'Missing/Invalid') AS  [Provider STP name]
	,DATEADD(MONTH, DATEDIFF(MONTH, 0, Arrival_Date), 0) as  [Month Year]
	 
	,a.Arrival_Date 


	,ad.[Fiscal_Year_Name] as [ArrivalDate FY] 



	,DATEPART(HOUR, a.Arrival_Time) as [Arrival Hour]
	,CAST(ISNULL(a.Arrival_Time,'00:00:00') AS datetime) + CAST(a.Arrival_Date AS datetime) AS [Arrival Date Time]
	,am.[SNOMED_Description]  as [Arrival Mode]
	,a.EC_Initial_Assessment_Date
	,a.EC_Initial_Assessment_Time
	,a.EC_Initial_Assessment_Time_Since_Arrival	
	,a.EC_Departure_Date 
	,a.EC_Departure_Time
	,EC_Departure_Time_Since_Arrival as [EC_Departure_Time_Since_Arrival]
	,case 
	when [EC_Departure_Time_Since_Arrival] >= 0 AND [EC_Departure_Time_Since_Arrival] <= 240 THEN '0-4'
	when [EC_Departure_Time_Since_Arrival] is null then '0-4'
	when [EC_Departure_Time_Since_Arrival] > 240 and [EC_Departure_Time_Since_Arrival] <= 720 THEN '5-12' 
	when [EC_Departure_Time_Since_Arrival] > 720 and [EC_Departure_Time_Since_Arrival] <= 1440 then '12-24'
	when [EC_Departure_Time_Since_Arrival] > 1440 and [EC_Departure_Time_Since_Arrival] <= 2880 then '24-48'
	when [EC_Departure_Time_Since_Arrival] > 2880 and [EC_Departure_Time_Since_Arrival] <= 4320 then '48-72'
	when [EC_Departure_Time_Since_Arrival] > 4320 then  '>72' 
	else 'Not recorded'
	end as [Time Grouper]
 
	,CASE WHEN EC_Departure_Time_Since_Arrival < (24*60) THEN EC_Departure_Time_Since_Arrival ELSE 0 END as TotalTimeInED
	,CASE WHEN EC_Departure_Time_Since_Arrival < (24*60) AND EC_Departure_Time_Since_Arrival > (60*6) THEN 1 ELSE 0 END as [6 Hour Breach] 
	,CASE WHEN EC_Departure_Time_Since_Arrival < (24*60) AND EC_Departure_Time_Since_Arrival > (60*6) THEN (EC_Departure_Time_Since_Arrival - (60*6)) ELSE 0 END AS [Time over 6 Hours]
	,CASE WHEN EC_Departure_Time_Since_Arrival < (24*60) AND EC_Departure_Time_Since_Arrival > (60*12) THEN 1 ELSE 0 END as [12 Hour Breach] 
	,CASE WHEN EC_Departure_Time_Since_Arrival < (24*60) AND EC_Departure_Time_Since_Arrival > (60*12) THEN EC_Departure_Time_Since_Arrival - (60*12) ELSE 0 END AS [Time over 12 Hours]
	,CASE WHEN EC_Departure_Time_Since_Arrival >= (24*60) THEN 1 ELSE 0 END as [24hrs_breach]
	,CASE WHEN EC_Departure_Time_Since_Arrival > 720 THEN 1 ELSE 0 END as [12hrs_breach] -- added to include 5-12 hrs breaches
	,CASE when EC_Departure_Time_Since_Arrival > 240 and a.EC_Departure_Time_Since_Arrival <= 720 THEN 1 else 0 end as'5-12hrs breach' -- added to include 5-12 hrs breaches
	,cASE When EC_Departure_Time_Since_Arrival >= 0 AND EC_Departure_Time_Since_Arrival <= 240 THEN 1 else 0 end as '0-4hrs breach' -- added to include 0-4 hrs breaches
		
	,a.EC_Seen_For_Treatment_Date
	,a.EC_Seen_For_Treatment_Time
	,a.EC_Seen_For_Treatment_Time_Since_Arrival
	,a.EC_Conclusion_Date
	,a.EC_Conclusion_Time
	,a.EC_Conclusion_Time_Since_Arrival
	
	,a.EC_Decision_To_Admit_Date
	,a.EC_Decision_To_Admit_Time
	,a.EC_Decision_To_Admit_Time_Since_Arrival

	,a.Decision_To_Admit_Receiving_Site
	,Decision_To_Admit_Treatment_Function_Code as [Decision To Admit Treatment Function Code]
	,tf.[Main_Description] as [Treatment Function Desc]
	,tf.[Category] as [Treatment Function Group]

 
	,a.EC_Chief_Complaint_SNOMED_CT as [MH ED Chief Complaint SNOMED Code]
	,cp.SNOMED_Description [MH ED Chief Complaint Description]
	,a.EC_Injury_Intent_SNOMED_CT as [MH ED Injury Intent SNOMED Code]
	,ii.SNOMED_Description  as [MH ED Injury Intent Description]

	,a.Der_EC_Diagnosis_All as [MH All ED SNOMED Diagnosis Codes]
	,COALESCE(LEFT(a.Der_EC_Diagnosis_All, NULLIF(CHARINDEX(',',a.Der_EC_Diagnosis_All),0)-1),a.Der_EC_Diagnosis_All) AS [MH Primary SNOMED Diagnosis Code]
	,pd.SNOMED_Description  as [MH Primary Diagnosis Description]
	,cast(null as varchar(20)) as [Secondary Diagnosis Code]
	,cast(null as varchar(300)) as [Secondary Diagnosis Description]
	,cast(null as varchar(20)) as [Third Diagnosis Code]
	,cast(null as varchar(300)) as [Third Diagnosis Description]
	,cast(null as varchar(20)) as [Fourth Diagnosis Code]
	,cast(null as varchar(300)) as [Fourth Diagnosis Description]

	,cast(null as int) as [Reduction in Inappropriate Flag]

	,cast(null as varchar(300)) as [Comorbidity_01]
	,cast(null as varchar(300)) as [Comorbidity_02]
	,cast(null as varchar(300)) as [Comorbidity_03]
	,cast(null as varchar(300)) as [Comorbidity_04]

	,cast(null as varchar(300)) as [Referred_To_Service_01]
    ,cast(null as date) as [Service_Request_Date_01]
    ,cast(null as varchar(8)) as [Service_Request_Time_01]
    ,cast(null as date) as [Service_Assessment_Date_01]
    ,cast(null as varchar(8)) as [Service_Assessment_Time_01]
    ,cast(null as varchar(300)) as [Referred_To_Service_02]
    ,cast(null as date) as [Service_Request_Date_02]
    ,cast(null as varchar(8)) as [Service_Request_Time_02]
    ,cast(null as date) as [Service_Assessment_Date_02]
    ,cast(null as varchar(8)) as [Service_Assessment_Time_02] 
	,cast(null as varchar(300)) as [Referred_To_Service_03]
    ,cast(null as date) as [Service_Request_Date_03]
    ,cast(null as varchar(8)) as [Service_Request_Time_03]
    ,cast(null as date) as [Service_Assessment_Date_03]
    ,cast(null as varchar(8)) as [Service_Assessment_Time_03]
	,cast(null as varchar(300)) as [Referred_To_Service_04]
    ,cast(null as date) as [Service_Request_Date_04]
    ,cast(null as varchar(8)) as [Service_Request_Time_04]
    ,cast(null as date) as [Service_Assessment_Date_04]
    ,cast(null as varchar(8)) as [Service_Assessment_Time_04]

	 ,dd.SNOMED_Description as  DischargeDestination
	,df.SNOMED_Description as [Discharge Followup Description]
	
	,CASE WHEN EC_Chief_Complaint_SNOMED_CT IN ('248062006' --- self harm
				,'272022009' --- depressive feelings 
				,'48694002' --- feeling anxious 
				,'248020004' --- behaviour: unsual 
				,'6471006' -- feeling suicidal
				,'7011001'
				,'366979004' --new depressive feelings code from Aril '22 (changed July 2024)
				)  THEN 1 ELSE 0 END as [Chief Complaint Flag]
	,CASE WHEN a.EC_Injury_Date IS NOT NULL THEN 1 ELSE 0 END as [Injury Flag]
	,CASE WHEN EC_Injury_Intent_SNOMED_CT = '276853009'THEN 1 ELSE 0 END as [Injury Intent Flag]
	,CASE WHEN COALESCE(LEFT(Der_EC_Diagnosis_All, NULLIF(CHARINDEX(',',Der_EC_Diagnosis_All),0)-1),Der_EC_Diagnosis_All) 
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
					) THEN 1 ELSE 0 END as [Diagnosis Flag]
	,CASE 
			WHEN EC_Chief_Complaint_SNOMED_CT IN ('248062006' --- self harm
				,'272022009' --- depressive feelings 
				,'48694002' --- feeling anxious 
				,'248020004' --- behaviour: unsual 
				,'6471006' -- feeling suicidal
				,'7011001'
				,'366979004'--new  depressive feelings code added April 2022 - updated here in July 2024
				) THEN 1  --- hallucinations/delusions 
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
					) 
			THEN 1 
		ELSE 0 
		END as [Mental Health Presentation Flag]
	,CASE 
		WHEN EC_Injury_Intent_SNOMED_CT = '276853009' THEN 1
		WHEN EC_Chief_Complaint_SNOMED_CT = '248062006' THEN 1
		ELSE 0 
	END as [Self Harm Flag] 
	
INTO #tempED
FROM  [Reporting_MESH_ECDS].[EC_Core]  a
--FROM  [MESH_ECDS].[EC_Core_1]  a

 left join
 (
 SELECT  
distinct 
[Organisation_Code]
,[Organisation_Name]
FROM  [UKHD_ODS].[All_Providers_SCD]
where   [Is_Latest] = 1
)o1 ON a.Provider_Code = o1.Organisation_Code --- providers 

left join
(
SELECT  
distinct
[Organisation_Code]
,[Organisation_Name]
FROM [UKHD_ODS].[NHS_Trust_Sites_Assets_And_Units_SCD]
where [Is_Latest] = 1
 ) o2 ON a.Site_Code_of_Treatment = o2.Organisation_Code --- sites
 
LEFT JOIN  [Internal_Reference].[ComCodeChanges] cc ON a.Attendance_HES_CCG_From_Treatment_Site_Code = cc.Org_Code 
LEFT JOIN [Reporting_UKHD_ODS].[Commissioner_Hierarchies] o3 ON COALESCE(cc.New_Code,a.Attendance_HES_CCG_From_Treatment_Site_Code) = o3.Organisation_Code --- CCG / STP / Region 

LEft join #TempGP7  gp on gp.GP_Practice_Code = coalesce(a.PDS_General_Practice_Code,a.GP_Practice_Code )
left join [#TrustBoroMap]gpTm on gpTm.Borough = gp.Local_Authority

 
left join #SNOMED  pd  on pd.SNOMED_Code =  COALESCE(LEFT(a.Der_EC_Diagnosis_All, NULLIF(CHARINDEX(',',a.Der_EC_Diagnosis_All),0)-1),a.Der_EC_Diagnosis_All) 
left join #SNOMED  ac  on ac.SNOMED_Code = a.[Accommodation_Status_SNOMED_CT]
left join #SNOMED  ii  on ii.SNOMED_Code = a.EC_Injury_Intent_SNOMED_CT
left join #SNOMED cp on cp.SNOMED_Code = a.EC_Chief_Complaint_SNOMED_CT									 
left join #SNOMED am on am.SNOMED_Code = a.EC_Arrival_Mode_SNOMED_CT
left join #SNOMED ats on ats.SNOMED_Code = a.EC_Attendance_Source_SNOMED_CT
left join #SNOMED df on df.SNOMED_Code = a.Discharge_Follow_Up_SNOMED_CT
left join #SNOMED dd on dd.SNOMED_Code = a.Discharge_Destination_SNOMED_CT
left join [UKHD_Data_Dictionary].[Treatment_Function_Code_SCD] tf on tf.Main_Code_Text = a.Decision_To_Admit_Treatment_Function_Code and tf.Is_Latest = 1
left join #Prov pp on pp.Parent_Organisation_Code = a.Der_Provider_Code
 
left join [#TempGP6]la on la.[PCDS_NoGaps]= pp.[Parent Organisation Postcode No Gaps]
left join [#TrustBoroMap]tm on tm.Borough = la.[Local_Authority_Name]

left join [UKHD_Data_Dictionary].[Ethnic_Category_Code_SCD_1]ec on ec.[Main_Code_Text] = a.Ethnic_Category and ec.is_latest = 1

Left join   [Internal_Reference].[Date] ad on convert(date,ad.[Date_PK]) = a.Arrival_Date
 
          
WHERE a.EC_Department_Type = '01' --- Type 1 EDs only 
AND a.Arrival_Date >= @StartDate
and a.Arrival_Date <= @EndDate

AND (EC_Discharge_Status_SNOMED_CT IS NULL OR EC_Discharge_Status_SNOMED_CT  NOT IN ('1077031000000103','1077781000000101', '63238001')) --exclude streamed and Dead on arrival
AND ([EC_AttendanceCategory] IS NULL OR [EC_AttendanceCategory] in ('1','2','3'))   --exclude follow ups and Dead on arrival
and COALESCE(o3.Region_Name,'Missing/Invalid') = 'London'
 --and Der_Pseudo_NHS_Number = '100033262860'
 

 

delete  from  #tempED where RowOrder > 1
 --select * from [UKHD_Data_Dictionary].[Treatment_Function_Code_SCD]
select * from #tempED where ec_ident = '2352791110'
select * FROM  [Reporting_MESH_ECDS].[EC_Core] where ec_ident = '2352791110'
 --select * from #tempED
 select max( arrival_date )FROM  [Reporting_MESH_ECDS].[EC_Core]
  --select * from [NHSE_Sandbox_London].[dbo].[MH_ECDS_MH_Ref_Borough_Trust_Mapping]
 select * FROM  [Reporting_MESH_ECDS].[EC_Core] where Der_Pseudo_NHS_Number =  '100430412618' order by arrival_date desc
 select * from #tempED where Der_Pseudo_NHS_Number =  '100430412618' 


 go

 	 


		--select * from [#TrustBoroMap]

		
		select 
		Der_Pseudo_NHS_Number,
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
		coalesce([Patient GP Local Authority Name],[Borough Type]) as [Borough],
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
		
		from #tempED a
		where Der_Provider_Site_Name <> 'Missing/Invalid'
		and Provider_Region_Name = 'London'
		and a.[Mental Health Presentation Flag] = 1
		and a.EC_Departure_Time_Since_Arrival > = 0 


		order by Arrival_Date 
 



 
		 
		 select SUM ([0-4hrs breach]) as [0-4hrs]
		 ,sum([5-12hrs breach]) as [5-12hrs]
		 ,sum([12hrs_breach]) as [12hr breaches]
		 ,COUNT(*) as Total
		 from  #tempED
		 where Der_Provider_Site_Name <> 'Missing/Invalid'
	
	
			select
			Arrival_Date as [Arrival DateTime]
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
			coalesce([Patient GP Local Authority Name],[Borough Type]) as [Patient Borough]
		
			from #tempED
			-- where [5-12hrs breach]=1
			where Der_Provider_Site_Name <> 'Missing/Invalid'
			order by Arrival_Date 