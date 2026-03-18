/*
    File: all_ed_v2_cleaned.sql
    Purpose: Cleaned Git-friendly version of the original script.
    Notes:
    - Formatting and readability improved.
    - Existing logic preserved as far as possible.
    - Historical commented debug sections retained.
*/

IF OBJECT_ID('Tempdb..#tempHO1') IS NOT NULL
        DROP TABLE #tempHO1
        SELECT
        DISTINCT
        h.UniqServReqID,
        h.Der_Pseudo_NHS_Number,
        h.UniqHospProvSpellID,
        StartDateHospProvSpell,
        h.DischDateHospProvSpell,
        h.Provider_Name

        INTO #tempHO1

        FROM [PATLondon].[MH_Spells] h WITH (NOLOCK)
        WHERE h.DischDateHospProvSpell IS NOT NULL
        AND
        (
        h.Der_Pseudo_NHS_Number IS NOT NULL
        AND
        h.Der_Pseudo_NHS_Number <>'0'
        AND
        h.Der_Pseudo_NHS_Number <>''
        )
--select top 5000 * from [MESH_MHSDS].[MHS501HospProvSpell_1]
--select top 5000 * FROM   #tempHO1
--select top 5000 * from [PATLondon].[MH_Spells]
--select min([StartDateHospProvSpell]) from [PATLondon].[MH_Spells]
--select top 5000 * from [NHSE_SUSPlus_Live].[dbo].[tbl_Data_SUS_EC] 
        IF OBJECT_ID('Tempdb..#Prov') IS NOT NULL
        DROP TABLE #Prov
        SELECT
        DISTINCT
            Parent_Organisation_Code,
            [Parent Organisation Name],
            [Parent Organisation Postcode],
            REPLACE([Parent Organisation Postcode],' ','') AS [Parent Organisation Postcode No Gaps],
            [Parent Organisation Postcode District],
            [Parent Organisation yr2011 LSOA],
            [MH Trust Flag],
            [MH Provider Abbrev]

        INTO #Prov

        FROM [PATLondon].[Ref_Trusts_and_Sites] a WITH (NOLOCK)


 IF OBJECT_ID('Tempdb..#SNOMED') IS NOT NULL
DROP TABLE #SNOMED

SELECT *
INTO #SNOMED
FROM
(

SELECT [Sheet_Name]

      , [ECDS_Group1]

      , [SNOMED_Code]
      , [SNOMED_Description]
        , ROW_NUMBER() OVER (
        PARTITION BY [SNOMED_Code]
        ORDER BY [Created_Date]desc) AS RowOrder
      , [SNOMED_TERM]

      , [Valid_From]
      , [Valid_To]

  FROM [UKHD_ECDS_TOS].[Code_Sets]


 WHERE [SNOMED_Description] IS NOT NULL

 )d WHERE RowOrder = 1



             DECLARE
            @StartDate DATE, @EndDate DATE

            SET @StartDate ='2025-04-01'
        SET @EndDate = DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1)--'2024-04-30'



 IF OBJECT_ID('Tempdb..#tempED') IS NOT NULL
DROP TABLE #tempED

    SELECT

    CONVERT(VARCHAR(255), a.Generated_Record_ID)+'|'+ CONVERT(VARCHAR(255), Unique_CDS_identifier)+'|'+ CONVERT(VARCHAR(255), Attendance_Unique_Identifier) +'|'+CONVERT(VARCHAR(255), EC_Ident) AS [Unique Record ID]
    , a.Der_Pseudo_NHS_Number

    , EC_Ident
    , a.Generated_Record_ID
    , Unique_CDS_identifier
    , Attendance_Unique_Identifier
    , ROW_NUMBER() OVER (
    PARTITION BY Der_Pseudo_NHS_Number, Attendance_Unique_Identifier, a.Arrival_Date
    ORDER BY a.Arrival_Date ) AS RowOrder

    , CASE
        WHEN Sex ='0' THEN'Unknown'
        WHEN sex ='1' THEN'Male'
        WHEN sex ='2' THEN'Female'
        WHEN sex ='9' THEN'Not specified'
    END AS Gender

    , a.Age_At_Arrival AS [Age at Arrival]
    , CASE
    WHEN (a.Age_At_Arrival <= 18 AND a.Age_At_Arrival IS NOT NULL) THEN'CYP'
    WHEN (a.Age_At_Arrival > 18 AND a.Age_At_Arrival IS NOT NULL) THEN'Adult'
    ELSE'Missing/Invalid' END AS [Age GROUP]
    , CASE
        WHEN a.Age_At_Arrival BETWEEN 0 AND 11 THEN'0-11'
        WHEN a.Age_At_Arrival BETWEEN 12 AND 17 THEN'12-17'
        WHEN a.Age_At_Arrival BETWEEN 18 AND 25 THEN'18-25'
        WHEN a.Age_At_Arrival BETWEEN 26 AND 64 THEN'26-64'
        WHEN a.Age_At_Arrival >= 65 THEN'65+'
        ELSE'Missing/Invalid'
    END AS AgeCat
    , ec.[Category] AS [Broad Ethnic Category]
    , ec.Main_Description AS [Ethnic Category]
    , CASE
    WHEN (ec.Main_Description ='' OR ec.Main_Description ='Not stated' OR ec.Main_Description ='Not known' OR ec.Main_Description IS NULL) THEN'Not Known / Not Stated / Incomplete'
    WHEN ec.Category ='Asian or Asian British' THEN'Asian'
    WHEN ec.Category ='Black or Black British' THEN'Black'
    WHEN ec.Main_Description IN ('mixed','Any other ethnic group','White & Black Caribbean','Any other mixed background','Chinese') THEN'Mixed/ Other'
    ELSE ec.[Category]
    END AS [Derived Broad Ethnic Category]
    , Index_Of_Multiple_Deprivation_Decile
    , Index_Of_Multiple_Deprivation_Decile_Description
    , Rural_Urban_Indicator
    , CAST(NULL AS FLOAT) AS [Ethnic proportion per 100000 of London Borough 2020]

    , NULL AS [Known to MH Services Flag]
--,null as [OLD Known to MH Services Flag] 
    , CAST(NULL AS DATE) AS [Last Completed IP Spell]
    , CAST(NULL AS VARCHAR(255)) AS [IP Spell Provider Name]
    , CAST(NULL AS VARCHAR(255)) AS [UniqHospProvSpellID]
    , CAST(NULL AS VARCHAR(255)) AS [IP Spell UniqServReqID]
    , NULL AS [ED Presentation within 28 days of Completed IP SPell]
    , NULL AS [Days between Completed IP Spell AND ED Presentation]

    , COALESCE(a.PDS_General_Practice_Code, a.GP_Practice_Code ) AS GP_Practice_Code
    , gp.GP_Name AS [Practice Name]
    , gp.PCDS_NoGaps AS [GP Practice PostCode No Gaps]
    , gp.[2019_CCG_Name] AS [Patient GP Practice 2019 CCG Code]
    , GP.[Local_Authority] AS [Patient GP Local Authority Name]

    , GP.GP_Region_Name AS [Patient GP Practice Region]
    , CASE
        WHEN gpTm.Borough IS NULL AND GP.[Local_Authority] IS NOT NULL THEN'Out of London Borough'
        WHEN gpTm.Borough IS NULL AND GP.[Local_Authority] IS NULL THEN'GP Practice Unknown'
        WHEN gpTm.Borough IS NOT NULL THEN'London patient'
        END AS [Borough Type]
    , gpTm.ICS AS [Patient ICS]
    , gpTm.Trust AS [Local MH Trust]
    , gp.Lower_Super_Output_Area_Code AS [Patient GP 2011_LSOA]
    , gp.Middle_Super_Output_Area_Code AS [Patient GP 2011_MS0A]
    , ac.[SNOMED_Description] AS Accommodation_Status_SNOMED_CT

    , Attendance_Postcode_District
    , Attendance_HES_CCG_From_Treatment_Origin
    , Attendance_HES_CCG_From_Treatment_Site_Code
    , Attendance_LSOA_Provider_Distance--The distance, in miles, between the LSOA centroid of the patient's submitted postcode and the LSOA centroid of the provider.
    , Attendance_LSOA_Treatment_Site_Distance--The distance between the LSOA centroid of the patient's submitted postcode and the LSOA centroid of the site of treatment.
    , ats.[SNOMED_Description] AS AttendanceSource
    , Patient_Type
    , a.Der_Provider_Code
--local patient ID, provider code and activity date/time.
    , COALESCE(o1.Organisation_Name,'Missing/Invalid') AS Der_Provider_Name
    , a.Der_Provider_Site_Code
    , pp.[Parent Organisation Postcode] AS [Provider PostCode]
    , pp.[Parent Organisation Postcode District] AS [Provider Postcode District]
    , pp.[Parent Organisation yr2011 LSOA] AS [Provider 2011 LSOA]
    , COALESCE(o2.Organisation_Name,'Missing/Invalid') AS Der_Provider_Site_Name
    , COALESCE(o3.Region_Code,'Missing/Invalid') AS Provider_Region_Code--- regions taken from CCG of provider rather than CCG of residence
    , COALESCE(o3.Region_Name,'Missing/Invalid') AS Provider_Region_Name
    , COALESCE(cc.New_Code, a.Attendance_HES_CCG_From_Treatment_Site_Code,'Missing/Invalid') AS Provider_CCGCode
    , COALESCE(o3.Organisation_Name,'Missing/Invalid') AS [Provider_CCG name]
    , tm.ICS AS [Provider ICB]
    , COALESCE(o3.STP_Code,'Missing/Invalid') AS Provider_STPCode
    , COALESCE(o3.STP_Name,'Missing/Invalid') AS [Provider STP name]
    , DATEADD(MONTH, DATEDIFF(MONTH, 0, Arrival_Date), 0) AS [Month Year]

    , a.Arrival_Date
    , ad.[Financial Year] AS [ArrivalDate FY]
    , DATEPART(HOUR, a.Arrival_Time) AS [Arrival Hour]
    , CAST(ISNULL(a.Arrival_Time,'00:00:00') AS DATETIME) + CAST(a.Arrival_Date AS DATETIME) AS [Arrival DATE Time]
    , am.[SNOMED_Description] AS [Arrival Mode]
    , a.EC_Initial_Assessment_Date
    , a.EC_Initial_Assessment_Time
    , a.EC_Initial_Assessment_Time_Since_Arrival
    , a.EC_Departure_Date
    , a.EC_Departure_Time
    , EC_Departure_Time_Since_Arrival AS [EC_Departure_Time_Since_Arrival]
    , CASE
    WHEN [EC_Departure_Time_Since_Arrival] >= 0 AND [EC_Departure_Time_Since_Arrival] <= 240 THEN'0-4'
    WHEN [EC_Departure_Time_Since_Arrival] IS NULL THEN'0-4'
    WHEN [EC_Departure_Time_Since_Arrival] > 240 AND [EC_Departure_Time_Since_Arrival] <= 720 THEN'5-12'
    WHEN [EC_Departure_Time_Since_Arrival] > 720 AND [EC_Departure_Time_Since_Arrival] <= 1440 THEN'12-24'
    WHEN [EC_Departure_Time_Since_Arrival] > 1440 AND [EC_Departure_Time_Since_Arrival] <= 2880 THEN'24-48'
    WHEN [EC_Departure_Time_Since_Arrival] > 2880 AND [EC_Departure_Time_Since_Arrival] <= 4320 THEN'48-72'
    WHEN [EC_Departure_Time_Since_Arrival] > 4320 THEN'>72'
    ELSE'Not recorded'
    END AS [Time Grouper]
    , CASE WHEN EC_Departure_Time_Since_Arrival > (60*6) THEN 1 ELSE 0 END AS [6 Hour Breach]
    , CASE WHEN EC_Departure_Time_Since_Arrival > (60*6) THEN (EC_Departure_Time_Since_Arrival - (60*6)) ELSE 0 END AS [Time OVER 6 Hours]
    , CASE WHEN EC_Departure_Time_Since_Arrival > (60*12) THEN 1 ELSE 0 END AS [12 Hour Breach]
    , CASE WHEN EC_Departure_Time_Since_Arrival > (60*12) THEN EC_Departure_Time_Since_Arrival - (60*12) ELSE 0 END AS [Time OVER 12 Hours]
    , CASE WHEN EC_Departure_Time_Since_Arrival >= (24*60) THEN 1 ELSE 0 END AS [24hrs_breach]
    , a.EC_Seen_For_Treatment_Date
    , a.EC_Seen_For_Treatment_Time
    , a.EC_Seen_For_Treatment_Time_Since_Arrival
    , a.EC_Conclusion_Date
    , a.EC_Conclusion_Time
    , a.EC_Conclusion_Time_Since_Arrival
    , a.[Der_EC_Duration]
    , a.EC_Decision_To_Admit_Date
    , a.EC_Decision_To_Admit_Time
    , a.EC_Decision_To_Admit_Time_Since_Arrival

    , a.Decision_To_Admit_Receiving_Site
    , Decision_To_Admit_Treatment_Function_Code AS [Decision To Admit Treatment Function Code]
    , tf.[Main_Description] AS [Treatment Function Desc]
    , tf.[Category] AS [Treatment Function GROUP]

    , a.EC_Chief_Complaint_SNOMED_CT AS [MH ED Chief Complaint SNOMED Code]
    , cp.SNOMED_Description [MH ED Chief Complaint Description]
    , a.EC_Injury_Intent_SNOMED_CT AS [MH ED Injury Intent SNOMED Code]
    , ii.SNOMED_Description AS [MH ED Injury Intent Description]

    , a.Der_EC_Diagnosis_All AS [MH ALL ED SNOMED Diagnosis Codes]
    , COALESCE(LEFT(a.Der_EC_Diagnosis_All, NULLIF(CHARINDEX(',', a.Der_EC_Diagnosis_All), 0)-1), a.Der_EC_Diagnosis_All) AS [MH Primary SNOMED Diagnosis Code]
    , pd.SNOMED_Description AS [MH Primary Diagnosis Description]
    , CAST(NULL AS VARCHAR(20)) AS [Secondary Diagnosis Code]
    , CAST(NULL AS VARCHAR(300)) AS [Secondary Diagnosis Description]
    , CAST(NULL AS VARCHAR(20)) AS [Third Diagnosis Code]
    , CAST(NULL AS VARCHAR(300)) AS [Third Diagnosis Description]
    , CAST(NULL AS VARCHAR(20)) AS [Fourth Diagnosis Code]
    , CAST(NULL AS VARCHAR(300)) AS [Fourth Diagnosis Description]

    , CAST(NULL AS INT) AS [Reduction IN Inappropriate Flag]

    , CAST(NULL AS VARCHAR(300)) AS [Comorbidity_01]
    , CAST(NULL AS VARCHAR(300)) AS [Comorbidity_02]
    , CAST(NULL AS VARCHAR(300)) AS [Comorbidity_03]
    , CAST(NULL AS VARCHAR(300)) AS [Comorbidity_04]

    , CAST(NULL AS VARCHAR(300)) AS [Referred_To_Service_01]
    , CAST(NULL AS DATE) AS [Service_Request_Date_01]
    , CAST(NULL AS VARCHAR(8)) AS [Service_Request_Time_01]
    , CAST(NULL AS DATE) AS [Service_Assessment_Date_01]
    , CAST(NULL AS VARCHAR(8)) AS [Service_Assessment_Time_01]
    , CAST(NULL AS VARCHAR(300)) AS [Referred_To_Service_02]
    , CAST(NULL AS DATE) AS [Service_Request_Date_02]
    , CAST(NULL AS VARCHAR(8)) AS [Service_Request_Time_02]
    , CAST(NULL AS DATE) AS [Service_Assessment_Date_02]
    , CAST(NULL AS VARCHAR(8)) AS [Service_Assessment_Time_02]
    , CAST(NULL AS VARCHAR(300)) AS [Referred_To_Service_03]
    , CAST(NULL AS DATE) AS [Service_Request_Date_03]
    , CAST(NULL AS VARCHAR(8)) AS [Service_Request_Time_03]
    , CAST(NULL AS DATE) AS [Service_Assessment_Date_03]
    , CAST(NULL AS VARCHAR(8)) AS [Service_Assessment_Time_03]
    , CAST(NULL AS VARCHAR(300)) AS [Referred_To_Service_04]
    , CAST(NULL AS DATE) AS [Service_Request_Date_04]
    , CAST(NULL AS VARCHAR(8)) AS [Service_Request_Time_04]
    , CAST(NULL AS DATE) AS [Service_Assessment_Date_04]
    , CAST(NULL AS VARCHAR(8)) AS [Service_Assessment_Time_04]

     , dd.SNOMED_Description AS DischargeDestination
    , df.SNOMED_Description AS [Discharge Followup Description]

    , CASE WHEN EC_Chief_Complaint_SNOMED_CT IN ('248062006'--- self harm
                ,'272022009'--- depressive feelings 
                ,'48694002'--- feeling anxious 
                ,'248020004'--- behaviour: unsual 
                ,'6471006'-- feeling suicidal
                ,'7011001'
                ,'366979004'--new depressive feelings code from Aril '22 (changed July 2024)
                ) THEN 1 ELSE 0 END AS [Chief Complaint Flag]
    , CASE WHEN a.EC_Injury_Date IS NOT NULL THEN 1 ELSE 0 END AS [Injury Flag]
    , CASE WHEN EC_Injury_Intent_SNOMED_CT ='276853009'THEN 1 ELSE 0 END AS [Injury Intent Flag]
    , CASE WHEN COALESCE(LEFT(Der_EC_Diagnosis_All, NULLIF(CHARINDEX(',', Der_EC_Diagnosis_All), 0)-1), Der_EC_Diagnosis_All)
                IN (
'52448006'--- dementia
                    ,'2776000'--- delirium 
                    ,'33449004'--- personality disorder
                    ,'72366004'--- eating disorder
                    ,'197480006'--- anxiety disorder
                    ,'35489007'--- depressive disorder
                    ,'13746004'--- bipolar affective disorder
                    ,'58214004'--- schizophrenia
                    ,'69322001'--- psychotic disorder
                    ,'397923000'--- somatisation disorder
                    ,'30077003'--- somatoform pain disorder
                    ,'44376007'--- dissociative disorder
                    ,'17226007'---- adjustment disorder
                    ,'50705009'---- factitious disorder
                    ) THEN 1 ELSE 0 END AS [Diagnosis Flag]
    , CASE
            WHEN EC_Chief_Complaint_SNOMED_CT IN ('248062006'--- self harm
                ,'272022009'--- depressive feelings 
                ,'48694002'--- feeling anxious 
                ,'248020004'--- behaviour: unsual 
                ,'6471006'-- feeling suicidal
                ,'7011001'
                ,'366979004'--new  depressive feelings code added April 2022 - updated here in July 2024
                ) THEN 1--- hallucinations/delusions 
            WHEN EC_Injury_Intent_SNOMED_CT ='276853009' THEN 1--- self inflicted injury 
            WHEN COALESCE(LEFT(Der_EC_Diagnosis_All, NULLIF(CHARINDEX(',', Der_EC_Diagnosis_All), 0)-1), Der_EC_Diagnosis_All)
                IN (
'52448006'--- dementia
                    ,'2776000'--- delirium 
                    ,'33449004'--- personality disorder
                    ,'72366004'--- eating disorder
                    ,'197480006'--- anxiety disorder
                    ,'35489007'--- depressive disorder
                    ,'13746004'--- bipolar affective disorder
                    ,'58214004'--- schizophrenia
                    ,'69322001'--- psychotic disorder
                    ,'397923000'--- somatisation disorder
                    ,'30077003'--- somatoform pain disorder
                    ,'44376007'--- dissociative disorder
                    ,'17226007'---- adjustment disorder
                    ,'50705009'---- factitious disorder
                    )
            THEN 1
        ELSE 0
        END AS [Mental Health Presentation Flag]
    , CASE
        WHEN EC_Injury_Intent_SNOMED_CT ='276853009' THEN 1
        WHEN EC_Chief_Complaint_SNOMED_CT ='248062006' THEN 1
        ELSE 0
    END AS [Self Harm Flag]

INTO #tempED
FROM [MESH_ECDS].[EC_Core_1] a

 LEFT JOIN
 (
 SELECT
DISTINCT
[Organisation_Code]
, [Organisation_Name]
FROM [UKHD_ODS].[All_Providers_SCD_1]
WHERE [Is_Latest] = 1
)o1 ON a.Provider_Code = o1.Organisation_Code--- providers 

LEFT JOIN
(
SELECT
DISTINCT
[Organisation_Code]
, [Organisation_Name]
FROM [UKHD_ODS].[NHS_Trust_Sites_Assets_And_Units_SCD_1]
WHERE [Is_Latest] = 1
 ) o2 ON a.Site_Code_of_Treatment = o2.Organisation_Code--- sites

LEFT JOIN [Internal_Reference].[ComCodeChanges_1] cc ON a.Attendance_HES_CCG_From_Treatment_Site_Code = cc.Org_Code
LEFT JOIN [Reporting_UKHD_ODS].[Commissioner_Hierarchies] o3 ON COALESCE(cc.New_Code, a.Attendance_HES_CCG_From_Treatment_Site_Code) = o3.Organisation_Code--- CCG / STP / Region 
LEFT JOIN [PATLondon].[Ref_GP_Data] gp ON gp.GP_Practice_Code = COALESCE(a.PDS_General_Practice_Code, a.GP_Practice_Code )
LEFT JOIN [PATLondon].[Ref_Borough_Trust_Mapping]gpTm ON gpTm.Borough = gp.Local_Authority

LEFT JOIN #SNOMED pd ON pd.SNOMED_Code = COALESCE(LEFT(a.Der_EC_Diagnosis_All, NULLIF(CHARINDEX(',', a.Der_EC_Diagnosis_All), 0)-1), a.Der_EC_Diagnosis_All)
LEFT JOIN #SNOMED ac ON ac.SNOMED_Code = a.[Accommodation_Status_SNOMED_CT]
LEFT JOIN #SNOMED ii ON ii.SNOMED_Code = a.EC_Injury_Intent_SNOMED_CT
LEFT JOIN #SNOMED cp ON cp.SNOMED_Code = a.EC_Chief_Complaint_SNOMED_CT
LEFT JOIN #SNOMED am ON am.SNOMED_Code = a.EC_Arrival_Mode_SNOMED_CT
LEFT JOIN #SNOMED ats ON ats.SNOMED_Code = a.EC_Attendance_Source_SNOMED_CT
LEFT JOIN #SNOMED df ON df.SNOMED_Code = a.Discharge_Follow_Up_SNOMED_CT
LEFT JOIN #SNOMED dd ON dd.SNOMED_Code = a.Discharge_Destination_SNOMED_CT
LEFT JOIN [UKHD_Data_Dictionary].[Treatment_Function_Code_SCD_1] tf ON tf.Main_Code_Text = a.Decision_To_Admit_Treatment_Function_Code
LEFT JOIN #Prov pp ON pp.Parent_Organisation_Code = a.Der_Provider_Code

LEFT JOIN [PATLondon].[Ref_PostCode_to_Local_Authority]la ON la.[PostCode No Gaps]= pp.[Parent Organisation Postcode No Gaps]
LEFT JOIN [PATLondon].[Ref_Borough_Trust_Mapping]tm ON tm.Borough = la.Name

LEFT JOIN [UKHD_Data_Dictionary].[Ethnic_Category_Code_SCD_1]ec ON ec.[Main_Code_Text] = a.Ethnic_Category AND ec.is_latest = 1

LEFT JOIN [PATLondon].[DIM_Date]ad ON ad.[Calendar Day] = a.Arrival_Date

WHERE a.EC_Department_Type ='01'--- Type 1 EDs only 
AND a.Arrival_Date >= @StartDate
AND a.Arrival_Date <= @EndDate

AND (EC_Discharge_Status_SNOMED_CT IS NULL OR EC_Discharge_Status_SNOMED_CT NOT IN ('1077031000000103','1077781000000101','63238001'))--exclude streamed and Dead on arrival
AND ([EC_AttendanceCategory] IS NULL OR [EC_AttendanceCategory] IN ('1','2','3'))--exclude follow ups and Dead on arrival
AND COALESCE(o3.Region_Name,'Missing/Invalid') ='London'

DELETE FROM #tempED WHERE RowOrder > 1


 IF OBJECT_ID('Tempdb..#tempComorb') IS NOT NULL
DROP TABLE #tempComorb

SELECT
    [EC_Ident],
    [Generated_Record_ID],
    [Comorbidity_01] = MIN(CASE WHEN y.rn = 1 THEN y.val END),
    [Comorbidity_02]= MIN(CASE WHEN y.rn = 2 THEN y.val END),
    [Comorbidity_03] = MIN(CASE WHEN y.rn = 3 THEN y.val END),
    [Comorbidity_04] = MIN(CASE WHEN y.rn = 4 THEN y.val END),
    [Comorbidity_05] = MIN(CASE WHEN y.rn = 5 THEN y.val END),
    [Comorbidity_06] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_07] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_08] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_09] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_10] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_11] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_12] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_13] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_14] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_15] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_16] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_17] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_18] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_19] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_20] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_21] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_22] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_23] = MIN(CASE WHEN y.rn = 6 THEN y.val END),
    [Comorbidity_24] = MIN(CASE WHEN y.rn = 6 THEN y.val END)

    INTO #tempComorb

FROM [Reporting_MESH_ECDS].[MESH_ECDS_EC_Comorbidities] t
  OUTER APPLY
    ( SELECT
          x.val,
          rn = ROW_NUMBER() OVER (ORDER BY rn)
      FROM
      ( VALUES
        ([Comorbidity_01], 1), ([Comorbidity_02], 2), ([Comorbidity_03], 3), ([Comorbidity_04], 4),
        ([Comorbidity_05], 5), ([Comorbidity_06], 6), ([Comorbidity_07], 7), ([Comorbidity_08], 8),
        ([Comorbidity_09], 9), ([Comorbidity_10], 10), ([Comorbidity_11], 11), ([Comorbidity_12], 12),
        ([Comorbidity_13], 13), ([Comorbidity_14], 14), ([Comorbidity_15], 15), ([Comorbidity_16], 16),
        ([Comorbidity_17], 17), ([Comorbidity_18], 18), ([Comorbidity_19], 19), ([Comorbidity_20], 20),
        ([Comorbidity_21], 21), ([Comorbidity_22], 22), ([Comorbidity_23], 23), ([Comorbidity_24], 24)
      ) x (val, rn)
      WHERE x.val IS NOT NULL
    ) y

    WHERE
     EXISTS (
                SELECT
                [EC_Ident],
                [Generated_Record_ID]
                FROM #tempED x
                WHERE x.[EC_Ident] = t.[EC_Ident]
--and RowOrder = 1
--and x.[Generated_Record_ID] = t.[Generated_Record_ID]
                )
            AND

          COALESCE( [Comorbidity_01], [Comorbidity_02], [Comorbidity_03], [Comorbidity_04],
                    [Comorbidity_05], [Comorbidity_06], [Comorbidity_07], [Comorbidity_08],
                    [Comorbidity_09], [Comorbidity_10], [Comorbidity_11], [Comorbidity_12],
                    [Comorbidity_13], [Comorbidity_14], [Comorbidity_15], [Comorbidity_16],
                    [Comorbidity_17], [Comorbidity_18], [Comorbidity_19], [Comorbidity_20],
                    [Comorbidity_21], [Comorbidity_22], [Comorbidity_23], [Comorbidity_24]
                    ) IS NOT NULL
                    AND
          COALESCE( [Comorbidity_01], [Comorbidity_02], [Comorbidity_03], [Comorbidity_04],
                    [Comorbidity_05], [Comorbidity_06], [Comorbidity_07], [Comorbidity_08],
                    [Comorbidity_09], [Comorbidity_10], [Comorbidity_11], [Comorbidity_12],
                    [Comorbidity_13], [Comorbidity_14], [Comorbidity_15], [Comorbidity_16],
                    [Comorbidity_17], [Comorbidity_18], [Comorbidity_19], [Comorbidity_20],
                    [Comorbidity_21], [Comorbidity_22], [Comorbidity_23], [Comorbidity_24]
                    ) <>''
GROUP BY
    t.[EC_Ident],
    t.[Generated_Record_ID] ;

    UPDATE f
       SET [Comorbidity_01] = CASE WHEN [Comorbidity_01] ='' THEN NULL ELSE [Comorbidity_01] END,
    [Comorbidity_02] = CASE WHEN [Comorbidity_02] ='' THEN NULL ELSE [Comorbidity_02] END,
    [Comorbidity_03] = CASE WHEN [Comorbidity_03] ='' THEN NULL ELSE [Comorbidity_03] END,
    [Comorbidity_04] = CASE WHEN [Comorbidity_04] ='' THEN NULL ELSE [Comorbidity_04] END,
    [Comorbidity_05] = CASE WHEN [Comorbidity_05] ='' THEN NULL ELSE [Comorbidity_05] END,
    [Comorbidity_06] = CASE WHEN [Comorbidity_06] ='' THEN NULL ELSE [Comorbidity_06] END,
    [Comorbidity_07] = CASE WHEN [Comorbidity_07] ='' THEN NULL ELSE [Comorbidity_07] END,
    [Comorbidity_08] = CASE WHEN [Comorbidity_08] ='' THEN NULL ELSE [Comorbidity_08] END,
    [Comorbidity_09] = CASE WHEN [Comorbidity_09] ='' THEN NULL ELSE [Comorbidity_09] END,
    [Comorbidity_10] = CASE WHEN [Comorbidity_10] ='' THEN NULL ELSE [Comorbidity_10] END,
    [Comorbidity_11] = CASE WHEN [Comorbidity_11] ='' THEN NULL ELSE [Comorbidity_11] END,
    [Comorbidity_12] = CASE WHEN [Comorbidity_12] ='' THEN NULL ELSE [Comorbidity_12] END,
    [Comorbidity_13] = CASE WHEN [Comorbidity_13] ='' THEN NULL ELSE [Comorbidity_13] END,
    [Comorbidity_14] = CASE WHEN [Comorbidity_14] ='' THEN NULL ELSE [Comorbidity_14] END,
    [Comorbidity_15] = CASE WHEN [Comorbidity_15] ='' THEN NULL ELSE [Comorbidity_15] END,
    [Comorbidity_16] = CASE WHEN [Comorbidity_16] ='' THEN NULL ELSE [Comorbidity_16] END,
    [Comorbidity_17] = CASE WHEN [Comorbidity_17] ='' THEN NULL ELSE [Comorbidity_17] END,
    [Comorbidity_18] = CASE WHEN [Comorbidity_18] ='' THEN NULL ELSE [Comorbidity_18] END,
    [Comorbidity_19] = CASE WHEN [Comorbidity_19] ='' THEN NULL ELSE [Comorbidity_19] END,
    [Comorbidity_20] = CASE WHEN [Comorbidity_20] ='' THEN NULL ELSE [Comorbidity_20] END,
    [Comorbidity_21] = CASE WHEN [Comorbidity_21] ='' THEN NULL ELSE [Comorbidity_21] END,
    [Comorbidity_22] = CASE WHEN [Comorbidity_22] ='' THEN NULL ELSE [Comorbidity_22] END,
    [Comorbidity_23] = CASE WHEN [Comorbidity_23] ='' THEN NULL ELSE [Comorbidity_23] END,
    [Comorbidity_24] = CASE WHEN [Comorbidity_24] ='' THEN NULL ELSE [Comorbidity_24] END

    FROM #tempComorb f

    INSERT INTO [PATLondon].[ECDS_Comorbidities_Cleaned]
    SELECT
    *
    FROM #tempComorb b
    WHERE NOT EXISTS ( SELECT
                [EC_Ident],
                [Generated_Record_ID]
                FROM [PATLondon].[ECDS_Comorbidities_Cleaned] x
                WHERE x.[EC_Ident] = b.[EC_Ident]
                AND x.[Generated_Record_ID] = b.[Generated_Record_ID]
                )

    UPDATE z
         SET z.[Comorbidity_01] = b.snomed_description

    FROM #tempED z
    INNER JOIN [PATLondon].[ECDS_Comorbidities_Cleaned]a ON a.EC_Ident = z.EC_Ident
    LEFT JOIN #SNOMED b ON b.snomed_Code = a.Comorbidity_01

     UPDATE z
         SET z.[Comorbidity_02] = b.snomed_description
    FROM #tempED z
    INNER JOIN [PATLondon].[ECDS_Comorbidities_Cleaned]a ON a.EC_Ident = z.EC_Ident
    LEFT JOIN #SNOMED b ON b.snomed_Code = a.Comorbidity_02

     UPDATE z
         SET z.[Comorbidity_03] = b.snomed_description
    FROM #tempED z
    INNER JOIN [PATLondon].[ECDS_Comorbidities_Cleaned]a ON a.EC_Ident = z.EC_Ident
    LEFT JOIN #SNOMED b ON b.snomed_Code = a.Comorbidity_03

     UPDATE z
         SET z.[Comorbidity_04] = b.snomed_description
    FROM #tempED z
    INNER JOIN [PATLondon].[ECDS_Comorbidities_Cleaned]a ON a.EC_Ident = z.EC_Ident
    LEFT JOIN #SNOMED b ON b.snomed_Code = a.Comorbidity_04

--select top 5000 * from #tempED
IF OBJECT_ID('Tempdb..#tempDiagCodes') IS NOT NULL
DROP TABLE #tempDiagCodes

SELECT
EC_Ident,
[Unique Record ID],
[MH ALL ED SNOMED Diagnosis Codes],
ltrim(rtrim([Diag1])) AS [Diag1],
CAST(NULL AS VARCHAR(300)) AS [Diag 1 Description],
ltrim(rtrim([Diag2])) AS [Diag2],
CAST(NULL AS VARCHAR(300)) AS [Diag 2 Description],
ltrim(rtrim([Diag3])) AS [Diag3],
CAST(NULL AS VARCHAR(300)) AS [Diag 3 Description],
ltrim(rtrim([Diag4])) AS [Diag4],
CAST(NULL AS VARCHAR(300)) AS [Diag 4 Description]
INTO #tempDiagCodes
FROM (
     SELECT
     EC_Ident,
     [Unique Record ID],
     [MH ALL ED SNOMED Diagnosis Codes],
'Diag'+ CAST(ROW_NUMBER()OVER(PARTITION BY EC_Ident, [Unique Record ID] ORDER BY EC_Ident) AS VARCHAR) AS Col,
     Split.value
     FROM #tempED AS Emp

     CROSS APPLY String_split([MH ALL ED SNOMED Diagnosis Codes],',') AS Split
--where emp.RowOrder = 1
     )
     AS tbl

    PIVOT (MAX(Value) FOR Col IN ([Diag1], [Diag2], [Diag3], [Diag4])

) AS Pvt

DELETE FROM #tempDiagCodes WHERE COALESCE(diag2, diag3, diag4) = NULL

UPDATE d
SET d.[Diag 2 Description] = a.SNOMED_Description
FROM #tempDiagCodes d
INNER JOIN #SNOMED a ON a.snomed_Code = d.Diag2

UPDATE d
SET d.[Diag 3 Description] = a.SNOMED_Description
FROM #tempDiagCodes d
INNER JOIN #SNOMED a ON a.snomed_Code = d.Diag3

UPDATE d
SET d.[Diag 4 Description] = a.SNOMED_Description
FROM #tempDiagCodes d
INNER JOIN #SNOMED a ON a.snomed_Code = d.Diag4

--select * from #tempDiagCodes

UPDATE edd
SET edd.[Secondary Diagnosis Description] = b.[Diag 2 Description],
    edd.[Secondary Diagnosis Code] = b.Diag2,
    edd.[Third Diagnosis Description] = b.[Diag 3 Description],
    edd.[Third Diagnosis Code] = b.Diag3,
    edd.[Fourth Diagnosis Description] = b.[Diag 4 Description],
    edd.[Fourth Diagnosis Code] = b.Diag4
FROM #tempED edd
INNER JOIN #tempDiagCodes b ON b.EC_Ident = edd.EC_Ident
--where edd.RowOrder = 1

--select top 5000 * from #tempED

IF OBJECT_ID('Tempdb..#tempRefToServ') IS NOT NULL
DROP TABLE #tempRefToServ

SELECT
       [EC_Ident]
      , [Generated_Record_ID]
      , [Referred_To_Service_01]
      , [Service_Request_Date_01]
      , [Service_Request_Time_01]
      , [Service_Assessment_Date_01]
      , [Service_Assessment_Time_01]
      , [Referred_To_Service_02]
      , [Service_Request_Date_02]
      , [Service_Request_Time_02]
      , [Service_Assessment_Date_02]
      , [Service_Assessment_Time_02]
      , [Referred_To_Service_03]
      , [Service_Request_Date_03]
      , [Service_Request_Time_03]
      , [Service_Assessment_Date_03]
      , [Service_Assessment_Time_03]
      , [Referred_To_Service_04]
      , [Service_Request_Date_04]
      , [Service_Request_Time_04]
      , [Service_Assessment_Date_04]
      , [Service_Assessment_Time_04]
    INTO #tempRefToServ

FROM [Reporting_MESH_ECDS].[MESH_ECDS_EC_PatientReferredTo] t
  OUTER APPLY
    ( SELECT
          x.val,
          rn = ROW_NUMBER() OVER (ORDER BY rn)
      FROM
      ( VALUES
        ([Referred_To_Service_01], 1), ([Service_Request_Date_01], 2), ([Service_Request_Time_01], 3), ([Service_Assessment_Date_01], 4),
        ([Service_Assessment_Time_01], 5), ([Referred_To_Service_02], 6), ([Service_Request_Date_02], 7), ([Service_Request_Time_02], 8),
        ([Service_Assessment_Date_02], 9), ([Service_Assessment_Time_02], 10), ([Referred_To_Service_03], 11), ([Service_Request_Date_03], 12),
        ([Service_Request_Time_03], 13), ([Service_Assessment_Date_03], 14), ([Service_Assessment_Time_03], 15), ([Referred_To_Service_04], 16),
        ([Service_Request_Date_04], 17), ([Service_Request_Time_04], 18), ([Service_Assessment_Date_04], 19), ([Service_Assessment_Time_04], 20)
      ) x (val, rn)
      WHERE x.val IS NOT NULL
    ) y

    WHERE EXISTS (
                SELECT
                [EC_Ident]
--[Generated_Record_ID] 
                FROM #tempED x
                WHERE x.[EC_Ident] = t.[EC_Ident]

                )
            AND

          COALESCE([Referred_To_Service_01], [Referred_To_Service_02], [Referred_To_Service_03], [Referred_To_Service_04]) IS NOT NULL
                    AND
          COALESCE([Referred_To_Service_01], [Referred_To_Service_02], [Referred_To_Service_03], [Referred_To_Service_04]) <>''
GROUP BY
    t.[EC_Ident],
    t.[Generated_Record_ID]
          , [Referred_To_Service_01]
      , [Service_Request_Date_01]
      , [Service_Request_Time_01]
      , [Service_Assessment_Date_01]
      , [Service_Assessment_Time_01]
      , [Referred_To_Service_02]
      , [Service_Request_Date_02]
      , [Service_Request_Time_02]
      , [Service_Assessment_Date_02]
      , [Service_Assessment_Time_02]
      , [Referred_To_Service_03]
      , [Service_Request_Date_03]
      , [Service_Request_Time_03]
      , [Service_Assessment_Date_03]
      , [Service_Assessment_Time_03]
      , [Referred_To_Service_04]
      , [Service_Request_Date_04]
      , [Service_Request_Time_04]
      , [Service_Assessment_Date_04]
      , [Service_Assessment_Time_04];

    UPDATE f
       SET [Referred_To_Service_01] = CASE WHEN [Referred_To_Service_01] ='' THEN NULL ELSE [Referred_To_Service_01] END,
            [Service_Request_Date_01] = CASE WHEN [Service_Request_Date_01] ='' THEN NULL ELSE [Service_Request_Date_01] END,
            [Service_Request_Time_01] = CASE WHEN [Service_Request_Time_01] ='' THEN NULL ELSE [Service_Request_Time_01] END,
            [Service_Assessment_Date_01] = CASE WHEN [Service_Assessment_Date_01] ='' THEN NULL ELSE [Service_Assessment_Date_01] END,
            [Service_Assessment_Time_01] = CASE WHEN [Service_Assessment_Time_01] ='' THEN NULL ELSE [Service_Assessment_Time_01] END,

            [Referred_To_Service_02] = CASE WHEN [Referred_To_Service_02] ='' THEN NULL ELSE [Referred_To_Service_02] END,
            [Service_Request_Date_02] = CASE WHEN [Service_Request_Date_02] ='' THEN NULL ELSE [Service_Request_Date_02] END,
            [Service_Request_Time_02] = CASE WHEN [Service_Request_Time_02] ='' THEN NULL ELSE [Service_Request_Time_02] END,
            [Service_Assessment_Date_02] = CASE WHEN [Service_Assessment_Date_02] ='' THEN NULL ELSE [Service_Assessment_Date_02] END,
            [Service_Assessment_Time_02] = CASE WHEN [Service_Assessment_Time_02] ='' THEN NULL ELSE [Service_Assessment_Time_02] END,

            [Referred_To_Service_03] = CASE WHEN [Referred_To_Service_03] ='' THEN NULL ELSE [Referred_To_Service_03] END,
            [Service_Request_Date_03] = CASE WHEN [Service_Request_Date_03] ='' THEN NULL ELSE [Service_Request_Date_03] END,
            [Service_Request_Time_03] = CASE WHEN [Service_Request_Time_03] ='' THEN NULL ELSE [Service_Request_Time_03] END,
            [Service_Assessment_Date_03] = CASE WHEN [Service_Assessment_Date_03] ='' THEN NULL ELSE [Service_Assessment_Date_03] END,
            [Service_Assessment_Time_03] = CASE WHEN [Service_Assessment_Time_03] ='' THEN NULL ELSE [Service_Assessment_Time_03] END,

            [Referred_To_Service_04] = CASE WHEN [Referred_To_Service_04] ='' THEN NULL ELSE [Referred_To_Service_04] END,
            [Service_Request_Date_04] = CASE WHEN [Service_Request_Date_04] ='' THEN NULL ELSE [Service_Request_Date_04] END,
            [Service_Request_Time_04] = CASE WHEN [Service_Request_Time_04] ='' THEN NULL ELSE [Service_Request_Time_04] END,
            [Service_Assessment_Date_04] = CASE WHEN [Service_Assessment_Date_04] ='' THEN NULL ELSE [Service_Assessment_Date_04] END,
            [Service_Assessment_Time_04] = CASE WHEN [Service_Assessment_Time_04] ='' THEN NULL ELSE [Service_Assessment_Time_04] END

    FROM #tempRefToServ f



    UPDATE z

        SET [Referred_To_Service_01] = b.snomed_description,
              [Service_Request_Date_01]= x.[Service_Request_Date_01],
              [Service_Request_Time_01]= x.[Service_Request_Time_01],
              [Service_Assessment_Date_01]= x.[Service_Assessment_Date_01],
              [Service_Assessment_Time_01]= x.[Service_Assessment_Time_01]

      FROM #tempED z
      INNER JOIN #tempRefToServ x ON x.[EC_Ident] = z.[EC_Ident]
      LEFT JOIN #SNOMED b ON b.snomed_Code = x.[Referred_To_Service_01]

          UPDATE z

        SET [Referred_To_Service_02] = b.snomed_description,
              [Service_Request_Date_02]= x.[Service_Request_Date_02],
              [Service_Request_Time_02]= x.[Service_Request_Time_02],
              [Service_Assessment_Date_02]= x.[Service_Assessment_Date_02],
              [Service_Assessment_Time_02]= x.[Service_Assessment_Time_02]

      FROM #tempED z
      INNER JOIN #tempRefToServ x ON x.[EC_Ident] = z.[EC_Ident]
      LEFT JOIN #SNOMED b ON b.snomed_Code = x.[Referred_To_Service_02]
      WHERE x.[Referred_To_Service_02] IS NOT NULL

          UPDATE z

        SET [Referred_To_Service_03] = b.snomed_description,
              [Service_Request_Date_03]= x.[Service_Request_Date_03],
              [Service_Request_Time_03]= x.[Service_Request_Time_03],
              [Service_Assessment_Date_03]= x.[Service_Assessment_Date_03],
              [Service_Assessment_Time_03]= x.[Service_Assessment_Time_03]

      FROM #tempED z
      INNER JOIN #tempRefToServ x ON x.[EC_Ident] = z.[EC_Ident]
      LEFT JOIN #SNOMED b ON b.snomed_Code = x.[Referred_To_Service_04]
      WHERE x.[Referred_To_Service_04] IS NOT NULL
                UPDATE z

        SET [Referred_To_Service_04] = b.snomed_description,
              [Service_Request_Date_04]= x.[Service_Request_Date_04],
              [Service_Request_Time_04]= x.[Service_Request_Time_04],
              [Service_Assessment_Date_04]= x.[Service_Assessment_Date_04],
              [Service_Assessment_Time_04]= x.[Service_Assessment_Time_04]

      FROM #tempED z
      INNER JOIN #tempRefToServ x ON x.[EC_Ident] = z.[EC_Ident]
      LEFT JOIN #SNOMED b ON b.snomed_Code = x.[Referred_To_Service_04]
      WHERE x.[Referred_To_Service_04] IS NOT NULL

--select top 5000 * from #tempED
/**
    select * from #tempED
    where
    [Referred_To_Service_01]
    in
    (
    'Referral for mental health assessment (procedure)',
    'Referral to liaison psychiatry service (procedure)',
    'Patient referral for alcoholism rehabilitation (procedure)',
    'Refer to Child and Adolescent Mental Health Service (procedure)'
    )

    **/

        IF OBJECT_ID('Tempdb..#tempEDDiagCodes') IS NOT NULL
        DROP TABLE #tempEDDiagCodes
        SELECT
        [EC_Ident],
        [Unique Record ID],
         NULL AS [REduction IN Inappropriate Flag],
        [MH ALL ED SNOMED Diagnosis Codes],
        [Diag1],
        a.SNOMED_Description AS [Diag 1 Description],
        NULL AS [Diag 1 MH Flag],
        [Diag2],
        b.SNOMED_Description AS [Diag 2 Description],
        NULL AS [Diag 2 MH Flag],
        [Diag3],
        c.SNOMED_Description AS [Diag 3 Description],
        NULL AS [Diag 3 MH Flag],
        [Diag4],
        d.SNOMED_Description AS [Diag 4 Description],
        NULL AS [Diag 4 MH Flag],
        [Diag5],
        e.SNOMED_Description AS [Diag 5 Description],
        NULL AS [Diag 5 MH Flag],
        [Diag6],
        f.SNOMED_Description AS [Diag 6 Description],
        NULL AS [Diag 6 MH Flag],
        [Diag7],
        g.SNOMED_Description AS [Diag 7 Description],
        NULL AS [Diag 7 MH Flag],
        [Diag8],
        h.SNOMED_Description AS [Diag 8 Description],
        NULL AS [Diag 8 MH Flag],
        [Diag9],
        i.SNOMED_Description AS [Diag 9 Description],
        NULL AS [Diag 9 MH Flag],
        [Diag10],
        j.SNOMED_Description AS [Diag 10 Description],
        NULL AS [Diag 10 MH Flag],
        [Diag11],
        k.SNOMED_Description AS [Diag 11 Description],
        NULL AS [Diag 11 MH Flag],
        [Diag12],
        l.SNOMED_Description AS [Diag 12 Description],
        NULL AS [Diag 12 MH Flag],
        [Diag13],
        m.SNOMED_Description AS [Diag 13 Description],
        NULL AS [Diag 13 MH Flag],
        [Diag14],
        n.SNOMED_Description AS [Diag 14 Description],
        NULL AS [Diag 14 MH Flag]

        INTO #tempEDDiagCodes

        FROM (
        SELECT
        [EC_Ident],
        [Unique Record ID],
        [MH ALL ED SNOMED Diagnosis Codes],
'Diag'+ CAST(ROW_NUMBER()OVER(PARTITION BY [Unique Record ID] ORDER BY [Unique Record ID]) AS VARCHAR) AS Col,
        Split.value
        FROM #tempED AS Emp
        CROSS APPLY String_split([MH ALL ED SNOMED Diagnosis Codes],',') AS Split

        )
        AS tbl
        PIVOT (MAX(Value) FOR Col IN ([Diag1], [Diag2], [Diag3], [Diag4], [Diag5], [Diag6], [Diag7], [Diag8], [Diag9], [Diag10], [Diag11], [Diag12], [Diag13], [Diag14])
        ) AS Pvt

        LEFT JOIN #SNOMED a ON a.SNOMED_Code = ltrim(rtrim(Diag1))
        LEFT JOIN #SNOMED b ON b.SNOMED_Code = ltrim(rtrim(Diag2))
        LEFT JOIN #SNOMED c ON c.SNOMED_Code = ltrim(rtrim(Diag3))
        LEFT JOIN #SNOMED d ON d.SNOMED_Code = ltrim(rtrim(Diag4))
        LEFT JOIN #SNOMED e ON e.SNOMED_Code = ltrim(rtrim(Diag5))
        LEFT JOIN #SNOMED f ON f.SNOMED_Code = ltrim(rtrim(Diag6))
        LEFT JOIN #SNOMED g ON g.SNOMED_Code = ltrim(rtrim(Diag7))
        LEFT JOIN #SNOMED h ON h.SNOMED_Code = ltrim(rtrim(Diag8))
        LEFT JOIN #SNOMED i ON i.SNOMED_Code = ltrim(rtrim(Diag9))
        LEFT JOIN #SNOMED j ON j.SNOMED_Code = ltrim(rtrim(Diag10))
        LEFT JOIN #SNOMED k ON k.SNOMED_Code = ltrim(rtrim(Diag11))
        LEFT JOIN #SNOMED l ON l.SNOMED_Code = ltrim(rtrim(Diag12))
        LEFT JOIN #SNOMED m ON m.SNOMED_Code = ltrim(rtrim(Diag13))
        LEFT JOIN #SNOMED n ON n.SNOMED_Code = ltrim(rtrim(Diag14))

--select * from  #tempEDDiagCodes

        IF OBJECT_ID('Tempdb..#tempMHDiag') IS NOT NULL
        DROP TABLE #tempMHDiag
        SELECT
        *
        INTO #tempMHDiag
        FROM
        (
        SELECT'52448006'  AS DCode--- dementia
        UNION ALL SELECT'2776000'--- delirium 
        UNION ALL SELECT'33449004'--- personality disorder
        UNION ALL SELECT'72366004'--- eating disorder
        UNION ALL SELECT'197480006'--- anxiety disorder
        UNION ALL SELECT'35489007'--- depressive disorder
        UNION ALL SELECT'13746004'--- bipolar affective disorder
        UNION ALL SELECT'58214004'--- schizophrenia
        UNION ALL SELECT'69322001'--- psychotic disorder
        UNION ALL SELECT'397923000'--- somatisation disorder
        UNION ALL SELECT'30077003'--- somatoform pain disorder
        UNION ALL SELECT'44376007'--- dissociative disorder
        UNION ALL SELECT'17226007'---- adjustment disorder
        UNION ALL SELECT'50705009'---- factitious disorder

        )s

--select * from cte_MH_Diag

    UPDATE dig
        SET dig.[Diag 1 MH Flag] = 1
     FROM #tempEDDiagCodes dig
    INNER JOIN #tempMHDiag a ON a.DCode = ltrim(rtrim([Diag1]))

        UPDATE dig
        SET dig.[Diag 2 MH Flag] = 1
     FROM #tempEDDiagCodes dig
    INNER JOIN #tempMHDiag a ON a.DCode = ltrim(rtrim([Diag2]))

        UPDATE dig
        SET dig.[Diag 3 MH Flag] = 1
     FROM #tempEDDiagCodes dig
    INNER JOIN #tempMHDiag a ON a.DCode = ltrim(rtrim([Diag3]))

        UPDATE dig
        SET dig.[Diag 4 MH Flag] = 1
     FROM #tempEDDiagCodes dig
    INNER JOIN #tempMHDiag a ON a.DCode = ltrim(rtrim([Diag4]))

        UPDATE dig
        SET dig.[Diag 5 MH Flag] = 1
     FROM #tempEDDiagCodes dig
    INNER JOIN #tempMHDiag a ON a.DCode = ltrim(rtrim([Diag5]))

        UPDATE dig
        SET dig.[Diag 6 MH Flag] = 1
     FROM #tempEDDiagCodes dig
    INNER JOIN #tempMHDiag a ON a.DCode = ltrim(rtrim([Diag6]))

        UPDATE dig
        SET dig.[Diag 7 MH Flag] = 1
     FROM #tempEDDiagCodes dig
    INNER JOIN #tempMHDiag a ON a.DCode = ltrim(rtrim([Diag7]))

        UPDATE dig
        SET dig.[Diag 8 MH Flag] = 1
     FROM #tempEDDiagCodes dig
    INNER JOIN #tempMHDiag a ON a.DCode = ltrim(rtrim([Diag8]))

        UPDATE dig
        SET dig.[Diag 9 MH Flag] = 1
     FROM #tempEDDiagCodes dig
    INNER JOIN #tempMHDiag a ON a.DCode = ltrim(rtrim([Diag9]))

        UPDATE dig
        SET dig.[Diag 10 MH Flag] = 1
     FROM #tempEDDiagCodes dig
    INNER JOIN #tempMHDiag a ON a.DCode = ltrim(rtrim([Diag10]))

        UPDATE dig
        SET dig.[Diag 11 MH Flag] = 1
     FROM #tempEDDiagCodes dig
    INNER JOIN #tempMHDiag a ON a.DCode = ltrim(rtrim([Diag11]))

        UPDATE dig
        SET dig.[Diag 12 MH Flag] = 1
     FROM #tempEDDiagCodes dig
    INNER JOIN #tempMHDiag a ON a.DCode = ltrim(rtrim([Diag12]))

        UPDATE dig
        SET dig.[Diag 13 MH Flag] = 1
     FROM #tempEDDiagCodes dig
    INNER JOIN #tempMHDiag a ON a.DCode = ltrim(rtrim([Diag13]))

        UPDATE dig
        SET dig.[Diag 14 MH Flag] = 1
     FROM #tempEDDiagCodes dig
    INNER JOIN #tempMHDiag a ON a.DCode = ltrim(rtrim([Diag14]))

        UPDATE f

            SET [REduction IN Inappropriate Flag] = CASE WHEN [Diag 1 MH Flag] = 1 AND [Diag 2 Description] IS NULL THEN 1 ELSE NULL END

        FROM #tempEDDiagCodes f

        UPDATE MHD

        SET mhd.[Reduction IN Inappropriate Flag] = edc.[REduction IN Inappropriate Flag],
            mhd.[Secondary Diagnosis Code] = edc.Diag2,
            mhd.[Secondary Diagnosis Description] = edc.[Diag 2 Description],
            mhd.[Third Diagnosis Code] = edc.Diag3,
            mhd.[Third Diagnosis Description] = edc.[Diag 3 Description],
            mhd.[Fourth Diagnosis Code] = edc.Diag4,
            mhd.[Fourth Diagnosis Description] = edc.[Diag 4 Description]

        FROM #tempED MHD
        INNER JOIN #tempEDDiagCodes edc ON edc.EC_Ident = MHD.EC_Ident

        INSERT INTO [PATLondon].[ECDS_Presentation_Diagnosis_Codes_and_Descriptions]

        SELECT * FROM #tempEDDiagCodes a
        WHERE NOT EXISTS
                    (
                    SELECT
                    EC_Ident
                    FROM [PATLondon].[ECDS_Presentation_Diagnosis_Codes_and_Descriptions]x
                    WHERE x.ec_ident = a.EC_Ident
                    )



                    DELETE f FROM [PATLondon].[ECDS_All_Presentations_London]f WHERE Arrival_Date >= @StartDate

INSERT INTO [PATLondon].[ECDS_All_Presentations_London]
        SELECT
[Unique Record ID]
      , [Der_Pseudo_NHS_Number]
      , [EC_Ident]
      , [Generated_Record_ID]
      , [Unique_CDS_identifier]
      , [Attendance_Unique_Identifier]
      , [RowOrder]
      , [Gender]
      , [Age at Arrival]
      , [Age GROUP]
      , [AgeCat]
      , [Broad Ethnic Category]
      , [Ethnic Category]
      , [Derived Broad Ethnic Category]
      , [Index_Of_Multiple_Deprivation_Decile]
      , [Index_Of_Multiple_Deprivation_Decile_Description]
      , [Rural_Urban_Indicator]
      , [Ethnic proportion per 100000 of London Borough 2020]
      , [Known to MH Services Flag]
      , [Last Completed IP Spell]
      , [IP Spell Provider Name]
      , [UniqHospProvSpellID]
      , [IP Spell UniqServReqID]
      , [ED Presentation within 28 days of Completed IP SPell]
      , [Days between Completed IP Spell AND ED Presentation]
      , [GP_Practice_Code]
      , [Practice Name]
      , [GP Practice PostCode No Gaps]
      , [Patient GP Practice 2019 CCG Code]
      , [Patient GP Local Authority Name]
      , [Patient GP Practice Region]
      , [Borough Type]
      , [Patient ICS]
      , [Local MH Trust]
      , [Patient GP 2011_LSOA]
      , [Patient GP 2011_MS0A]
      , [Accommodation_Status_SNOMED_CT]
      , [Attendance_Postcode_District]
      , [Attendance_HES_CCG_From_Treatment_Origin]
      , [Attendance_HES_CCG_From_Treatment_Site_Code]
      , [Attendance_LSOA_Provider_Distance]
      , [Attendance_LSOA_Treatment_Site_Distance]
      , [AttendanceSource]
      , [Patient_Type]
      , [Der_Provider_Code]
      , [Der_Provider_Name]
      , [Der_Provider_Site_Code]
      , [Provider PostCode]
      , [Provider Postcode District]
      , [Provider 2011 LSOA]
      , [Der_Provider_Site_Name]
      , [Provider_Region_Code]
      , [Provider_Region_Name]
      , [Provider_CCGCode]
      , [Provider_CCG name]
      , [Provider ICB]
      , [Provider_STPCode]
      , [Provider STP name]
      , [Month Year]
      , [Arrival_Date]
      , [ArrivalDate FY]
      , [Arrival Hour]
      , [Arrival DATE Time]
      , [Arrival Mode]
      , [EC_Initial_Assessment_Date]
      , [EC_Initial_Assessment_Time]
      , [EC_Initial_Assessment_Time_Since_Arrival]
      , [EC_Departure_Date]
      , [EC_Departure_Time]
      , [Der_EC_Duration] AS [Total Time IN ED]
      , [Time Grouper]
      , [6 Hour Breach]
      , [Time OVER 6 Hours]
      , [12 Hour Breach]
      , [Time OVER 12 Hours]
      , [24hrs_breach]
      , [EC_Seen_For_Treatment_Date]
      , [EC_Seen_For_Treatment_Time]
      , [EC_Seen_For_Treatment_Time_Since_Arrival]
      , [EC_Conclusion_Date]
      , [EC_Conclusion_Time]
      , [EC_Conclusion_Time_Since_Arrival]
      , [EC_Decision_To_Admit_Date]
      , [EC_Decision_To_Admit_Time]
      , [EC_Decision_To_Admit_Time_Since_Arrival]
      , [Decision_To_Admit_Receiving_Site]
      , [Decision To Admit Treatment Function Code]
      , [Treatment Function Desc]
      , [Treatment Function GROUP]
      , [MH ED Chief Complaint SNOMED Code]
      , [MH ED Chief Complaint Description]
      , [MH ED Injury Intent SNOMED Code]
      , [MH ED Injury Intent Description]
      , [MH ALL ED SNOMED Diagnosis Codes]
      , [MH Primary SNOMED Diagnosis Code]
      , [MH Primary Diagnosis Description]
      , [Secondary Diagnosis Code]
      , [Secondary Diagnosis Description]
      , [Third Diagnosis Code]
      , [Third Diagnosis Description]
      , [Fourth Diagnosis Code]
      , [Fourth Diagnosis Description]
      , [Reduction IN Inappropriate Flag]
      , [Comorbidity_01]
      , [Comorbidity_02]
      , [Comorbidity_03]
      , [Comorbidity_04]
      , [Referred_To_Service_01]
      , [Service_Request_Date_01]
      , [Service_Request_Time_01]
      , [Service_Assessment_Date_01]
      , [Service_Assessment_Time_01]
      , [Referred_To_Service_02]
      , [Service_Request_Date_02]
      , [Service_Request_Time_02]
      , [Service_Assessment_Date_02]
      , [Service_Assessment_Time_02]
      , [Referred_To_Service_03]
      , [Service_Request_Date_03]
      , [Service_Request_Time_03]
      , [Service_Assessment_Date_03]
      , [Service_Assessment_Time_03]
      , [Referred_To_Service_04]
      , [Service_Request_Date_04]
      , [Service_Request_Time_04]
      , [Service_Assessment_Date_04]
      , [Service_Assessment_Time_04]
      , [DischargeDestination]
      , [Discharge Followup Description]
      , [Chief Complaint Flag]
      , [Injury Flag]
      , [Injury Intent Flag]
      , [Diagnosis Flag]
      , [Mental Health Presentation Flag]
      , [Self Harm Flag]
      , NULL AS [KnownInLast24Months]
      , NULL [PreviouslyKnown]

FROM #tempED


 DROP TABLE #tempED



 IF OBJECT_ID('Tempdb..#tempRef') IS NOT NULL
DROP TABLE #tempRef

 SELECT

 ROW_NUMBER() OVER (
PARTITION BY a.UniqServReqID, a.OrgIDProv
ORDER BY a.UniqMonthID DEsc, a.UniqSubmissionID desc ) AS RowOrder,
a.UniqServReqID,
 sor.Description AS [Source of Referral],
 a.ReferralRequestReceivedDate,
 a.ServDischDate,
 st.ReferRejectionDate,
 b.Der_Pseudo_NHS_Number
 INTO #tempRef

 FROM [MESH_MHSDS].[MHS101Referral_2]a WITH(NOLOCK)
 INNER JOIN [MESH_MHSDS].[MHSDS_SubmissionFlags_1] sf WITH(NOLOCK) ON sf.NHSEUniqSubmissionID = a.NHSEUniqSubmissionID AND sf.Der_IsLatest ='Y'
 LEFT JOIN [MESH_MHSDS].[MHS102ServiceTypeReferredTo_2] st WITH(NOLOCK) ON st.RecordNumber = a.RecordNumber AND a.UniqServReqID = st.UniqServReqID
 LEFT JOIN [PATLondon].[DIM_Date] dt WITH(NOLOCK) ON dt.[Calendar Day] = a.ReferralRequestReceivedDate
 LEFT JOIN [MESH_MHSDS].[MHS001MPI]b WITH(NOLOCK) ON b.Person_ID = a.Person_ID
                                            AND b.UniqSubmissionID = a.UniqSubmissionID
                                            AND b.UniqMonthID = a.UniqMonthID
                                            AND b.RecordNumber = a.RecordNumber

LEFT JOIN [PATLondon].[Ref_Source_Of_Referral_for_Mental_Health_Services]sor WITH(NOLOCK) ON sor.Code = a.SourceOfReferralMH
LEFT JOIN [PATLondon].[MH_ALL_ED_Referrals]plr ON plr.UniqServReqID = a.UniqServReqID

     WHERE
     A.ReferralRequestReceivedDate >= @StartDate-- dateadd(year,-1,convert(datetime, cast(year(dateadd(month, -3, getdate())) as varchar(10)) + '-04-01', 120)) 
     AND b.Der_Pseudo_NHS_Number IS NOT NULL
     AND plr.UniqServReqID IS NULL

    DELETE FROM #tempRef WHERE RowOrder <> 1



    DELETE a FROM [PATLondon].[MH_ALL_ED_Referrals]a WHERE EXISTS (SELECT x. UniqServReqID FROM #tempRef x WHERE x.UniqServReqID = a.UniqServReqID)

    IF EXISTS(SELECT * FROM sys.indexes WHERE OBJECT_ID = OBJECT_ID('[PATLondon].[MH_ALL_ED_Referrals]') AND NAME ='ix_ED_Referral')
    DROP INDEX ix_ED_Referral ON [PATLondon].[MH_ALL_ED_Referrals]

        INSERT INTO [PATLondon].[MH_ALL_ED_Referrals]
        SELECT
        DISTINCT
        RowOrder,
        UniqServReqID,
        CONVERT(DATE, ReferralRequestReceivedDate) AS ReferralRequestReceivedDate,
        [Source of Referral],
        CONVERT(DATE, ServDischDate) AS ServDischDate,
        CONVERT(DATE, ReferRejectionDate) AS ReferRejectionDate,
        Der_Pseudo_NHS_Number,
        NULL AS der_Is_Latest

        FROM #tempRef

         CREATE INDEX ix_ED_Referral ON [PATLondon].[MH_ALL_ED_Referrals] ( UniqServReqID, Der_Pseudo_NHS_Number, ReferralRequestReceivedDate)



     UPDATE x

     SET x.[Known to MH Services Flag] = CASE

                                        WHEN
                                           ( (DATEDIFF(day, ccc.ReferralRequestReceivedDate, CONVERT(DATE, x.Arrival_Date) )>= 0) AND DATEDIFF(MONTH, ccc.ReferralRequestReceivedDate, CONVERT(DATE, x.Arrival_Date)) <=6 )
                                           THEN 1
                                        WHEN
                                            (
                                                (DATEDIFF(MONTH, ccc.ReferralRequestReceivedDate, CONVERT(DATE, x.Arrival_Date))>6 )
                                            AND (ServDischDate IS NULL OR (ServDischDate > Arrival_Date))
                                            AND (ReferRejectionDate IS NULL OR (ReferRejectionDate > Arrival_Date))
                                            )

                                          THEN 1
                                        ELSE NULL
                                        END
                                        ,

    x.[Derived Broad Ethnic Category] = CASE WHEN [Ethnic Category] IS NULL AND x.[Derived Broad Ethnic Category] IS NULL THEN'Not Known / Not Stated / Incomplete' ELSE x.[Derived Broad Ethnic Category] END

     FROM [PATLondon].[ECDS_All_Presentations_London]x
    LEFT JOIN [PATLondon].[MH_ALL_ED_Referrals] ccc ON ccc.Der_Pseudo_NHS_Number = x.Der_Pseudo_NHS_Number
                                                    AND ccc.ReferralRequestReceivedDate <= x.Arrival_Date
    WHERE x.Arrival_Date >= @StartDate



    UPDATE f

         SET f.[Known to MH Services Flag] = CASE
                                              WHEN ccc.[Source of Referral] ='Acute Secondary Care:Emergency Care Department'
                                                    THEN NULL
                                                ELSE f.[Known to MH Services Flag]
                                            END

    FROM [PATLondon].[ECDS_All_Presentations_London]f
    INNER JOIN [PATLondon].[MH_ALL_ED_Referrals]ccc ON ccc.Der_Pseudo_NHS_Number = f.Der_Pseudo_NHS_Number
                                                    AND CONVERT(DATE, ccc.ReferralRequestReceivedDate) = CONVERT(DATE, f.Arrival_Date)
    WHERE f.Arrival_Date >= @StartDate

    UPDATE f

         SET f.[Known to MH Services Flag] = 1

    FROM [PATLondon].[ECDS_All_Presentations_London]f
    INNER JOIN [PATLondon].[MH_ALL_ED_Referrals] ccc ON ccc.Der_Pseudo_NHS_Number = f.Der_Pseudo_NHS_Number

    WHERE
    (
    DATEDIFF(day, ccc.ReferralRequestReceivedDate, CONVERT(DATE, f.Arrival_Date)) > 0
    AND
    DATEDIFF(month, ccc.ReferralRequestReceivedDate, CONVERT(DATE, f.Arrival_Date)) <=6
    )
    AND ccc.[Source of Referral] ='Acute Secondary Care:Emergency Care Department'
    AND f.Arrival_Date >= @StartDate

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  UPDATE x

     SET x.[KnownInLast24Months] = CASE

                                        WHEN
                                           ( (DATEDIFF(day, ccc.ReferralRequestReceivedDate, CONVERT(DATE, x.Arrival_Date) )>= 0) AND DATEDIFF(MONTH, ccc.ReferralRequestReceivedDate, CONVERT(DATE, x.Arrival_Date)) <=24 )
                                           THEN 1
                                        WHEN
                                            (
                                                (DATEDIFF(MONTH, ccc.ReferralRequestReceivedDate, CONVERT(DATE, x.Arrival_Date))>24 )
                                            AND (ServDischDate IS NULL OR (ServDischDate > Arrival_Date))
                                            AND (ReferRejectionDate IS NULL OR (ReferRejectionDate > Arrival_Date))
                                            )

                                          THEN 1
                                        ELSE NULL
                                        END

     FROM [PATLondon].[ECDS_All_Presentations_London]x
    LEFT JOIN [PATLondon].[MH_ALL_ED_Referrals] ccc ON ccc.Der_Pseudo_NHS_Number = x.Der_Pseudo_NHS_Number
                                                    AND ccc.ReferralRequestReceivedDate <= x.Arrival_Date
    WHERE x.Arrival_Date >= @StartDate

    UPDATE f

         SET f.[KnownInLast24Months] = CASE
                                              WHEN ccc.[Source of Referral] ='Acute Secondary Care:Emergency Care Department'
                                                    THEN NULL
                                                ELSE f.[KnownInLast24Months]
                                            END

    FROM [PATLondon].[ECDS_All_Presentations_London]f
    INNER JOIN [PATLondon].[MH_ALL_ED_Referrals]ccc ON ccc.Der_Pseudo_NHS_Number = f.Der_Pseudo_NHS_Number
                                                    AND CONVERT(DATE, ccc.ReferralRequestReceivedDate) = CONVERT(DATE, f.Arrival_Date)
    WHERE f.Arrival_Date >= @StartDate

    UPDATE f

         SET f.[KnownInLast24Months] = 1

    FROM [PATLondon].[ECDS_All_Presentations_London]f
    INNER JOIN [PATLondon].[MH_ALL_ED_Referrals] ccc ON ccc.Der_Pseudo_NHS_Number = f.Der_Pseudo_NHS_Number

    WHERE
    (
    DATEDIFF(day, ccc.ReferralRequestReceivedDate, CONVERT(DATE, f.Arrival_Date)) > 0
    AND
    DATEDIFF(month, ccc.ReferralRequestReceivedDate, CONVERT(DATE, f.Arrival_Date)) <=24
    )
    AND ccc.[Source of Referral] ='Acute Secondary Care:Emergency Care Department'
    AND f.Arrival_Date >= @StartDate

---Previously known

  UPDATE x

     SET x.[PreviouslyKnown] = CASE
                                        WHEN
                                           ( (DATEDIFF(day, ccc.ReferralRequestReceivedDate, CONVERT(DATE, x.Arrival_Date) )>= 0) AND DATEDIFF(MONTH, ccc.ReferralRequestReceivedDate, CONVERT(DATE, x.Arrival_Date)) >24 )
                                           THEN 1
                                        WHEN
                                            (
                                                (DATEDIFF(MONTH, ccc.ReferralRequestReceivedDate, CONVERT(DATE, x.Arrival_Date))>24 )
                                            AND (ServDischDate IS NULL OR (ServDischDate > Arrival_Date))
                                            AND (ReferRejectionDate IS NULL OR (ReferRejectionDate > Arrival_Date))
                                            )

                                          THEN 1
                                        ELSE NULL
                                        END

     FROM [PATLondon].[ECDS_All_Presentations_London]x
    LEFT JOIN [PATLondon].[MH_ALL_ED_Referrals] ccc ON ccc.Der_Pseudo_NHS_Number = x.Der_Pseudo_NHS_Number
                                                    AND ccc.ReferralRequestReceivedDate <= x.Arrival_Date
    WHERE x.Arrival_Date >= @StartDate



    UPDATE f

         SET f.[PreviouslyKnown] = CASE
                                              WHEN ccc.[Source of Referral] ='Acute Secondary Care:Emergency Care Department'
                                                    THEN NULL
                                                ELSE f.[PreviouslyKnown]
                                            END

    FROM [PATLondon].[ECDS_All_Presentations_London]f
    INNER JOIN [PATLondon].[MH_ALL_ED_Referrals]ccc ON ccc.Der_Pseudo_NHS_Number = f.Der_Pseudo_NHS_Number
                                                    AND CONVERT(DATE, ccc.ReferralRequestReceivedDate) = CONVERT(DATE, f.Arrival_Date)
    WHERE f.Arrival_Date >= @StartDate

    UPDATE f

         SET f.[PreviouslyKnown] = 1

    FROM [PATLondon].[ECDS_All_Presentations_London]f
    INNER JOIN [PATLondon].[MH_ALL_ED_Referrals] ccc ON ccc.Der_Pseudo_NHS_Number = f.Der_Pseudo_NHS_Number

    WHERE
    (
    DATEDIFF(day, ccc.ReferralRequestReceivedDate, CONVERT(DATE, f.Arrival_Date)) > 0
    AND
    DATEDIFF(month, ccc.ReferralRequestReceivedDate, CONVERT(DATE, f.Arrival_Date)) >24
    )
    AND ccc.[Source of Referral] ='Acute Secondary Care:Emergency Care Department'
    AND f.Arrival_Date >= @StartDate


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    UPDATE y

    SET y.[Ethnic proportion per 100000 of London Borough 2020] = CAST((1/NULLIF(CONVERT(FLOAT, ep.Value), 0)) AS FLOAT) *100000

    FROM [PATLondon].[ECDS_All_Presentations_London]y
    LEFT JOIN [PATLondon].[Ref_Ethnicity_2020_Census_Population_by_London_Borough]ep ON ep.[Broad Ethnic Category] = y.[Derived Broad Ethnic Category]
                                                                                                        AND ep.Borough= y.[Patient GP Local Authority Name]

    WHERE y.[Ethnic proportion per 100000 of London Borough 2020]IS NULL

DROP INDEX ix_ED_Attendance ON [PATLondon].[ECDS_All_Presentations_London]

CREATE INDEX ix_ED_Attendance ON [PATLondon].[ECDS_All_Presentations_London] ([Unique Record ID], EC_Ident, Unique_CDS_identifier, [arrival DATE time])

        UPDATE f
        SET f.[Last Completed IP Spell] = (SELECT MAX(DischDateHospProvSpell) FROM #tempHO1 z WHERE z.DischDateHospProvSpell < f.Arrival_Date AND z.Der_Pseudo_NHS_Number = f.Der_Pseudo_NHS_Number )

        FROM [PATLondon].[ECDS_All_Presentations_London]f
        INNER JOIN #tempHO1 g ON g.Der_Pseudo_NHS_Number = f.Der_Pseudo_NHS_Number
        WHERE f.Der_Pseudo_NHS_Number IS NOT NULL
        AND f.Arrival_Date >= @StartDate



        UPDATE f
        SET f.[IP Spell Provider Name] = g.Provider_Name,
        f.[UniqHospProvSpellID] = g.UniqHospProvSpellID,
        f.[IP Spell UniqServReqID] = g.UniqServReqID

        FROM [PATLondon].[ECDS_All_Presentations_London]f
        INNER JOIN #tempHO1 g ON g.Der_Pseudo_NHS_Number = f.Der_Pseudo_NHS_Number
        AND g.DischDateHospProvSpell = f.[Last Completed IP Spell]
        AND f.Arrival_Date >= @StartDate

        UPDATE s

        SET s.[Days between Completed IP Spell AND ED Presentation] = DATEDIFF(day, [Last Completed IP Spell], Arrival_Date),
        s.[ED Presentation within 28 days of Completed IP SPell]= CASE WHEN DATEDIFF(day, [Last Completed IP Spell], Arrival_Date) <=28 THEN 1 ELSE NULL END

        FROM [PATLondon].[ECDS_All_Presentations_London]s
        WHERE [Last Completed IP Spell] IS NOT NULL
        AND s.Arrival_Date >= @StartDate


