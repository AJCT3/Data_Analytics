CREATE PROCEDURE [PATLondon].[SP_Update GP Details]
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('tempdb..#TempGP1') IS NOT NULL
        DROP TABLE #TempGP1;

    SELECT DISTINCT
        B.[GP_Code] AS GP_CODE,
        B.[GP_PCN_Code] AS GP_PCN_Code,
        B.[GP_PCN_Name] AS GP_PCN_Name,
        B.[GP_STP_Code] AS [GP_STP_Code],
        REPLACE(B.[GP_STP_Name], ' INTEGRATED CARE BOARD', '') AS GP_STP_Name,
        B.[GP_Region_Code] AS GP_Region_Code,
        REPLACE(B.[GP_Region_Name], ' COMMISSIONING REGION', '') AS GP_Region_Name,
        C.PRACTICE AS Practice_code,
        C.[CCG2019_20_Q4] AS CCG1920,
        REPLACE(D.Organisation_Name, ' CCG', '') AS [New CCG]
    INTO #TempGP1
    FROM [Reporting_UKHD_ODS].[GP_Hierarchies_All_1] B
    LEFT JOIN [Internal_Reference].[RightCare_practice_CCG_pcn_quarter_lookup_1] C
        ON B.[GP_Code] COLLATE DATABASE_DEFAULT = C.Practice COLLATE DATABASE_DEFAULT
    LEFT JOIN [Reporting_UKHD_ODS].[Commissioner_Hierarchies] D
        ON D.Organisation_Code COLLATE DATABASE_DEFAULT = C.[CCG2019_20_Q4] COLLATE DATABASE_DEFAULT;

    IF OBJECT_ID('tempdb..#TempGP2') IS NOT NULL
        DROP TABLE #TempGP2;

    SELECT DISTINCT
        GP_PCN_Code,
        GP_PCN_Name,
        [New CCG],
        COUNT(GP_PCN_NAME) AS GPS
    INTO #TempGP2
    FROM #TempGP1 X
    GROUP BY
        GP_PCN_Code,
        GP_PCN_Name,
        [New CCG];

    IF OBJECT_ID('tempdb..#TempGP3') IS NOT NULL
        DROP TABLE #TempGP3;

    SELECT
        GP_PCN_Code,
        GP_PCN_Name,
        [New CCG],
        ROW_NUMBER() OVER (
            PARTITION BY GP_PCN_Code, GP_PCN_Name
            ORDER BY GPS DESC
        ) AS LA_ORDER
    INTO #TempGP3
    FROM #TempGP2;

    IF OBJECT_ID('tempdb..#TempGP4') IS NOT NULL
        DROP TABLE #TempGP4;

    SELECT DISTINCT
        B.[GP_Code] AS GP_CODE,
        B.GP_Name,
        B.[GP_PCN_Code] AS GP_PCN_Code,
        B.[GP_PCN_Name] AS GP_PCN_Name,
        B.[GP_STP_Code] AS [GP_STP_Code],
        REPLACE(B.[GP_STP_Name], ' INTEGRATED CARE BOARD', '') AS GP_STP_Name,
        B.[GP_Region_Code] AS GP_Region_Code,
        REPLACE(B.[GP_Region_Name], ' COMMISSIONING REGION', '') AS GP_Region_Name,
        C.PRACTICE AS Practice_code,
        C.[CCG2019_20_Q4] AS CCG1920,
        REPLACE([GP_Postcode], ' ', '') AS [PCDS_NoGaps],
        REPLACE(LEFT([GP_Postcode], 7), ' ', '') AS [PCDS_7],
        REPLACE(LEFT([GP_Postcode], 6), ' ', '') AS [PCDS_6],
        REPLACE(LEFT([GP_Postcode], 5), ' ', '') AS [PCDS_5],
        REPLACE(LEFT([GP_Postcode], 4), ' ', '') AS [PCDS_4],
        LTRIM(RTRIM(LEFT([GP_Postcode], 3))) AS [PCDS_3],
        CAST(NULL AS VARCHAR(255)) AS [2019_CCG_Name],
        Z.[New CCG] AS [New CCG],
        ROW_NUMBER() OVER (
            PARTITION BY GP_CODE
            ORDER BY CASE WHEN GP_PCN_Rel_End_Date IS NULL THEN 1 ELSE 0 END DESC,
                     GP_PCN_Rel_End_Date DESC
        ) AS GP_ORDER,
        CAST(NULL AS VARCHAR(9)) AS [Lower_Super_Output_Area_Code],
        CAST(NULL AS VARCHAR(80)) AS [Lower_Super_Output_Area_Name],
        CAST(NULL AS VARCHAR(9)) AS [Middle_Super_Output_Area_Code],
        CAST(NULL AS VARCHAR(80)) AS [Middle_Super_Output_Area_Name],
        CAST(NULL AS VARCHAR(9)) AS [Longitude],
        CAST(NULL AS VARCHAR(9)) AS [Latitude],
        CAST(NULL AS VARCHAR(40)) AS [Spatial_Accuracy]
    INTO #TempGP4
    FROM [Reporting_UKHD_ODS].[GP_Hierarchies_All_1] B
    LEFT JOIN [Internal_Reference].[RightCare_practice_CCG_pcn_quarter_lookup_1] C
        ON B.[GP_Code] COLLATE DATABASE_DEFAULT = C.Practice COLLATE DATABASE_DEFAULT
    LEFT JOIN #TempGP3 Z
        ON Z.GP_PCN_CODE = B.GP_PCN_CODE
    WHERE Z.LA_ORDER = 1;

    UPDATE F
    SET
        F.[Lower_Super_Output_Area_Code] = G.[Lower_Super_Output_Area_Code],
        F.[Lower_Super_Output_Area_Name] = G.[Lower_Super_Output_Area_Name],
        F.[Middle_Super_Output_Area_Code] = G.[Middle_Super_Output_Area_Code],
        F.[Middle_Super_Output_Area_Name] = G.[Middle_Super_Output_Area_Name],
        F.[Longitude] = G.[Longitude],
        F.[Latitude] = G.[Latitude],
        F.[Spatial_Accuracy] = G.[Spatial_Accuracy]
    FROM #TempGP4 F
    INNER JOIN [UKHD_Other].[National_Statistics_Postcode_Lookup_SCD_1] G
        ON REPLACE([Postcode_1], ' ', '') = F.[PCDS_NoGaps];

    UPDATE G
    SET G.[2019_CCG_Name] = [2019 CCG NAME]
    FROM #TempGP4 G
    LEFT JOIN [PATLondon].[Ref_2019_Postcodes_With_Legacy_CCG] H
        ON REPLACE(H.[PCDS], ' ', '') = G.[PCDS_NoGaps];

    UPDATE G
    SET G.[2019_CCG_Name] = [2019 CCG NAME]
    FROM #TempGP4 G
    LEFT JOIN [PATLondon].[Ref_2019_Postcodes_With_Legacy_CCG] H
        ON H.[PCDS Trim 7] = G.PCDS_7
    WHERE G.[2019_CCG_Name] IS NULL;

    UPDATE G
    SET G.[2019_CCG_Name] = [2019 CCG NAME]
    FROM #TempGP4 G
    LEFT JOIN [PATLondon].[Ref_2019_Postcodes_With_Legacy_CCG] H
        ON H.[PCDS Trim 6] = G.PCDS_6
    WHERE G.[2019_CCG_Name] IS NULL;

    IF OBJECT_ID('[PATLondon].[Ref_GP_Data]') IS NOT NULL
        DROP TABLE [PATLondon].[Ref_GP_Data];

    SELECT
        GP_CODE,
        Practice_code AS [GP_Practice_Code],
        GP_Name,
        GP_PCN_Code,
        GP_PCN_Name,
        [GP_STP_Code],
        GP_STP_Name,
        GP_Region_Code,
        GP_Region_Name,
        CCG1920,
        [PCDS_NoGaps],
        [2019_CCG_Name],
        LA.Name AS [Local_Authority],
        [Lower_Super_Output_Area_Code],
        [Lower_Super_Output_Area_Name],
        [Middle_Super_Output_Area_Code],
        [Middle_Super_Output_Area_Name],
        [Longitude],
        [Latitude],
        [Spatial_Accuracy]
    INTO [PATLondon].[Ref_GP_Data]
    FROM #TempGP4 B
    LEFT JOIN [PATLondon].[Ref_PostCode_to_Local_Authority] LA
        ON LA.[PostCode No Gaps] = [PCDS_NoGaps]
    WHERE GP_ORDER = 1;

    UPDATE GP
    SET GP.[Local_Authority] = COALESCE(LA2.Name, LA3.Name, LA4.Name, LA5.Name)
    FROM [PATLondon].[Ref_GP_Data] GP
    INNER JOIN #TempGP4 GP4
        ON GP4.Practice_code = GP.GP_Practice_Code
    LEFT JOIN [PATLondon].[Ref_PostCode_to_Local_Authority] LA2
        ON LEFT(LA2.[PostCode No Gaps], 7) = GP4.PCDS_7
    LEFT JOIN [PATLondon].[Ref_PostCode_to_Local_Authority] LA3
        ON LEFT(LA3.[PostCode No Gaps], 6) = GP4.PCDS_6
    LEFT JOIN [PATLondon].[Ref_PostCode_to_Local_Authority] LA4
        ON LEFT(LA4.[PostCode No Gaps], 5) = GP4.PCDS_5
    LEFT JOIN [PATLondon].[Ref_PostCode_to_Local_Authority] LA5
        ON LEFT(LA5.[PostCode No Gaps], 4) = GP4.PCDS_4
    WHERE GP.[Local_Authority] IS NULL;

    -- SELECT TOP 500 * FROM [PATLondon].[Ref_PostCode_to_Local_Authority];
    -- SELECT * FROM [PATLondon].[Ref_GP_Data] WHERE GP_Region_Name = 'London' AND [Local_Authority] IS NULL;
    -- SELECT * FROM #TempGP4 WHERE [PCDS_NoGaps] = 'SW59JA';

    IF OBJECT_ID('[PATLondon].[Ref_Trusts_and_Sites]') IS NOT NULL
        DROP TABLE [PATLondon].[Ref_Trusts_and_Sites];

    SELECT DISTINCT
        A.Parent_Organisation_Code,
        B.Organisation_Name AS [Parent Organisation Name],
        B.Postcode AS [Parent Organisation Postcode],
        LEFT(B.Postcode, 3) AS [Parent Organisation Postcode District],
        C.[yr2011_LSOA] AS [Parent Organisation yr2011 LSOA],
        CASE
            WHEN A.Parent_Organisation_Code IN ('RAT', 'RKL', 'RPG', 'RQY', 'RRP', 'RV3', 'RV5', 'RWK', 'TAF') THEN 1
            ELSE NULL
        END AS [MH Trust Flag],
        CAST(NULL AS VARCHAR(255)) AS [MH Provider Abbrev],
        A.Organisation_Code AS [Site Organisation Code],
        A.Organisation_Name AS [Site Name],
        A.Postcode AS [Site  Postcode],
        LEFT(A.Postcode, 3) AS [Site Postcode District],
        D.[yr2011_LSOA] AS [Site yr2011 LSOA]
    INTO [PATLondon].[Ref_Trusts_and_Sites]
    FROM [UKHD_ODS].[NHS_Trusts_SCD_1] B
    LEFT JOIN [UKHD_ODS].[NHS_Trust_Sites_Assets_And_Units_SCD_1] A
        ON B.Organisation_Code = A.Parent_Organisation_Code
       AND A.[Is_Latest] = 1
    LEFT JOIN [UKHD_ODS].[Postcode_Grid_Refs_Eng_Wal_Sco_And_NI_SCD_1] C
        ON REPLACE(C.[Postcode_8_chars], ' ', '') = REPLACE(B.Postcode, ' ', '')
       AND C.[Is_Latest] = 1
    LEFT JOIN [UKHD_ODS].[Postcode_Grid_Refs_Eng_Wal_Sco_And_NI_SCD_1] D
        ON REPLACE(D.[Postcode_8_chars], ' ', '') = REPLACE(A.Postcode, ' ', '')
       AND D.[Is_Latest] = 1
    WHERE B.[Is_Latest] = 1;

    UPDATE R
    SET R.[MH Provider Abbrev] = CASE
        WHEN Parent_Organisation_Code = 'RAT' THEN 'NELFT'
        WHEN Parent_Organisation_Code = 'RKL' THEN 'WLT'
        WHEN Parent_Organisation_Code = 'RV3' THEN 'CNWL'
        WHEN Parent_Organisation_Code = 'RPG' THEN 'OXLEAS'
        WHEN Parent_Organisation_Code = 'RWK' THEN 'ELFT'
        WHEN Parent_Organisation_Code = 'RRP' THEN 'BEH'
        WHEN Parent_Organisation_Code = 'RQY' THEN 'SWLStG'
        WHEN Parent_Organisation_Code = 'RV5' THEN 'SLAM'
        WHEN Parent_Organisation_Code = 'TAF' THEN 'CANDI'
        ELSE NULL
    END
    FROM [PATLondon].[Ref_Trusts_and_Sites] R;
END;
GO
