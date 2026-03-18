/*
    File: inpatient_spells_v1_8_cleaned.sql
    Purpose: Git-friendly cleanup of the original "Inpatietns V1.8.sql" script.

    Notes:
    - Business logic has been preserved as closely as possible.
    - This pass focuses on readability, casing, spacing, and general consistency.
    - Existing object names, output column names, and load behaviour have been retained.
*/

----------------------------------------------------------------------------------------
 IF OBJECT_ID('tempdb..#tempSPellURef') IS NOT NULL
DROP TABLE #tempSPellURef
SELECT
DISTINCT

UniqServReqID

INTO #tempSPellURef
FROM [MESH_MHSDS].[MHS501HospProvSpell]
----------------------------------------------------------------------------------------

 IF OBJECT_ID('tempdb..#tempRef') IS NOT NULL
DROP TABLE #tempRef
SELECT
DISTINCT
a.UniqServReqID,
ReferralRequestReceivedDate,
[Referring Organisation],
[Referring Org Type],
[Referring Care Professional Staff Group],
[Source of REferral] as [Referral Source],
--[Type of Service Referred to],
[Primary Reason for Referral],
[Clinical Priority],
ServDischDate,
ReferRejectionDate

INTO #tempRef

  FROM [PATLondon].[MH_Referrals_with_Care_Contacts_London]a
  INNER JOIN #tempSPellURef b on b.UniqServReqID = a.UniqServReqID


----------------------------------------------------------------------------------------------

        IF OBJECT_ID('tempdb..#TempDiagCodes') IS NOT NULL
        DROP TABLE #TempDiagCodes
        SELECT
        [ICD10_L4_Code]

, LTRIM(RTRIM(SUBSTRING([ICD10_L4_Desc], CHARINDEX(': ', [ICD10_L4_Desc]) + 1, LEN([ICD10_L4_Desc])))) as [ICD10_L4_Desc]

, LTRIM(RTRIM(SUBSTRING([ICD10_L1_Desc], CHARINDEX(': ', [ICD10_L1_Desc]) + 1, LEN([ICD10_L1_Desc])))) as [ICD10_L1_Desc]

, LTRIM(RTRIM(SUBSTRING([ICD10_Chapter_Desc], CHARINDEX(': ', [ICD10_Chapter_Desc]) + 1, LEN([ICD10_Chapter_Desc])))) as [ICD10_Chapter_Desc]

        INTO #TempDiagCodes

        FROM [PATLondon].[Ref_ClinCode_ICD10]


DECLARE @fin_yearStart date

SET @fin_yearStart = '2019-01-01'


        IF OBJECT_ID('tempdb..#DiagAll') IS NOT NULL
        DROP TABLE #DiagAll

        SELECT
        [Person_ID]
, [OrgIDProv]
, ROW_NUMBER() OVER (
        PARTITION BY  [UniqServReqID]
        ORDER BY   UniqMonthID DEsc,  UniqSubmissionID desc  ) as RowOrder
, [UniqSubmissionID]
, [UniqMonthID]
, [RecordNumber]
, [RowNumber]
, [ServiceRequestId]
, [DiagSchemeInUse]
, [PrimDiag] as [ICD10_4 Diagnosis Code]
, 'Primary' as [Diagnosis Level]
, b.ICD10_L4_Desc
, b.ICD10_L1_Desc
, b.ICD10_Chapter_Desc
, [CodedDiagTimestamp] as [Diagnosis Time Stamp]
, [UniqServReqID]

, [NHSEUniqSubmissionID]

, [Der_Person_ID]

        INTO #DiagAll

        FROM [MESH_MHSDS].[MHS604PrimDiag]a
        INNER JOIN  #TempDiagCodes b on b.[ICD10_L4_Code] = a.[PrimDiag]
        WHERE  CONVERT(date, [CodedDiagTimestamp])>= @fin_yearStart


        UNION ALL

        SELECT
        [Person_ID]
, [OrgIDProv]
, ROW_NUMBER() OVER (
        PARTITION BY  [UniqServReqID]
        ORDER BY   UniqMonthID DEsc,  UniqSubmissionID desc  ) as RowOrder
, [UniqSubmissionID]
, [UniqMonthID]
, [RecordNumber]
, [RowNumber]
, [ServiceRequestId]
, [DiagSchemeInUse]
, [SecDiag] as [ICD10_4 Diagnosis Code]
, 'Secondary' as [Diagnosis Level]
, b.ICD10_L4_Desc
, b.ICD10_L1_Desc
, b.ICD10_Chapter_Desc
, [CodedDiagTimestamp] as [Diagnosis Time Stamp]
, [UniqServReqID]

, [NHSEUniqSubmissionID]

, [Der_Person_ID]

        FROM [MESH_MHSDS].[MHS605SecDiag]a
        INNER JOIN  #TempDiagCodes b on b.[ICD10_L4_Code] = a.[SecDiag]
        WHERE  CONVERT(date, [CodedDiagTimestamp])>= @fin_yearStart
--WHERE UniqServReqID = 'RXG10064189'

        UNION ALL

        SELECT
         [Person_ID]
, [OrgIDProv]
, ROW_NUMBER() OVER (
        PARTITION BY  [UniqServReqID]
        ORDER BY   UniqMonthID DEsc,  UniqSubmissionID desc  ) as RowOrder
, [UniqSubmissionID]
, [UniqMonthID]
, [RecordNumber]
, [RowNumber]
, [ServiceRequestId]
, [DiagSchemeInUse]
, [ProvDiag] as [ICD10_4 Diagnosis Code]
, 'Provisional' as [Diagnosis Level]
, b.ICD10_L4_Desc
, b.ICD10_L1_Desc
, b.ICD10_Chapter_Desc
, [CodedProvDiagTimestamp] as [Diagnosis Time Stamp]
, [UniqServReqID]

, [NHSEUniqSubmissionID]

, [Der_Person_ID]

        FROM [MESH_MHSDS].[MHS603ProvDiag]a
        INNER JOIN  #TempDiagCodes b on b.[ICD10_L4_Code] = a.[ProvDiag]
        WHERE  CONVERT(date, [CodedProvDiagTimestamp])>= @fin_yearStart


  DELETE FROM #DiagAll WHERE RowOrder <> 1



        insert INTO [PATLondon].[MH_Referrals_Diagnoses]
        SELECT
        *

        FROM #DiagAll a
        WHERE NOT EXISTS (
                            SELECT
                            [UniqServReqID],
                            [Diagnosis Level]
                            FROM [PATLondon].[MH_Referrals_Diagnoses] x
                            WHERE x.UniqServReqID = a.UniqServReqID
                            and x.[Diagnosis Level] = a.[Diagnosis Level]
                        )

        UPDATE f
        SET f.Der_Person_ID = g.Der_Person_ID,
        f.[Diagnosis Time Stamp] = g.[Diagnosis Time Stamp],
        f.[ICD10_4 Diagnosis Code] = g.[ICD10_4 Diagnosis Code],
        f.ICD10_Chapter_Desc = g.ICD10_Chapter_Desc,
        f.ICD10_L1_Desc = g.ICD10_L1_Desc,
        f.ICD10_L4_Desc = g.ICD10_L4_Desc,
        f.NHSEUniqSubmissionID = g.NHSEUniqSubmissionID,
        f.OrgIDProv = g.OrgIDProv,
        f.Person_ID = g.Person_ID,
        f.RecordNumber = g.RecordNumber,
        f.ServiceRequestId = g.ServiceRequestId,
        f.UniqSubmissionID = g.UniqSubmissionID

        FROM [PATLondon].[MH_Referrals_Diagnoses] f
        INNER JOIN #DiagAll g on g.UniqServReqID = f.UniqServReqID
                                    and g.UniqMonthID > f.UniqMonthID
                                    and g.[Diagnosis Level] = f.[Diagnosis Level]

        DROP TABLE #DiagAll


-------------------------------------------------------------------------------------------------------------------------------


DECLARE @EndDate Date, @LastDate Date, @DateSerial int
SET @LastDate = (SELECT MAX(StartDateHospProvSpell) FROM [MESH_MHSDS].[MHS501HospProvSpell_2] )
SET @EndDate = EOMONTH(@LastDate)
SET @DateSerial = (SELECT UniqMonthID FROM [PATLondon].[Ref_Other_Dates] WHERE MonthEndDate = @EndDate)


IF OBJECT_ID('tempdb..#temp11') IS NOT NULL
DROP TABLE #temp11

SELECT
DISTINCT


ROW_NUMBER() OVER (
PARTITION BY  a.UNIQHOSPPROVSPELLID
ORDER BY  a.UniqMonthID DEsc, a.UniqSubmissionID desc, gp.rowNumber desc ) as RowOrder,
gpd.GP_Name as GP_Practice_Name,
gpd.Local_Authority as [Local Authority Name],
gpd.PCDS_NoGaps as ODS_GPPrac_PostCode,
gpd.GP_Practice_Code as ODS_GPPrac_OrgCode,
COALESCE(LEFT( gpd.Local_Authority, CHARINDEX('ICB ', gpd.Local_Authority + 'ICB ') - 1), [2019_CCG_Name]) as [Patient GP Practice CCG],
gpd.[GP_Region_Name] as [Patient GP Practice Region],
gpd.[2019_CCG_Name] as [2019 Patient GP Practice CCG],

a.RecordNumber,
a.UniqServReqID,
a.OrgIDProv,
b.Der_Person_ID,
a.Person_ID,
b.OrgIDCCGRes as [patients postcode ccg],
 COALESCE(o3.Organisation_Name, 'Missing/Invalid') AS [CCG name by PatPostcode],
 o3.STP_Name as [STP name by PatPostcode],
 o3.Region_Name [Region name by PatPostcode],
 b.LSOA2011,
 LAD17NM as LAName,
 h.Trust as [Res MH Trust by PatPostcode],
 h.ICS as [ICB of Res MH Trust by PatPostcode],
 h.Borough as [ Borough Res MH Trust by PatPostcode],
COALESCE(b.Der_Pseudo_NHS_Number, '') as Der_Pseudo_NHS_Number,
EthnicCategory,
Gender,
prov.[ICD10_4 Diagnosis Code] as [Provisional Diag Code],
prov.[ICD10_L4_Desc] as [Prov. Diag Desc],
prov.[ICD10_Chapter_Desc] as [Prov. Diag Chapter],
prim.[ICD10_4 Diagnosis Code] as [Primary Diag Code],
prim.[ICD10_L4_Desc] as [Prim. Diag Desc],
prim.[ICD10_Chapter_Desc] as [Prim. Diag Chapter],
sec.[ICD10_4 Diagnosis Code] as [Secondary Diag Code],
sec.[ICD10_L4_Desc] as [Sec. Diag Desc],
sec.[ICD10_Chapter_Desc] as [Sec. Diag Chapter],
HospProvSpellID,
b.UniqMonthID,
a.UniqSubmissionID,
A.UNIQHOSPPROVSPELLID,
StartDateHospProvSpell,
StartTimeHospProvSpell,
A.SourceAdmMHHospProvSpell AS SourceAdmCodeHospProvSpell,
a.MethAdmMHHospProvSpell as AdmMethCodeHospProvSpell,
EstimatedDischDateHospProvSpell,
PlannedDischDateHospProvSpell,
DischDateHospProvSpell,
DischTimeHospProvSpell,
PlannedDestDisch,
destOfdischhospProvSpell,
CASE WHEN A.DischDateHospProvSpell IS NOT NULL THEN 'Closed'
    WHEN A.DischDateHospProvSpell IS NULL AND a.UniqMonthID >= @DateSerial THEN 'Open'
    WHEN A.DischDateHospProvSpell IS NULL AND a.UniqMonthID < @DateSerial THEN 'Inactive'
    END AS AdmissionCat,
CASE
WHEN DischDateHospProvSpell is not NULL
THEN DATEDIFF(DAY, startDateHospProvSpell, DischDateHospProvSpell)+1
ELSE NULL
END AS [HOSP_LOS],
CASE
WHEN DischDateHospProvSpell is  NULL
THEN DATEDIFF(day, A.StartDateHospProvSpell, COALESCE(A.DischDateHospProvSpell, @EndDate) )+1
ELSE NULL
END as [HOSP_LOS at Last UPDATE for Incomplete Spells],
a.SourceAdmMHHospProvSpell,
a.MethAdmMHHospProvSpell
--,
--MAX(a.Effective_From) as LasEffectiveDate
INTO #temp11
 FROM [MESH_MHSDS].[MHS501HospProvSpell]  a
 INNER JOIN [MESH_MHSDS].[MHSDS_SubmissionFlags]  s ON s.NHSEUniqSubmissionID = a.NHSEUniqSubmissionID AND s.Der_IsLatest = 'Y'
 LEFT JOIN [MESH_MHSDS].[MHS001MPI]b on b.Person_ID = a.Person_ID
--and b.UniqSubmissionID = a.UniqSubmissionID
                                            and b.UniqMonthID = a.UniqMonthID
                                            and b.RecordNumber = a.RecordNumber
LEFT JOIN [MESH_MHSDS].[MHS002GP_ALL]gp on gp.RecordNumber = a.RecordNumber
LEFT JOIN [PATLondon].[Ref_GP_Data] gpd on gpd.GP_Practice_Code = gp.GMPReg
LEFT JOIN [PATLondon].[MH_Referrals_Diagnoses]prov on prov.UniqServReqID = a.UniqServReqID and prov.[Diagnosis Level] = 'Provisional'
LEFT JOIN [PATLondon].[MH_Referrals_Diagnoses]prim on prim.UniqServReqID = a.UniqServReqID and prim.[Diagnosis Level] = 'Primary'
LEFT JOIN [PATLondon].[MH_Referrals_Diagnoses]sec  on sec.UniqServReqID = a.UniqServReqID and sec.[Diagnosis Level] = 'Secondary'
LEFT JOIN [Reporting_UKHD_ODS].[Commissioner_Hierarchies] o3 ON COALESCE(b.OrgIDCCGRes, b.OrgIDSubICBLocResidence) = o3.Organisation_Code
LEFT JOIN [PATLondon].[Ref_LSOAMap2] as la on la.LSOA11CD=b.LSOA2011
LEFT JOIN [PATLondon].[Ref_Borough_Trust_Mapping]h on h.Borough =la.[LAD17NM]



 DELETE FROM #temp11 WHERE RowOrder <> 1


        IF OBJECT_ID('tempdb..#WardAtAdmission') IS NOT NULL
        DROP TABLE #WardAtAdmission

        SELECT
        DISTINCT
        a.recordnumber,
        a.Der_Person_ID,
        a.Person_ID,

        NULL as WardStayOrder,
        ROW_NUMBER() OVER (
        PARTITION BY  a.UNIQHOSPPROVSPELLID, a.wardstayID
        ORDER BY a.UniqMonthID, a.UniqSubmissionID    asc
        ) as MonthRowOrder,
        a.UniqMonthID,
        NULL as [Last Submission],
        a.UniqHospProvSpellID,
        a.UniqWardStayID,
        a.UniqSubmissionID,
        CASE
        WHEN  MHAdmittedPatientClass IN ('10', '200', '11', '201', '12', '202') THEN 'Adult Acute (CCG commissioned)'
        WHEN  MHAdmittedPatientClass IN ('13', '203', '14', '204', '15', '16', '17', '18', '19', '20', '21', '22', '205',
                                    '206', '207', '208', '209', '210', '211', '212', '213') THEN 'Adult Specialist'
        WHEN  MHAdmittedPatientClass IN ('23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '300',
                                '301', '302', '303', '304', '305', '306', '307', '307', '308', '309', '310', '311') THEN 'CYP'
        ELSE 'Missing/Invalid'
        END as AdmissionTypeNHSE,
        CASE
        WHEN MHAdmittedPatientClass in ('10') THEN 'Adult acute'
        WHEN MHAdmittedPatientClass in ('11') THEN 'Older adult acute'
        WHEN MHAdmittedPatientClass in ('12', '13', '14', '15', '17', '19', '20', '21', '22', '35', '36', '37', '38', '39', '40') THEN 'Adult specialist'
        WHEN MHAdmittedPatientClass in ('23', '24') THEN 'CYP acute'
        WHEN MHAdmittedPatientClass in ('25', '26', '27', '28', '29', '30', '31', '32', '33', '34') THEN 'CYP specialist'
        ELSE 'Unknown'
        END as AdmissionType_MHUEC,

        HospitalBedTypeName,
        b.UniqServReqID,
        ISNULL(SpecialisedMHServiceCode, 'Non Specialised Service') AS SpecialisedMHServiceCode, -- Identify if and what specialised activity the spell relates to
        a.OrgIDProv,
        a.SiteIDOfWard as SiteIDOfTreat,
        a.WardType,
        a.WardIntendedSex as WardSexTypeCode,
        a.WardCode,
         a.MHAdmittedPatientClass  as HospitalBedTypeMH,

        a.WardLocDistanceHome,
        CAST(NULL as date) as Start_DateWardStay,
        CAST(NULL as time) as Start_TimeWardStay,
        CAST(NULL as date) as End_DateWardStay,
        CAST(NULL as time) as End_TimeWardStay,
        BedDaysWSEndRP,
        Der_Age_at_StartWardStay,
        a.[EFFECTIVE_FROM],
        CASE
        WHEN bb.[Main_Code_Text] is not NULL
        THEN COALESCE(bb.[Main_Description_60_Chars], 'Not known (not recorded)')
        ELSE NULL
        END as [Main Reason for AWOL],
        [StartDateMHAbsWOLeave],
        [StartTimeMHAbsWOLeave],
        [EndDateMHAbsWOLeave],
        [EndTimeMHAbsWOLeave],
        [AWOLDaysEndRP] as [AWOL Days],
        [PoliceAssistArrDate],
        [PoliceAssistArrTime],
        [PoliceAssistReqDate],
        [PoliceAssistReqTime],
        [PoliceRestraintForceUsedInd],
        [StartDateMHLeaveAbs],
        [StartTimeMHLeaveAbs],
        [EndDateMHLeaveAbs],
        [EndTimeMHLeaveAbs],
        [LOADaysRP],
        CASE
        WHEN bbb.[Main_Code_Text] is not NULL
        THEN COALESCE(bbb.[Main_Description_60_Chars], 'Not known (not recorded)')
        ELSE NULL
        END as [MHLeaveAbsEndReason]
        INTO #WardAtAdmission

        FROM [MESH_MHSDS].[MHS502WardStay_All] a
         INNER JOIN [MESH_MHSDS].[MHSDS_SubmissionFlags] s ON s.NHSEUniqSubmissionID = a.NHSEUniqSubmissionID AND s.Der_IsLatest = 'Y'
        INNER JOIN #temp11 b on b.UniqHospProvSpellID = a.UniqHospProvSpellID
                                and b.RecordNumber = a.RecordNumber
        LEFT JOIN [MESH_MHSDS].[MHS516PoliceAssistanceRequest]par on par.[UniqWardStayID] = a.[UniqWardStayID]
                                                            and par.[UniqHospProvSpellID] = a.[UniqHospProvSpellID]
                                                            and par.[UniqServReqID] = a.[UniqServReqID]

        LEFT JOIN [MESH_MHSDS].[MHS511AbsenceWithoutLeave]awl on awl.[UniqWardStayID] = a.[UniqWardStayID]
                                                               and awl.[UniqHospProvSpellID] = a.[UniqHospProvSpellID]
                                                               and awl.[UniqServReqID] = a.[UniqServReqID]
        LEFT JOIN [UKHD_Data_Dictionary].[Mental_Health_Leave_Of_Absence_End_Reason_SCD_1]bb on bb.[Main_Code_Text] = awl.[MHAbsWOLeaveEndReason]
        LEFT JOIN [MESH_MHSDS].[MHS510LeaveOfAbsence] loa on loa.[UniqWardStayID] = a.[UniqWardStayID]
                                                           and loa.[UniqHospProvSpellID] = a.[UniqHospProvSpellID]
                                                           and loa.[UniqServReqID] = a.[UniqServReqID]
        LEFT JOIN [UKHD_Data_Dictionary].[Mental_Health_Leave_Of_Absence_End_Reason_SCD]bbb on bbb.[Main_Code_Text] = loa.[MHLeaveAbsEndReason]


        UPDATE f
        SET f.[Last Submission] = 1

        FROM #WardAtAdmission f
        INNER JOIN
        (
        SELECT
         UniqWardStayID,
        UniqHospProvSpellID,
        MAX(MonthRowORder) as LastSumbit

        FROM #WardAtAdmission
        GROUP BY
     UniqWardStayID, UniqHospProvSpellID
        )g on g.UniqWardStayID = f.UniqWardStayID
            and g.UniqHospProvSpellID = f.UniqHospProvSpellID
            and g.LastSumbit = f.MonthRowOrder

    DELETE FROM #WardAtAdmission  WHERE [Last Submission] is NULL

    UPDATE DT

    SET dt.Start_DateWardStay = b.StartDateWardStay,
        dt.Start_TimeWardStay = b.StartTimeWardStay,
        dt.End_DateWardStay = b.EndDateWardStay,
        dt.End_TimeWardStay = b.EndTimeWardStay,
        dt.BedDaysWSEndRP = b.BedDaysWSEndRP,
        dt.Der_Age_at_StartWardStay = b.Der_Age_at_StartWardStay

    FROM #WardAtAdmission dt
    INNER JOIN [MESH_MHSDS].[MHS502WardStay_2] b on b.Der_Person_ID = dt.Der_Person_ID
                                                    and b.UniqHospProvSpellID = dt.UniqHospProvSpellID
                                                    and b.UniqMonthID = dt.UniqMonthID
                                                    and b.UniqSubmissionID = dt.UniqSubmissionID
                                                    and b.UniqServReqID = dt.UniqServReqID
                                                    and b.UniqWardStayID = dt.UniqWardStayID

     UPDATE r
        SET r.WardStayOrder = g.WardStayOrder
     FROM #WardAtAdmission r
        INNER JOIN
        (
        SELECT
        UniqHospProvSpellID,
        UniqWardStayID,
            ROW_NUMBER() OVER (
        PARTITION BY  UNIQHOSPPROVSPELLID
        ORDER BY Start_DateWardStay, Start_TimeWardStay  asc
        ) as WardStayOrder
        FROM #WardAtAdmission

        )g on g.UniqHospProvSpellID = r.UniqHospProvSpellID
            and g.UniqWardStayID = r.UniqWardStayID

--SELECT top 5000 * FROM #WardAtAdmission

         DELETE a FROM [PATLondon].[MH_Ward_Stays]        a WHERE EXISTS
(SELECT b.[UniqHospProvSpellID] FROM #WardAtAdmission b WHERE b.[UniqHospProvSpellID] = a.[UniqHospProvSpellID] )

IF EXISTS(SELECT * FROM sys.indexes WHERE object_id = object_id('[PATLondon].[MH_Ward_Stays]') AND NAME ='ix_WardStay_Num')
DROP INDEX ix_WardStay_Num ON [PATLondon].[MH_Ward_Stays]

insert INTO [PATLondon].[MH_Ward_Stays]

SELECT
*

FROM #WardAtAdmission

   CREATE INDEX ix_WardStay_Num ON [PATLondon].[MH_Ward_Stays] ( UniqHospProvSpellID, UniqWardStayID,  Der_Person_ID, WardStayOrder)

DELETE a FROM [PATLondon].[MH_Spells]        a WHERE EXISTS
(SELECT b.[UniqHospProvSpellID] FROM #temp11 b WHERE b.[UniqHospProvSpellID] = a.[UniqHospProvSpellID] )

IF EXISTS(SELECT * FROM sys.indexes WHERE object_id = object_id('[PATLondon].[MH_Spells]') AND NAME ='ix_Spell_Num')
DROP INDEX ix_Spell_Num ON [PATLondon].[MH_Spells]

insert INTO [PATLondon].[MH_Spells]

SELECT
a.[UniqMonthID],
a.[UniqHospProvSpellID],
a.UniqSubmissionID,
a.[Person_ID],
a.[Der_Person_ID],
a.[Der_Pseudo_NHS_Number],
ODS_GPPrac_OrgCode,
[GP_Practice_Name],
[ODS_GPPrac_PostCode],
[Local Authority Name],
[2019 Patient GP Practice CCG] as [2019 GP CCG NAME],
[Patient GP Practice Region],

gen.Main_Description as [Gender],
ec.[Main_Description] as [Ethnic Category],
ec.[Category]as [Broad Ethnic Category],
CASE
WHEN (ec.[Main_Description] = '' OR ec.[Main_Description] = 'Not stated' OR ec.[Main_Description] = 'Not known' OR  ec.[Main_Description] is NULL) THEN 'Not Known / Not Stated / Incomplete'
WHEN ec.Category = 'Asian or Asian British' THEN 'Asian'
WHEN ec.Category = 'Black or Black British' THEN 'Black'
WHEN ec.[Main_Description] in ('mixed', 'Any other ethnic group', 'White & Black Caribbean', 'Any other mixed background', 'Chinese') THEN 'Mixed/ Other'
ELSE ec.[Category]
END as [Derived Broad Ethnic Category],
[patients postcode ccg],
[CCG name by PatPostcode],
[STP name by PatPostcode],
[Region name by PatPostcode],
a.LSOA2011,

LAName as [Pat Postcode Lan Name],
[Res MH Trust by PatPostcode],
[ICB of Res MH Trust by PatPostcode],
[ Borough Res MH Trust by PatPostcode],
a.[OrgIDProv],
o1.Organisation_Name as [Provider_Name],
pt.[Postcode] as [Provider_PostCode],
tm.ICS as [Provider ICS Full Name],
tm.ICB as [Provider ICS Abbrev],
o1.Region_Name as [Provider Region Name],
d.[Organisation_Name] as [Admission Site Name],

CASE
WHEN o1.ODS_Organisation_Type = 'NHS TRUST' THEN 'NHS TRUST'
WHEN o1.ODS_Organisation_Type = 'CARE TRUST' THEN 'NHS TRUST'
WHEN o1.ODS_Organisation_Type IN ('INDEPENDENT SECTOR HEALTHCARE PROVIDER', 'INDEPENDENT SECTOR H/C PROVIDER SITE', 'NON-NHS ORGANISATION') THEN 'NON-NHS TRUST'
ELSE 'Missing/Invalid'
END as Provider_Type,
COALESCE(o2.Region_Code, 'Missing/Invalid') AS Region_Code, --- regions taken FROM CCG rather than provider
COALESCE(o2.Region_Name, 'Missing/Invalid') AS Region_Name,
COALESCE(cc.New_Code, s.OrgIDCCGRes, 'Missing/Invalid') AS CCGCode,
COALESCE(o2.Organisation_Name, 'Missing/Invalid') AS [CCG name],
COALESCE(o2.STP_Code, 'Missing/Invalid') AS STPCode,
COALESCE(o2.STP_Name, 'Missing/Invalid') AS [STP name],
CASE WHEN s.AgeRepPeriodStart < 18 THEN '0-17'
WHEN s.AgeRepPeriodStart BETWEEN 18 AND 24 THEN '18-24'
WHEN s.AgeRepPeriodStart BETWEEN 25 AND 34 THEN '25-34'
WHEN s.AgeRepPeriodStart BETWEEN 35 AND 44 THEN '35-44'
WHEN s.AgeRepPeriodStart BETWEEN 45 AND 54 THEN '45-54'
WHEN s.AgeRepPeriodStart BETWEEN 55 AND 64 THEN '55-64'
WHEN s.AgeRepPeriodStart > 64 THEN '65+'
ELSE 'Missing/Invalid' END AS AgeBand, -- Create age bands
ref.[AgeServReferRecDate],
CASE
WHEN ref.AgeServReferRecDate BETWEEN 0 AND 17 THEN '0-17'
WHEN ref.AgeServReferRecDate >=18 THEN '18+'
END as [AgeCat],
a.[UniqServReqID],
ref.ReferralRequestReceivedDate,
CONVERT(Date, DATEADD(month, DATEDIFF(month, 0, CONVERT(Date, ref.ReferralRequestReceivedDate)), 0)) as [RefMonth],
CAST(NULL as varchar(400)) as [Referring Organisation],
CAST(NULL as varchar(400)) as [Referring Org Type],
CAST(NULL as varchar(400)) as [Referring Care Professional Staff Group],
CAST(NULL as varchar(400)) as [Referral Source],
CAST(NULL as varchar(400)) as [Primary Reason for Referral],
CAST(NULL as varchar(255)) as [Clinical Priority],
CAST(NULL as float) as [Ethnic proportion per 100000 of London Borough 2020],
CAST(NULL as float) as [Ethnic proportion per 100000 of England 2020],
a.[RecordNumber],
[Provisional Diag Code],
[Prov. Diag Desc],
[Prov. Diag Chapter],
[Primary Diag Code],
[Prim. Diag Desc],
[Prim. Diag Chapter],
[Secondary Diag Code],
[Sec. Diag Desc],
[Sec. Diag Chapter],
[StartDateHospProvSpell],
[StartTimeHospProvSpell],
DATEADD(MONTH, DATEDIFF(MONTH, 0, [StartDateHospProvSpell]), 0)  as [Adm_MonthYear],
[SourceAdmCodeHospProvSpell],
CASE WHEN [SourceAdmCodeHospProvSpell] = '19' THEN 'Usual place of residence'
WHEN [SourceAdmCodeHospProvSpell] = '29' THEN 'Temporary place of residence'
WHEN [SourceAdmCodeHospProvSpell] IN ('37', '40', '42') THEN 'Criminal setting'
WHEN [SourceAdmCodeHospProvSpell] IN ('49', '51', '52', '53') THEN 'NHS healthcare provider'
WHEN [SourceAdmCodeHospProvSpell] = '87' THEN 'Independent sector healthcare provider'
WHEN [SourceAdmCodeHospProvSpell] IN ('55', '56', '66', '88') THEN 'Other'
WHEN [SourceAdmCodeHospProvSpell] = NULL THEN 'NULL'
ELSE 'Missing/Invalid' END AS SourceOfAdmission, -- Create source of admission groups
moa.description as [Der_AdmissionMethod],
[HospitalBedTypeMH],
b.SpecialisedMHServiceCode as [Specialised Service Code for Initial Ward Admission],
  CASE
WHEN  HospitalBedTypeMH IN ('10', '200', '11', '201', '12', '202') THEN 'Adult Acute (CCG commissioned)'
WHEN  HospitalBedTypeMH IN ('13', '203', '14', '204', '15', '16', '17', '18', '19', '20', '21', '22', '205',
                            '206', '207', '208', '209', '210', '211', '212', '213') THEN 'Adult Specialist'
WHEN  HospitalBedTypeMH IN ('23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '300',
                            '301', '302', '303', '304', '305', '306', '307', '307', '308', '309', '310', '311') THEN 'CYP'
ELSE 'Missing/Invalid'
END as[BedType_Category],
ISNULL(scdb.Description, 'Missing/Invalid') AS [BedType],
CASE WHEN hs.OrgIDComm IN ('13N', '13R', '13V', '13X', '13Y', '14C', '14D', '14E', '14F', '14G', '85J', '27T', '14A', '14E', '14G', '14F', '13R', 'L5H9Q',
'N8S0C', 'Q7O8U', 'X8H3R', 'P7L6U', 'F3I2L', 'S7T0C', 'Z1U2L', 'C9Z7X', 'F9H5S', 'K5B5Y', 'S6Z6H', 'J3T7D', 'I0H0N', 'O5V1Z', 'E2S1E', 'A8R9E', 'S5L0S', 'N5T4E', 'O6H3T',
'I2T5F', 'K4Z4O', 'Z0X9Q', 'B9Q0L', 'I3Q3V', 'X4I1M', 'N9S3D', 'D8D1G', 'Z4P6N', 'D4U5V', 'P9W2J', 'L4H0W', 'B5S8O', 'G1U9X', 'X6C7V', 'C8S2X', 'R7G8O', 'H3F5A', 'I4B8X',
'X4L0A', 'B0N9F', 'N5E8H', 'M4X2K', 'A3Y0R', 'W6B3O', 'O1N4A', 'Z0B3G') THEN 'Yes'
ELSE 'No' END AS SpecCommCode,
[EstimatedDischDateHospProvSpell],
[PlannedDischDateHospProvSpell],
pdd.[Description] AS [Planned Discharge Destination],
[DischDateHospProvSpell],
[DischTimeHospProvSpell],
dd.[Description] AS [Discharge Destination],
ROW_NUMBER()OVER (PARTITION BY A.Person_ID,  A.[UniqHospProvSpellID] ORDER BY REF.RecordNumber DESC) [RN],
CASE
WHEN DischDateHospProvSpell is not NULL and ([HOSP_LOS] >=1 AND [HOSP_LOS]   <8)  THEN 'Up to 1 week'
WHEN DischDateHospProvSpell is not NULL and ([HOSP_LOS] >=8 AND [HOSP_LOS]   <15)  THEN 'BTWn 1 and 2 wks'
WHEN DischDateHospProvSpell is not NULL and ([HOSP_LOS] >=15 AND [HOSP_LOS]  <31) THEN 'Btwn 2wks and 1mth'
WHEN DischDateHospProvSpell is not NULL and ([HOSP_LOS] >=31 AND [HOSP_LOS]  <91) THEN 'BTWn 1 mth and 3mths'
WHEN DischDateHospProvSpell is not NULL and ([HOSP_LOS] >=91 AND [HOSP_LOS]  <181) THEN 'BTWn 3 mths and 6 mths'
WHEN DischDateHospProvSpell is not NULL and ([HOSP_LOS] >=181 AND [HOSP_LOS] <366) THEN 'BTWn 6 mths and 1 yr'
WHEN DischDateHospProvSpell is not NULL and ([HOSP_LOS] >=366) THEN '1 yr and above'
END aS [loS Tranche],
CASE
WHEN DischDateHospProvSpell is not NULL and ([HOSP_LOS]   >=60 and [HOSP_LOS]  < 90)  THEN 'Stranded'
WHEN DischDateHospProvSpell is not NULL and ([HOSP_LOS]   >=90) THEN 'Super Stranded'
ELSE NULL
END as 'Stranded_Status',
 [HOSP_LOS],
 [HOSP_LOS at Last UPDATE for Incomplete Spells],
a.AdmissionCat AS Der_HospSpellStatus,
NULL as [Male Psychosis 18-44 Flag],
NULL as [Male Personality Disorder 18-44 Flag],
NULL as [BiPolar Flag],
CAST(NULL as varchar(255)) as UniqMHActEpisodeID,
CAST(NULL as varchar(255)) as SectionType,
CAST(NULL as varchar(255)) as [NHS LEgal Status Description],
CAST(NULL as date) as [Legal Status Start Date],
CAST(NULL as time) as [Legal Status Start Time],
CAST(NULL as date) as [Legal Status END Date],
CAST(NULL as time) as [Legal Status END Time],
CAST(NULL as varchar(255)) as [Linked S136 Prior to Adm],
CAST(NULL as varchar(255)) as [Known to MH Services Flag],
NULL as [AWOL FLag],
CAST(NULL as varchar(100)) as [AWOL Wardstay ID],
CAST(NULL as varchar(100)) as [Admission Type],
CAST(NULL as int) as [NewLOS],
CAST(NULL as varchar(5)) [KnownInLast24Months],
CAST(NULL as varchar(5)) [PreviouslyKnown]

--INTO [PATLondon].[MH_Spells]
FROM  #temp11 a
LEFT JOIN #WardAtAdmission b on b.UniqHospProvSpellID = a.UniqHospProvSpellID and b.wardstayorder = 1

LEFT JOIN [MESH_MHSDS].[MHS001MPI]s on s.Der_Person_ID = a.Der_Person_ID
        and s.UniqSubmissionID = a.UniqSubmissionID
        and s.UniqMonthID = a.UniqMonthID
        and s.RecordNumber = a.RecordNumber
LEFT JOIN [Reporting_UKHD_ODS].[Provider_Hierarchies] o1 ON a.OrgIDProv = o1.Organisation_Code
LEFT JOIN [Internal_Reference].[ComCodeChanges_1]cc ON s.OrgIDCCGRes = cc.Org_Code
-- Temporary fix before 2021 CCGs come INTO effect
LEFT JOIN [Reporting_UKHD_ODS].[Commissioner_Hierarchies] o2 ON COALESCE(cc.New_Code, s.OrgIDCCGRes) = o2.Organisation_Code
LEFT JOIN [MESH_MHSDS].[MHS101Referral_ALL]ref on ref.UniqServReqID = a.UniqServReqID
                            and ref.RecordNumber = a.RecordNumber
                            and ref.UniqMonthID = a.UniqMonthID
                            and ref.UniqSubmissionID = a.UniqSubmissionID
LEFT JOIN [MESH_MHSDS].[MHS001MPI] mp on mp.RecordNumber = a.RecordNumber
LEFT JOIN [Reporting_UKHD_ODS].[Commissioner_Hierarchies]t on t.Organisation_Code = mp.OrgIDCCGRes

LEFT JOIN [UKHD_Data_Dictionary].[Ethnic_Category_Code_SCD_1]ec with (nolock) on ec.[Main_Code_Text] = mp.NHSDEthnicity
LEFT JOIN [UKHD_Data_Dictionary].[Person_Gender_Code_SCD_1]gen  with (nolock)on gen.[Main_Code_Text] = mp.Gender

LEFT JOIN [PATLondon].[Ref_Method_of_Admission]moa on moa.code = a.AdmMethCodeHospProvSpell
LEFT JOIN [PATLondon].[Ref_Mental_Health_Admitted_Patient_Classification] scdb ON b.HospitalBedTypeMH = scdb.[Code]
--[UKHD_Data_Dictionary].[Mental_Health_Admitted_Patient_Classification_SCD_1]
LEFT JOIN [PATLondon].[Ref_Discharge_Destination]pdd on CAST(pdd.code as varchar(10)) = PlannedDestDisch
LEFT JOIN [PATLondon].[Ref_Discharge_Destination]dd on CAST(dd.code as varchar(10)) = destOfdischhospProvSpell
LEFT JOIN [MESH_MHSDS].[MHS512HospSpellComm] hs ON hs.RecordNumber = a.RecordNumber AND hs.UniqHospProvSpellID = a.UniqHospProvSpellID -- Get specialised activity information for each admission / hospital spell, for each month. May be bringing in duplicates via multiple ward stays, but 'duplicates' flag and SELECT DISTINCT in metrics will void these.
LEFT JOIN [UKHD_ODS].[NHS_Trust_Sites_Assets_And_Units_SCD_1]d on d.[Organisation_Code] = b.SiteIDOfTreat and d.[Is_Latest] = 1
LEFT JOIN [UKHD_ODS].[NHS_Trusts_SCD_1]pt on pt.[Organisation_Code] = a.OrgIDProv and pt.[Is_Latest] = 1

LEFT JOIN [PATLondon].[Ref_ICS_Trust_Mapping]tm on tm.Site = o1.Organisation_Name


 DELETE FROM  [PATLondon].[MH_Spells]    WHERE rn <>1

   CREATE INDEX ix_Spell_Num ON [PATLondon].[MH_Spells] ( UniqHospProvSpellID,  Der_Person_ID, [StartDateHospProvSpell])

 UPDATE r

        SET r.[Male Psychosis 18-44 Flag] = 1

 FROM [PATLondon].[MH_Spells] r
  WHERE

  (
 CHARINDEX('psychosis', [Prov. Diag Desc])>0 OR
 CHARINDEX('psychosis', [Prim. Diag Desc])>0 OR
 CHARINDEX('psychosis', [Sec. Diag Desc])>0
 )
 AND
 ([AgeServReferRecDate] >= 18 and [AgeServReferRecDate] <= 44)
 AND
 Gender = 'Male'

  UPDATE r

        SET r.[Male Personality Disorder 18-44 Flag] = 1

 FROM [PATLondon].[MH_Spells] r
  WHERE
 (
 CHARINDEX('personality', [Prov. Diag Desc])>0 OR
 CHARINDEX('personality', [Prim. Diag Desc])>0 OR
 CHARINDEX('personality', [Sec. Diag Desc])>0
 )
 AND
 ([AgeServReferRecDate] >= 18 and [AgeServReferRecDate] <= 44)
 AND
 Gender = 'Male'

   UPDATE r

        SET r.[BiPolar Flag] = 1

 FROM [PATLondon].[MH_Spells] r
 WHERE
 (
 CHARINDEX('bipolar', [Prov. Diag Desc])>0 OR
 CHARINDEX('bipolar', [Prim. Diag Desc])>0 OR
 CHARINDEX('bipolar', [Sec. Diag Desc])>0
 )

    UPDATE y

    SET y.[Ethnic proportion per 100000 of London Borough 2020] = CAST(CAST((1/NULLIF(ep.Value, 0)) as float)* CAST(100000 as float)  as float),
        y.[Ethnic proportion per 100000 of England 2020] = CAST(CAST((1/NULLIF(ee.Value, 0)) as float)* CAST(100000 as float) as float)

    FROM [PATLondon].[MH_Spells]y
    LEFT JOIN [PATLondon].[Ref_Ethnicity_2020_Census_Population_by_London_Borough]ep on ep.[Broad Ethnic Category] = y.[Derived Broad Ethnic Category]
                                                                                                        and ep.Borough = y.[Local Authority Name]
    LEFT JOIN [PATLondon].[Ref_Ethnicity_2020_Census_Population_by_England_Region]ee on ee.Ethnicity = y.[Derived Broad Ethnic Category]
                                                                                                    and  ee.Area = y.[Patient GP Practice Region]


UPDATE ws
        SET ws.[AWOL Flag] = 1,
        ws.[AWOL WardStay ID] = g.UniqWardStayID

FROM [PATLondon].[MH_Spells]ws
INNER JOIN [PATLondon].[MH_Ward_Stays] g on g.UniqHospProvSpellID = ws.UniqHospProvSpellID
                                       and g.StartDateMHAbsWOLeave is not NULL

IF OBJECT_ID('tempdb..#IPSpells') IS NOT NULL
DROP TABLE #IPSpells

SELECT
DISTINCT
Der_Pseudo_NHS_Number,
[DischDateHospProvSpell]

INTO #IPSpells

FROM [PATLondon].[MH_Spells]
WHERE Der_Pseudo_NHS_Number is not NULL

       UPDATE x

     SET x.[Known to MH Services Flag] = CASE

                                        WHEN
                                           ( DATEDIFF(day, ccc.ReferralRequestReceivedDate, CONVERT(date, x.StartDateHospProvSpell) )>= 0
                                         and DATEDIFF(MONTH, ccc.ReferralRequestReceivedDate, CONVERT(date, x.StartDateHospProvSpell)) <=6

                                         )
                                           THEN 1
                                        WHEN
                                            (
                                                (DATEDIFF(MONTH, ccc.ReferralRequestReceivedDate, CONVERT(date, StartDateHospProvSpell))>6 )
                                            and (ccc.ServDischDate is NULL or (ccc.ServDischDate > StartDateHospProvSpell))
                                            and (ReferRejectionDate is NULL or (ReferRejectionDate > StartDateHospProvSpell))
                                            )

                                          THEN 1
                                        ELSE NULL
                                        END

     FROM [PATLondon].[MH_Spells] x
    LEFT JOIN #tempRef ccc on ccc.UniqServReqID = x.UniqServReqID
    and ccc.ReferralRequestReceivedDate <= x.StartDateHospProvSpell

    UPDATE f

         SET f.[Known to MH Services Flag] = CASE
                                              WHEN ccc.[Referral Source] = 'Acute Secondary Care:�Emergency Care Department'
                                                    THEN NULL
                                                ELSE f.[Known to MH Services Flag]
                                            END

     FROM [PATLondon].[MH_Spells] f
    INNER JOIN #tempRef ccc on ccc.UniqServReqID = f.UniqServReqID
    and CONVERT(date, ccc.ReferralRequestReceivedDate) = f.StartDateHospProvSpell

    UPDATE f

         SET f.[Known to MH Services Flag] = 1

     FROM [PATLondon].[MH_Spells] f
    INNER JOIN #tempRef ccc on ccc.UniqServReqID = f.UniqServReqID

    WHERE
    (
    DATEDIFF(day, ccc.ReferralRequestReceivedDate, CONVERT(date, f.StartDateHospProvSpell)) > 0
    and
    DATEDIFF(month, ccc.ReferralRequestReceivedDate, CONVERT(date, f.StartDateHospProvSpell)) <=6
    )
    and ccc.[Referral Source] = 'Acute Secondary Care:�Emergency Care Department'

---------------------------------------------------------------------------------------------------------------------------------------------
--New Known and Previously Known columns added 07/05/2025
---------------------------------------------------------------------------------------------------------------------------------------------

       UPDATE x

     SET x.[KnownInLast24Months] = CASE

                                        WHEN
                                           ( DATEDIFF(day, ccc.ReferralRequestReceivedDate, CONVERT(date, x.StartDateHospProvSpell) )>= 0
                                           and DATEDIFF(MONTH, ccc.ReferralRequestReceivedDate, CONVERT(date, x.StartDateHospProvSpell)) <=24
                                           )
                                           THEN 1
                                        WHEN
                                            (
                                                (DATEDIFF(MONTH, ccc.ReferralRequestReceivedDate, CONVERT(date, StartDateHospProvSpell))>24 )
                                            and (ServDischDate is NULL or (ServDischDate > StartDateHospProvSpell))
                                            and (ReferRejectionDate is NULL or (ReferRejectionDate > StartDateHospProvSpell))
                                            )

                                          THEN 1
                                        ELSE NULL
                                        END

     FROM [PATLondon].[MH_Spells] x
    LEFT JOIN #tempRef ccc on ccc.UniqServReqID = x.UniqServReqID
    and ccc.ReferralRequestReceivedDate <= x.StartDateHospProvSpell

    UPDATE f

         SET f.[KnownInLast24Months] = CASE
                                              WHEN ccc.[Referral Source] = 'Acute Secondary Care:�Emergency Care Department'
                                                    THEN NULL
                                                ELSE f.[KnownInLast24Months]
                                            END

     FROM [PATLondon].[MH_Spells] f
    INNER JOIN #tempRef ccc on ccc.UniqServReqID = f.UniqServReqID
    and CONVERT(date, ccc.ReferralRequestReceivedDate) = f.StartDateHospProvSpell

    UPDATE f

         SET f.[KnownInLast24Months] = 1

     FROM [PATLondon].[MH_Spells] f
    INNER JOIN #tempRef ccc on ccc.UniqServReqID = f.UniqServReqID

    WHERE
    (
    DATEDIFF(day, ccc.ReferralRequestReceivedDate, CONVERT(date, f.StartDateHospProvSpell)) > 0
    and
    DATEDIFF(month, ccc.ReferralRequestReceivedDate, CONVERT(date, f.StartDateHospProvSpell)) <=24
    )
    and ccc.[Referral Source] = 'Acute Secondary Care:�Emergency Care Department'

--Previously Known

    UPDATE x

     SET x.PreviouslyKnown = CASE

                                        WHEN
                                           ( DATEDIFF(day, ccc.ReferralRequestReceivedDate, CONVERT(date, x.StartDateHospProvSpell) )>= 0
                                           and DATEDIFF(MONTH, ccc.ReferralRequestReceivedDate, CONVERT(date, x.StartDateHospProvSpell)) >24
                                           )
                                           THEN 1
                                        WHEN
                                            (
                                                (DATEDIFF(MONTH, ccc.ReferralRequestReceivedDate, CONVERT(date, StartDateHospProvSpell))> 24 )
                                            and (ServDischDate is NULL or (ServDischDate > StartDateHospProvSpell))
                                            and (ReferRejectionDate is NULL or (ReferRejectionDate > StartDateHospProvSpell))
                                            )

                                          THEN 1
                                        ELSE NULL
                                        END

     FROM [PATLondon].[MH_Spells] x
    LEFT JOIN #tempRef ccc on ccc.UniqServReqID = x.UniqServReqID
    and ccc.ReferralRequestReceivedDate <= x.StartDateHospProvSpell

    UPDATE f

         SET f.PreviouslyKnown = CASE
                                              WHEN ccc.[Referral Source] = 'Acute Secondary Care:�Emergency Care Department'
                                                    THEN NULL
                                                ELSE f.PreviouslyKnown
                                            END

     FROM [PATLondon].[MH_Spells] f
    INNER JOIN #tempRef ccc on ccc.UniqServReqID = f.UniqServReqID
    and CONVERT(date, ccc.ReferralRequestReceivedDate) = f.StartDateHospProvSpell

    UPDATE f

         SET f.PreviouslyKnown = 1

     FROM [PATLondon].[MH_Spells] f
    INNER JOIN #tempRef ccc on ccc.UniqServReqID = f.UniqServReqID

    WHERE
    (
    DATEDIFF(day, ccc.ReferralRequestReceivedDate, CONVERT(date, f.StartDateHospProvSpell)) > 0
    and
    DATEDIFF(month, ccc.ReferralRequestReceivedDate, CONVERT(date, f.StartDateHospProvSpell)) >24
    )
    and ccc.[Referral Source] = 'Acute Secondary Care:�Emergency Care Department'


UPDATE d

SET d.[Referring Organisation] = ccc.[Referring Organisation],
    d.[Referring Org Type] = ccc.[Referring Org Type],
    d.[Referring Care Professional Staff Group] = ccc.[Referring Care Professional Staff Group],
    d.[Referral Source] = ccc.[Referral Source],
    d.[Primary Reason for Referral] = ccc.[Primary Reason for Referral],
    d.[Clinical Priority] = ccc.[Clinical Priority]

 FROM [PATLondon].[MH_Spells] d
 INNER JOIN #tempRef ccc on ccc.UniqServReqID = d.UniqServReqID

---------------------------------------------------------------------------------------------------------------------------------------------

    DROP TABLE #tempRef

    UPDATE f
    SET f.NewLOS = CASE
                        WHEN DischDateHospProvSpell is not NULL
                        THEN DATEDIFF(DAY, startDateHospProvSpell, DischDateHospProvSpell)
                        ELSE NULL
                        END

    FROM [PATLondon].[MH_Spells] f



---------------------------------------------------------------------------------------------------------------------------------------------
--PATCH FOR WARD NAME AT ADMISSION - MARCH 2026
---------------------------------------------------------------------------------------------------------------------------------------------


IF OBJECT_ID('Tempdb..#CMH1') IS NOT NULL 
dROP TABLE #CMH1

SELECT distinct
[OrgIDProv]
,[SiteIDOfWard]
   
,[UniqWardCode]
   
,[WardCode]
into #CMH1
FROM [MESH_MHSDS].[MHS903WardDetails]
where [SiteIDOfWard] is not null

--select * from #CMH1 order by [SiteIDOfWard]

IF OBJECT_ID('Tempdb..#CMH2') IS NOT NULL 
dROP TABLE #CMH2
  select distinct
       [UniqHospProvSpellID]
	   ,ROW_NUMBER() OVER (
		PARTITION BY  [UniqHospProvSpellID]  
		ORDER BY   SiteIDOfWard desc ) as RowOrder 
      ,a.[OrgIDProv]
      ,b.OrgIDProv as WardOrg
      ,[SiteIDOfTreat]
      ,[WardType]
      ,[WardSexTypeCode]
      ,a.[WardCode]
      ,b.SiteIDOfWard
      ,c.Organisation_Name
      into #CMH2
  FROM [PATLondon].[MH_Ward_Stays]a
  left join #CMH1 b on b.WardCode = a.WardCode and b.OrgIDProv = a.OrgIDProv
  left join [UKHD_ODS].[NHS_Trust_Sites_Assets_And_Units_SCD_1]c on c.Organisation_Code = b.SiteIDOfWard
                                                                and c.Is_Latest = 1
  where WardStayOrder = 1

  DELETE from #CMH2 where RowOrder > 


 update x

  set [Admission Site Name] = coalesce(b.Organisation_Name, 'Not Recorded')

  from [PATLondon].[MH_Spells]  x
  left join #CMH2 b on b.UniqHospProvSpellID = x.UniqHospProvSpellID