/*
    File: mh_act_v3b_cleaned.sql
    Purpose: Git-friendly cleanup of the original SQL script.
    Notes:
      - Formatting and readability improved.
      - Original logic preserved as far as possible.
      - Existing comments, debug blocks, and legacy naming retained unless purely cosmetic.
*/

IF OBJECT_ID('Tempdb..#temp19') IS NOT NULL
        drop table #temp19
        select
        distinct
        UniqMonthID,
        [FinYear_YYYY_YY],
        convert(Date,MonthStartDate) as MonthDate
        into #temp19
        from [PATLondon].[Ref_Other_Dates]




 IF OBJECT_ID('Tempdb..#tempRef') IS NOT NULL
dROP TABLE #tempRef
select
distinct
UniqServReqID,
[Referring Organisation],
[Referring Org Type],
[Referring Care Professional Staff Group],
[Source of Referral - Derived] as [Referral Source],
--[Type of Service Referred to],
[Primary Reason for Referral],
[Clinical Priority]

into #tempRef

from [PATLondon].[MH_Referrals_with_Care_Contacts_London]



 IF OBJECT_ID('Tempdb..#tempMHA') IS NOT NULL
dROP TABLE #tempMHA



SELECT
m.Der_Person_ID, -- New Der_Person_ID due to NHSD changes to the Person_ID field
m.Person_ID,
mp.Der_Pseudo_NHS_Number,
gpd.GP_Name as   GP_Practice_Name,
gpd.Local_Authority as  [GP Local Authority],
gpd.GP_Practice_Code,
gpd.GP_Region_Name as [Patient GP Practice Region] ,
gpd.Lower_Super_Output_Area_Code as [GP LSOA],
ec.[Main_Description] as [Ethnic Category],
case
  when (ec.[Main_Description] = '' OR ec.[Main_Description] = 'Not stated' OR ec.[Main_Description] = 'Not known' OR  ec.[Main_Description] is null) then 'Not Known / Not Stated / Incomplete'
  when ec.Category = 'Asian or Asian British' then 'Asian'
  when ec.Category = 'Black or Black British' then 'Black'
  when ec.[Main_Description] in ('mixed','Any other ethnic group','White & Black Caribbean','Any other mixed background','Chinese') then 'Mixed/ Other'
    ELSE ec.[Category]
END as [Derived Broad Ethnic Category],
 gen.[Main_Description]  as Gender,
cast(null as varchar(400)) as [Referring Organisation],
cast(null as varchar(400)) as [Referring Org Type],
cast(null as varchar(400)) as [Referring Care Professional Staff Group],
cast(null as varchar(400)) as [Referral Source],
cast(null as varchar(400)) as [Primary Reason for Referral],
cast(null as varchar(255)) as [Clinical Priority],
cast(null as float) as [Ethnic proportion per 100000 of London Borough 2020],

m.RecordNumber,
m.UniqMonthID,
um.MonthDate,
um.FinYear_YYYY_YY,
s.ReportingPeriodStartDate AS ReportingPeriodStart,
s.ReportingPeriodEndDate AS ReportingPeriodEnd,
m.OrgIDProv,
d.Organisation_Name as [Provider Name],
tm.ICB as [Provider ICB],
d.ProviderPostCode,
d.ProviderPostCodeNoGaps,
m.UniqMHActEpisodeID, -- Unique episode ID
m.NHSDLegalStatus AS SectionType, -- Section type
MHA.Description [NHS Legal Status Description],
m.StartDateMHActLegalStatusClass AS StartDate, -- Start date of episode
m.StartTimeMHActLegalStatusClass AS StartTime, -- Start time of episode
m.EndDateMHActLegalStatusClass AS EndDate, -- End date of episode
m.EndTimeMHActLegalStatusClass AS EndTime, -- End time of episode
ROW_NUMBER()OVER(PARTITION BY m.UniqMHActEpisodeID ORDER BY m.UniqMonthID desc ,gp.rowNumber DESC,m.MHS401UniqID desc) AS MostRecentFlagSpells, -- Identifies most recent month an episode is flowed
m.MHS401UniqID AS UniqID ,-- Unique ID used later to distinguish duplicate episodes in the same detention spell
null as [IP Flag],
 cast(null as int) as [IP Spell LOS],
 cast(null as varchar(30)) as UniqHospProvSpellID,
r.UniqServReqID

INTO #tempMHA

FROM [MESH_MHSDS].[MHS401MHActPeriod]m  with (nolock) -- Uses of the Act / episodes
inner join #temp19 um on um.UniqMonthID = m.UniqMonthID
LEFT JOIN [MESH_MHSDS].[MHS001MPI] mp  with (nolock)  on mp.RecordNumber = m.RecordNumber -- for WHERE clause, ensure only England data
left join [MESH_MHSDS].[MHS002GP_All]gp   with (nolock) on gp.RecordNumber = m.RecordNumber and gp.UniqMonthID = mp.UniqMonthID
left join [PATLondon].[Ref_GP_Data] gpd on gpd.GP_Practice_Code = gp.GMPReg

LEFT JOIN [MESH_MHSDS].[MHS101Referral_ALL] r   with (nolock) ON m.RecordNumber = r.RecordNumber AND m.Person_ID = r.Person_ID
INNER JOIN [MESH_MHSDS].[MHSDS_SubmissionFlags] s   with (nolock) ON m.NHSEUniqSubmissionID = s.NHSEUniqSubmissionID AND s.Der_IsLatest = 'Y' -- Ensure data is latest submission
LEFT JOIN (SELECT DISTINCT c.UniqMHActEpisodeID, c.UniqMonthID, MAX(c.StartDateCommTreatOrd) AS StartDateCommTreatOrd FROM [MESH_MHSDS].[MHS404CommTreatOrder_1] c   with (nolock) GROUP BY c.UniqMHActEpisodeID, c.UniqMonthID) c ON c.UniqMHActEpisodeID = m.UniqMHActEpisodeID AND c.UniqMonthID = m.UniqMonthID -- Join onto CTOs/Recalls/CDs to remove any MHA episodes that have a CTO/Recall/CD record flowed at the same time (as these episodes are inactive) - MAX used as a handful of cases of multiple StartDates per CTO
LEFT JOIN (SELECT DISTINCT cr.UniqMHActEpisodeID, cr.UniqMonthID, MAX(cr.StartDateCommTreatOrdRecall) AS StartDateCommTreatOrdRecall FROM [MESH_MHSDS].[MHS405CommTreatOrderRecall_1] cr   with (nolock) GROUP BY cr.UniqMHActEpisodeID, cr.UniqMonthID) cr ON cr.UniqMHActEpisodeID = m.UniqMHActEpisodeID AND cr.UniqMonthID = m.UniqMonthID
LEFT JOIN (SELECT DISTINCT cd.UniqMHActEpisodeID, cd.UniqMonthID, MAX(cd.StartDateMHCondDisch) AS StartDateMHCondDisch FROM [MESH_MHSDS].[MHS403ConditionalDischarge_1] cd   with (nolock) GROUP BY cd.UniqMHActEpisodeID, cd.UniqMonthID) cd ON cd.UniqMHActEpisodeID = m.UniqMHActEpisodeID AND cd.UniqMonthID = m.UniqMonthID

left join [Reporting_UKHD_ODS].[Commissioner_Hierarchies]t  with (nolock)on t.Organisation_Code = COALESCE(mp.OrgIDSubICBLocResidence, mp.OrgIDCCGRes )
left join [UKHD_Data_Dictionary].[Ethnic_Category_Code_SCD_1]ec with (nolock) on ec.Main_Code_Text = mp.NHSDEthnicity and ec.is_latest = 1
left join [UKHD_Data_Dictionary].[Person_Gender_Code_SCD_1]gen  with (nolock)on gen.Main_Code_Text = mp.Gender  and gen.is_latest = 1
left join [PATLondon].[Ref_MH_Act_Legal_Class_Code]mhA  with (nolock)on mha.Code = m.NHSDLegalStatus
left join
            (
            SELECT
            distinct
            [Organisation_Code]
            ,[Organisation_Name]
            ,Postcode as ProviderPostCode
            ,replace(Postcode,' ','') as ProviderPostCodeNoGaps
            FROM  [UKHD_ODS].[All_Providers_SCD_1]
            where   [Is_Latest] = 1

            )d on d.[Organisation_Code] = m.OrgIDProv
left join [PATLondon].[Ref_ICS_Trust_Mapping]tm on tm.Site = d.[Organisation_Name]

WHERE um.MonthDate >= '2024-04-01'     -- Ensure data is only from October 2018

AND c.UniqMHActEpisodeID IS NULL -- Remove MHS401 episodes if MHS404 data is being flowed for that episode
AND cr.UniqMHActEpisodeID IS NULL -- Remove MHS401 episodes if MHS405 data is being flowed for that episode
AND cd.UniqMHActEpisodeID IS NULL -- Remove MHS401 episodes if MHS403 data is being flowed for that episode


  delete f from  #tempMHA f
 where MostRecentFlagSpells <> 1


 update gp

 set    gp.Der_Pseudo_NHS_Number = mp.Der_Pseudo_NHS_Number,
        gp.GP_Practice_Name =  gpd.GP_Name,
        gp.[GP Local Authority] = gpd.Local_Authority,
        gp.GP_Practice_Code = gpd.GP_Practice_Code,
        gp.[Patient GP Practice Region] = gpd.GP_Region_Name,
        gp.[GP LSOA] = gpd.Lower_Super_Output_Area_Code,
        gp.[Ethnic Category] = ec.[Main_Description],
        gp.[Derived Broad Ethnic Category] =
case
  when (ec.[Main_Description] = '' OR ec.[Main_Description] = 'Not stated' OR ec.[Main_Description] = 'Not known' OR  ec.[Main_Description] is null) then 'Not Known / Not Stated / Incomplete'
  when ec.Category = 'Asian or Asian British' then 'Asian'
  when ec.Category = 'Black or Black British' then 'Black'
  when ec.[Main_Description] in ('mixed','Any other ethnic group','White & Black Caribbean','Any other mixed background','Chinese') then 'Mixed/ Other'
    ELSE ec.[Category]
END,
 gp.Gender = gen.[Main_Description]
 from #tempMHA gp
 LEFT JOIN [MESH_MHSDS].[MHS001MPI_1] mp  with (nolock)  on mp.RecordNumber = gp.RecordNumber -- for WHERE clause, ensure only England data
left join [MESH_MHSDS].[MHS002GP_1]g    with (nolock) on g.RecordNumber = gp.RecordNumber and gp.UniqMonthID = mp.UniqMonthID
left join [PATLondon].[Ref_GP_Data] gpd on gpd.GP_Practice_Code = g.GMPCodeReg
left join [UKHD_Data_Dictionary].[Ethnic_Category_Code_SCD_1]ec with (nolock) on ec.Main_Code_Text = mp.NHSDEthnicity and ec.is_latest = 1
left join [UKHD_Data_Dictionary].[Person_Gender_Code_SCD_1]gen  with (nolock)on gen.Main_Code_Text = mp.Gender  and gen.is_latest = 1
where gp.GP_Practice_Name is null




GO


    update f

            set  f.[Referring Organisation] = g.[Referring Organisation],
                f.[Referring Org Type] = g.[Referring Org Type],
                f.[Referring Care Professional Staff Group] = g.[Referring Care Professional Staff Group],
                f.[Referral Source] = g.[Referral Source],
                f.[Primary Reason for Referral] = g.[Primary Reason for Referral],
                f.[Clinical Priority] = g.[Clinical Priority]

  from #tempMHA f
  left join #tempRef g on g.UniqServReqID = f.UniqServReqID


DECLARE @ENDRPDATE DATE


 set @ENDRPDATE = (select max(ReferralRequestReceivedDate) from  [PATLondon].[MH_Referrals_with_Care_Contacts_London])
 Print @ENDRPDATE


        update f
            set f.[IP Flag] = 1,
                f.[IP Spell LOS] = b.HOSP_LOS,
                f.UniqHospProvSpellID = b.UniqHospProvSpellID,
                f.UniqServReqID = b.UniqServReqID
        from  #tempMHA f
        inner join [PATLondon].[MH_Spells]b on coalesce(b.Der_Person_ID,b.person_id) = coalesce(f.Der_Person_ID,f.person_id)
        and b.RecordNumber = f.RecordNumber
        AND (
            CAST(f.StartDate AS DATETIME) + CAST(f.StartTime AS DATETIME)) BETWEEN DATEADD(HOUR,-24, CAST(b.StartDateHospProvSpell AS DATETIME) + CAST(b.StartTimeHospProvSpell AS DATETIME))
        AND (CASE WHEN b.DischDateHospProvSpell is null THEN @ENDRPDate ELSE CAST(b.DischDateHospProvSpell AS DATETIME) + CAST(b.DischTimeHospProvSpell AS DATETIME) END) -- Only bring in episodes related to an admission
        --where f.UniqServReqID is null

         update f
            set f.[IP Flag] = 1,
                f.[IP Spell LOS] = b.HOSP_LOS,
                f.UniqHospProvSpellID = b.UniqHospProvSpellID,
                f.UniqServReqID = b.UniqServReqID
        from  #tempMHA f
        inner join [PATLondon].[MH_Spells]b on coalesce(b.Der_Person_ID,b.person_id) = coalesce(f.Der_Person_ID,f.person_id)
        and b.Provider_Name = f.[Provider Name]
        AND (
            CAST(f.StartDate AS DATETIME) + CAST(f.StartTime AS DATETIME)) BETWEEN DATEADD(HOUR,-24, CAST(b.StartDateHospProvSpell AS DATETIME) + CAST(b.StartTimeHospProvSpell AS DATETIME))
        AND (CASE WHEN b.DischDateHospProvSpell is null THEN @ENDRPDate ELSE CAST(b.DischDateHospProvSpell AS DATETIME) + CAST(b.DischTimeHospProvSpell AS DATETIME) END) -- Only bring in episodes related to an admission
        where f.[IP Flag] is null


         update f
            set f.[IP Flag] = 1,
                f.[IP Spell LOS] = b.HOSP_LOS,
                f.UniqHospProvSpellID = b.UniqHospProvSpellID
        from  #tempMHA f
        inner join [PATLondon].[MH_Spells]b on b.UniqServReqID = f.UniqServReqID
        where [IP Flag] is null
        --and b.RecordNumber = f.RecordNumber
        AND (
            CAST(f.StartDate AS DATETIME) + CAST(f.StartTime AS DATETIME)) BETWEEN DATEADD(HOUR,-24, CAST(b.StartDateHospProvSpell AS DATETIME) + CAST(b.StartTimeHospProvSpell AS DATETIME))
        AND (CASE WHEN b.DischDateHospProvSpell is null THEN @ENDRPDate ELSE CAST(b.DischDateHospProvSpell AS DATETIME) + CAST(b.DischTimeHospProvSpell AS DATETIME) END) -- Only bring in episodes related to an admission


    update y


    set y.[Ethnic proportion per 100000 of London Borough 2020] = (1/NULLIF(ep.Value,0))*100000

    from #tempMHA y
    left join  [PATLondon].[Ref_Ethnicity_2020_Census_Population_by_London_Borough]ep on ep.[Broad Ethnic Category] = y.[Derived Broad Ethnic Category]
                                                                                                        and ep.[Borough with NHS Pref] = y.[GP Local Authority]


    --select top 1000 * from #tempMHA

    delete x from  [PATLondon].[MH_Act]x
    where exists (select a.UniqMHActEpisodeID from #tempMHA a where a.UniqMHActEpisodeID = x.UniqMHActEpisodeID)


insert into [PATLondon].[MH_Act]
SELECT
*


from #tempMHA


     drop table #temp19


     update f
     set [Admission Type] = null
     from [PATLondon].[MH_Spells] f

        update sp

        set  UniqMHActEpisodeID = mha.UniqMHActEpisodeID,
        SectionType = mha.SectionType,
        [NHS LEgal Status Description] = mha.[NHS Legal Status Description],
        [Legal Status Start Date] = mha.StartDate,
        [Legal Status Start Time] = mha.StartTime,
        [Legal Status End Date] = mha.EndDate,
        [Legal Status End Time] = mha.EndTime

        from [PATLondon].[MH_Spells]sp
        inner join  [PATLondon].[MH_Act] mha on mha.UniqHospProvSpellID = sp.UniqHospProvSpellID
        and mha.[NHS Legal Status Description]
            in
            (
            'Formally detained under Mental Health Act Section 2',
            'Formally detained under Mental Health Act Section 3'
            )
    

                update sp

        set  UniqMHActEpisodeID = mha.UniqMHActEpisodeID,
        SectionType = mha.SectionType,
        [NHS LEgal Status Description] = mha.[NHS Legal Status Description],
        [Legal Status Start Date] = mha.StartDate,
        [Legal Status Start Time] = mha.StartTime,
        [Legal Status End Date] = mha.EndDate,
        [Legal Status End Time] = mha.EndTime

        from [PATLondon].[MH_Spells]sp
        inner join  [PATLondon].[MH_Act] mha on mha.UniqHospProvSpellID = sp.UniqHospProvSpellID
        and
        (
        mha.[NHS Legal Status Description]

            NOT in
            (
            'Formally detained under Mental Health Act Section 2',
            'Formally detained under Mental Health Act Section 3'
            )
            and sp.[NHS LEgal Status Description] is null
        )
        --if one of the Linked sections is a 36, this flags it
        update sp
            set sp.[Linked S136 Prior to Adm] = 1
        from [PATLondon].[MH_Spells]sp
        inner join  [PATLondon].[MH_Act] mha on mha.UniqHospProvSpellID = sp.UniqHospProvSpellID
        where  mha.[NHS Legal Status Description]
        in
        (
        'Formally detained under Mental Health Act Section 136'
        )
        and
        (
        mha.StartDate <= sp.StartDateHospProvSpell
        and
        DATEDIFF(day,mha.StartDate,sp.StartDateHospProvSpell)<=2
         )

            update sp

            set    SectionType = mha.SectionType,
                [NHS LEgal Status Description] = mha.[NHS Legal Status Description]
            from [PATLondon].[MH_Spells]sp
            inner join   [PATLondon].[MH_Act]  mha on mha.UniqHospProvSpellID = sp.UniqHospProvSpellID
            and mha.[NHS Legal Status Description]
            in
            (
            'Formally detained under Mental Health Act Section 2',
            'Formally detained under Mental Health Act Section 3'
            )





    update adm

            set    [Admission Type] = 'Formal'
            from [PATLondon].[MH_Spells]adm
             where [NHS LEgal Status Description]
            in
            (
            'Formally detained under Mental Health Act Section 2',
            'Formally detained under Mental Health Act Section 3'
            )


    update adm


            set    [Admission Type] =
                                    case  when  [NHS LEgal Status Description] is null then 'No Link to MHA'
                                        else 'Informal' end
            from [PATLondon].[MH_Spells]adm
            where adm.[Admission Type]  is null

