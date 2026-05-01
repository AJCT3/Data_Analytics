USE [Informatics_Reporting]
GO

/****** Object:  View [etl].[vw_ClaimGeneral]    Script Date: 19/07/2018 09:41:36 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






ALTER VIEW [etl].[vw_ClaimGeneral]
AS
    SELECT 
        C.ClaimId, 
        C.ClaimRef, 
        C.OldRef, 
        CL.ClaimantFirstName, 
        CL.ClaimantSurname, 
        CL.PatientFirstName, 
        CL.PatientSurname,
        SC.ScheduleName,
        CST.ClaimStatusTypeName, 
        CASE CST.StatusType
            WHEN 1 THEN 'Open'
            WHEN 2 THEN 'Closed'
            WHEN 3 THEN 'Incident'
            WHEN 4 THEN 'Non-Claim'
            ELSE 'Unknown'
        END AS CurrentStatusType,
        C.StatusDateChange,
        C.SettlementDate,
        C.IncidentDate,
        C.NotificationDate,
        CSH.DateOfChange AS DateOfEntry,
        C.CreationDate,
        C.OpenDate,
        C.CloseDate,
        
		case 
			when cst.ClaimStatusTypeName IN ('Settled below excess', 'Claim Cancelled', 'No IBNR Cover') then 1 
				else null
		End as [Void Claim Flag],--NEw Field
		S.SchemeAbbrev,  	
	CASE 
		WHEN SchemeAbbrev = 'DH CL' AND convert(Date,IncidentDate) > '1995-03-31' THEN 'CNST' 
		WHEN SchemeAbbrev = 'DH CL' AND convert(Date,IncidentDate) < '1995-04-01' THEN 'ELS' 
		ELSE SchemeAbbrev 
	END as [SchemeAbbrev adj],	--NEw Field	
	CASE 
		WHEN SchemeAbbrev IN ('CNST', 'ELS', 'ExRHA', 'DH CL') THEN 'Clinical' 
		ELSE 'Non Clinical' 
	END as [Clinical / Non Clinical], 	--NEw Field	
	CASE 
		WHEN (
			(CST.StatusType=  2 AND ISNULL(DamagesPaid.DamagesPayments, 0) > 0) 
			OR ClaimStatusTypeName IN ('Periodical Payments', 'Periodical Payments with Reverse Indemnity', 'periodical payments and Indemnity Given')
			) THEN 'Successful' 
		WHEN (CST.StatusType=  2   AND ISNULL(DamagesPaid.DamagesPayments, 0)  <= 0) THEN 'Unsuccessful' 
		ELSE 'Open' 
	END as [Successful/Unsuccessful],  	--NEw Field	
		CASE
		WHEN [ClaimStatusTypeName] IN('Periodical Payments', 'Periodical Payments with Reverse Indemnity', 'periodical payments and Indemnity Given') THEN   c.[SettlementDate] 
		WHEN CST.StatusType = 2 THEN  c.[CloseDate] 
		ELSE null
		END as [Closure Date (Settlement Year for PPOs)],--NEw field
        M.MemberNum AS MemberCode,
        O.OrgName AS MemberName, 
        C.MemberRef, 
        GC.GroupedClaimsName AS GroupedClaimsCode,
        GC.Description AS GroupedClaimsDescription,
		IGC.GroupedClaimsName AS InternalGroupedClaimsCode,--NEw field
        IGC.Description AS InternalGroupedClaimsDescription,--NEw field
        Site.SiteCode,
        ISNULL(Site.SiteName, CASE WHEN C.SiteId IS NULL THEN NULL ELSE 'Other' END) AS SiteName,
        CR.EstSettlementDate,
        C.IncidentDescription,
        CU2.FirstName AS UserWhoEnteredFirstName,
        CU2.Surname AS UserWhoEnteredSurname,
        CU1.FirstName AS HandlerFirstName,
        CU1.Surname AS HandlerSurname,
        CASE CST.StatusType
            WHEN 2 THEN 
                    ISNULL(DamagesPaid.DamagesPayments, 0) +
                    ISNULL(DefenceCostsPaid.DefenceCostsPayments, 0) +
                    ISNULL(ClaimantCostsPaid.ClaimantCostsPayments, 0) 
            ELSE 	ISNULL(Reserve.Reserve, 0)
        END AS TotalClaim,
        CASE CST.StatusType
            WHEN 2 THEN 0
            ELSE
                    ISNULL(Reserve.Reserve, 0) -
                    (ISNULL(DamagesPaid.DamagesPayments, 0) +
                    ISNULL(DefenceCostsPaid.DefenceCostsPayments, 0) +
                    ISNULL(ClaimantCostsPaid.ClaimantCostsPayments, 0)) 
        END AS TotalOSEstimate,
        CASE CST.StatusType
            WHEN 2 THEN 0
            ELSE 	ISNULL(Reserve.Damages, 0) - ISNULL(DamagesPaid.DamagesPayments, 0) 
        END AS OSDamages,
        CASE CST.StatusType
            WHEN 2 THEN 0
            ELSE	ISNULL(Reserve.DefenceCosts, 0) - ISNULL(DefenceCostsPaid.DefenceCostsPayments, 0) 
        END AS OSDefenceCosts,
        CASE CST.StatusType
            WHEN 2 THEN 0
            ELSE	ISNULL(Reserve.ClaimantCosts, 0) - ISNULL(ClaimantCostsPaid.ClaimantCostsPayments, 0) 
        END AS OSPlaintifCosts,
        ISNULL(DamagesPaid.DamagesPayments, 0) +
        ISNULL(DefenceCostsPaid.DefenceCostsPayments, 0) +
        ISNULL(ClaimantCostsPaid.ClaimantCostsPayments, 0) AS TotalPaid,
        ISNULL(DamagesPaid.DamagesPayments, 0) AS DamagesPaid,
        ISNULL(DefenceCostsPaid.DefenceCostsPayments, 0) AS DefenceCostsPaid,
        ISNULL(ClaimantCostsPaid.ClaimantCostsPayments, 0) AS ClaimantCostsPaid,
        ISNULL(NHSLAPaid.NHSLAPayments, 0) AS NHSLAPayments,
        ISNULL(MemberPayments.Amount, 0) AS MemberPayments,
        ISNULL(NHSLADefenceCostsPaid.NHSLADefenceCostsPayments, 0) AS NHSLADefenceCostsPaid,
        C.Excess AS ApplicableExcess,
        CASE
            WHEN CASE CST.StatusType
                WHEN 2 THEN 
                        ISNULL(DamagesPaid.DamagesPayments, 0) +
                        ISNULL(DefenceCostsPaid.DefenceCostsPayments, 0) +
                        ISNULL(ClaimantCostsPaid.ClaimantCostsPayments, 0) 
                ELSE 	ISNULL(Reserve.Reserve, 0)
            END > C.Excess THEN C.Excess
            ELSE CASE CST.StatusType
                WHEN 2 THEN 
                        ISNULL(DamagesPaid.DamagesPayments, 0) +
                        ISNULL(DefenceCostsPaid.DefenceCostsPayments, 0) +
                        ISNULL(ClaimantCostsPaid.ClaimantCostsPayments, 0) 
                ELSE 	ISNULL(Reserve.Reserve, 0)
            END
        END AS [Actual Excess (RPST Only)],
        ISNULL(Reserve.Reserve, 0) - C.Excess AS NHSLAFunded,
        C.HumanRightsAct AS InquestCosts,
        C.Mediation,
        CP.ProbabilityDesc AS Probability,
        Reserve.PercentageShare,
        DefenceSolicitor.DefenceSolicitorId, 
        DefenceSolicitor.OrgName AS SolicitorDescription,
        DefenceSolicitor.ContactRef AS SolicitorRef,
        ISNULL(DefenceSolicitor.FirstName, '') + ' ' + ISNULL(DefenceSolicitor.Surname, '') AS DefenceSolicitorName,    
        ClaimantSolicitor.OrganisationId as ClaimantSolicitorOrganisationId, 
        ClaimantSolicitor.OrgName AS ClaimantSolicitor,    
        ISNULL(AR.AddedByAddClaimWizard, 0) AS PendingFirstReserve,
        CAST(
            CASE
                WHEN pc.PortalClaimId IS NULL
                THEN 0
                ELSE 1
        END AS BIT) AS PortalClaim,
        pc.ExitReason  AS ExitReason,
        pc.ExitComments,
        pc.ExitedBy  AS ExitedBy,
         pc.ClaimType  AS ClaimType,
		
		(SELECT MAX(Foo) FROM
			(
				SELECT Foo = CAST(C.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(CST.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(S.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(O.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(M.Timestamp AS BIGINT) UNION ALL 
				SELECT Foo = CAST(CU1.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(CL.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(CSH.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(CU2.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(SC.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(GC.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(IGC.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(MS.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(Site.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(CR.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(AR.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(CP.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(PC.CMSTimestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(Reserve.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(DamagesPaid.MaxTimestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(DefenceCostsPaid.MaxTimestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(ClaimantCostsPaid.MaxTimestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(NHSLAPaid.MaxTimestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(NHSLADefenceCostsPaid.MaxTimestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(MemberPayments.MaxTimestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(MemberPayments.MaxTimestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(DefenceSolicitor.MaxTimestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(ClaimantSolicitor.MaxTimestamp AS BIGINT) 
			) AS CMSTimestamp
		) AS CMSTimestamp
    FROM [ClaimsManagement].[dbo].Claim C
    INNER JOIN [ClaimsManagement].[dbo].ClaimStatusType CST ON CST.ClaimStatusTypeId = C.StatusId
    INNER JOIN [ClaimsManagement].[dbo].Scheme S ON S.SchemeId = C.SchemeId
    INNER JOIN [ClaimsManagement].[dbo].Organisation O ON O.OrganisationId = C.MemberId
    INNER JOIN [ClaimsManagement].[dbo].Member M ON M.MemberId = C.MemberId
    INNER JOIN [ClaimsManagement].[dbo].CmsUser CU1 ON CU1.CmsUserId = C.HandlerId
    LEFT OUTER JOIN [ClaimsManagement].[dbo].Claimant CL ON CL.ClaimId = C.ClaimId
    LEFT OUTER JOIN [ClaimsManagement].[dbo].ClaimStatusHistory CSH ON CSH.ClaimId = C.ClaimId AND OldStatusId IS NULL
    LEFT OUTER JOIN [ClaimsManagement].[dbo].CmsUser CU2 ON CU2.CmsUserId = CSH.CmsUserId
    LEFT OUTER JOIN [ClaimsManagement].[dbo].Schedule SC ON C.ScheduleId = SC.ScheduleId
    LEFT OUTER JOIN [ClaimsManagement].[dbo].GroupedClaims GC ON C.GroupedClaimsId = GC.GroupedClaimsId
	LEFT OUTER JOIN [ClaimsManagement].[dbo].GroupedClaims IGC ON C.InternalGroupedClaimsId = IGC.GroupedClaimsId
    LEFT OUTER JOIN [ClaimsManagement].[dbo].MemberSite MS ON MS.MemberId = C.MemberId AND MS.SiteId = C.SiteId
    LEFT OUTER JOIN [ClaimsManagement].[dbo].Site ON MS.SiteId = Site.SiteId
    LEFT OUTER JOIN [ClaimsManagement].[dbo].ClaimReserve CR ON C.LatestReserveId = CR.ClaimReserveId
    LEFT OUTER JOIN [ClaimsManagement].[dbo].ActivityReferral AR ON C.PendingActivityId = AR.ActivityId and AR.Outcome is null
    LEFT OUTER JOIN [ClaimsManagement].[dbo].ClaimProbability CP ON CR.ProbabilityId = CP.ClaimProbabilityId
    LEFT OUTER JOIN (
        SELECT C.ClaimId, ISNULL(DefenceCosts, 0) + ((ISNULL(Damages,0) + ISNULL(ClaimantCosts,0)) * CONVERT(MONEY, PercentageShare) / 100 ) AS Reserve, 
            DefenceCosts, Damages * CONVERT(MONEY, PercentageShare) / 100 AS Damages, 
            ClaimantCosts * CONVERT(MONEY, PercentageShare) / 100 AS ClaimantCosts,
            PercentageShare,
			SLC.Timestamp
        FROM [ClaimsManagement].[dbo].ClaimReserve CR
        INNER JOIN [ClaimsManagement].[dbo].Claim C ON C.LatestReserveId = CR.ClaimReserveId
        INNER JOIN [ClaimsManagement].[dbo].ShareLinkClaim SLC ON C.ShareLinkId = SLC.ShareLinkId AND C.ClaimId = SLC.ClaimId
    ) AS Reserve ON C.ClaimId = Reserve.ClaimId
    LEFT OUTER JOIN (
		SELECT SUM(CASE PT.VATRecoverable WHEN 1 THEN Amount ELSE Amount + VAT END) AS DamagesPayments, 
			ClaimId,
			(SELECT MAX(Foo) FROM
				(
					SELECT Foo = MAX(CAST(CPI.Timestamp AS BIGINT)) UNION ALL
					SELECT Foo = MAX(CAST(CP.Timestamp AS BIGINT)) UNION ALL
					SELECT Foo = MAX(CAST(PT.Timestamp AS BIGINT)) 
				) AS MaxFoo
			) AS MaxTimestamp
        FROM [ClaimsManagement].[dbo].ClaimPaymentItem CPI
        INNER JOIN [ClaimsManagement].[dbo].ClaimPayment CP ON CP.ClaimPaymentId = CPI.ClaimPaymentId
        INNER JOIN [ClaimsManagement].[dbo].PaymentType PT ON CPI.PaymentTypeId = PT.PaymentTypeId
        WHERE CP.StatusId IN (1, 2, 4, 5)
        AND PT.DamDefClaim = 1
        AND CPI.ExcessInvoicePortion <> 4
        GROUP BY ClaimId
    ) AS DamagesPaid ON C.ClaimId = DamagesPaid.ClaimId
    LEFT OUTER JOIN (
        SELECT SUM(CASE PT.VATRecoverable WHEN 1 THEN Amount ELSE Amount + VAT END) AS DefenceCostsPayments, 
			ClaimId, 
			(SELECT MAX(Foo) FROM
				(
					SELECT Foo = MAX(CAST(CPI.Timestamp AS BIGINT)) UNION ALL
					SELECT Foo = MAX(CAST(CP.Timestamp AS BIGINT)) UNION ALL
					SELECT Foo = MAX(CAST(PT.Timestamp AS BIGINT)) 
				) AS MaxFoo
			) AS MaxTimestamp
        FROM [ClaimsManagement].[dbo].ClaimPaymentItem CPI
        INNER JOIN [ClaimsManagement].[dbo].ClaimPayment CP ON CP.ClaimPaymentId = CPI.ClaimPaymentId
        INNER JOIN [ClaimsManagement].[dbo].PaymentType PT ON CPI.PaymentTypeId = PT.PaymentTypeId
        WHERE CP.StatusId IN (1, 2, 4, 5)
        AND PT.DamDefClaim = 2
        AND CPI.ExcessInvoicePortion <> 4
        GROUP BY ClaimId
    ) AS DefenceCostsPaid ON C.ClaimId = DefenceCostsPaid.ClaimId
    LEFT OUTER JOIN (
        SELECT SUM(CASE PT.VATRecoverable WHEN 1 THEN Amount ELSE Amount + VAT END) AS ClaimantCostsPayments, 
			ClaimId,
			(SELECT MAX(Foo) FROM
				(
					SELECT Foo = MAX(CAST(CPI.Timestamp AS BIGINT)) UNION ALL
					SELECT Foo = MAX(CAST(CP.Timestamp AS BIGINT)) UNION ALL
					SELECT Foo = MAX(CAST(PT.Timestamp AS BIGINT)) 
				) AS MaxFoo
			) AS MaxTimestamp
        FROM [ClaimsManagement].[dbo].ClaimPaymentItem CPI
        INNER JOIN [ClaimsManagement].[dbo].ClaimPayment CP ON CP.ClaimPaymentId = CPI.ClaimPaymentId
        INNER JOIN [ClaimsManagement].[dbo].PaymentType PT ON CPI.PaymentTypeId = PT.PaymentTypeId
        WHERE CP.StatusId IN (1, 2, 4, 5)
        AND PT.DamDefClaim = 3
        AND CPI.ExcessInvoicePortion <> 4
        GROUP BY ClaimId
    ) AS ClaimantCostsPaid ON C.ClaimId = ClaimantCostsPaid.ClaimId
    LEFT OUTER JOIN (
        SELECT SUM(CASE PT.VATRecoverable WHEN 1 THEN Amount ELSE Amount + VAT END) AS NHSLAPayments, 
			ClaimId,
			(SELECT MAX(Foo) FROM
				(
					SELECT Foo = MAX(CAST(CPI.Timestamp AS BIGINT)) UNION ALL
					SELECT Foo = MAX(CAST(CP.Timestamp AS BIGINT)) UNION ALL
					SELECT Foo = MAX(CAST(PT.Timestamp AS BIGINT)) 
				) AS MaxFoo
			) AS MaxTimestamp
        FROM [ClaimsManagement].[dbo].ClaimPaymentItem CPI
        INNER JOIN [ClaimsManagement].[dbo].ClaimPayment CP ON CP.ClaimPaymentId = CPI.ClaimPaymentId
        INNER JOIN [ClaimsManagement].[dbo].PaymentType PT ON CPI.PaymentTypeId = PT.PaymentTypeId
        WHERE CP.StatusId IN (1, 2, 4, 5)
        AND CP.FundTypeId = 1
        AND CPI.ExcessInvoicePortion <> 4
        GROUP BY ClaimId
    ) AS NHSLAPaid ON C.ClaimId = NHSLAPaid.ClaimId
    LEFT OUTER JOIN (
        SELECT SUM(CASE PT.VATRecoverable WHEN 1 THEN Amount ELSE Amount + VAT END) AS NHSLADefenceCostsPayments,
			ClaimId ,
			(SELECT MAX(Foo) FROM
				(
					SELECT Foo = MAX(CAST(CPI.Timestamp AS BIGINT)) UNION ALL
					SELECT Foo = MAX(CAST(CP.Timestamp AS BIGINT)) UNION ALL
					SELECT Foo = MAX(CAST(PT.Timestamp AS BIGINT)) 
				) AS MaxFoo
			) AS MaxTimestamp
        FROM [ClaimsManagement].[dbo].ClaimPaymentItem CPI
        INNER JOIN [ClaimsManagement].[dbo].ClaimPayment CP ON CP.ClaimPaymentId = CPI.ClaimPaymentId
        INNER JOIN [ClaimsManagement].[dbo].PaymentType PT ON CPI.PaymentTypeId = PT.PaymentTypeId
        WHERE CP.StatusId IN (1, 2, 4, 5)
        AND CP.FundTypeId = 1
        AND PT.DamDefClaim = 2
        AND CPI.ExcessInvoicePortion <> 4
        GROUP BY ClaimId
    ) AS NHSLADefenceCostsPaid ON C.ClaimId = NHSLADefenceCostsPaid.ClaimId
    LEFT OUTER JOIN (
        SELECT SUM(CASE PT.VATRecoverable WHEN 1 THEN Amount ELSE Amount + VAT END) AS Amount, 
			ClaimId,
			(SELECT MAX(Foo) FROM
				(
					SELECT Foo = MAX(CAST(CPI.Timestamp AS BIGINT)) UNION ALL
					SELECT Foo = MAX(CAST(CP.Timestamp AS BIGINT)) UNION ALL
					SELECT Foo = MAX(CAST(PT.Timestamp AS BIGINT)) 
				) AS MaxFoo
			) AS MaxTimestamp
        FROM [ClaimsManagement].[dbo].ClaimPaymentItem CPI
        INNER JOIN [ClaimsManagement].[dbo].ClaimPayment CP ON CP.ClaimPaymentId = CPI.ClaimPaymentId
        INNER JOIN [ClaimsManagement].[dbo].PaymentType PT ON CPI.PaymentTypeId = PT.PaymentTypeId
        WHERE CP.StatusId IN (1, 2, 4, 5)
        AND CP.FundTypeId = 2
        AND CPI.ExcessInvoicePortion <> 4
        GROUP BY ClaimId
    ) AS MemberPayments ON C.ClaimId = MemberPayments.ClaimId
    LEFT OUTER JOIN (
        SELECT CC1.ClaimId, CC1.ContactRef, O.OrgName, C1.FirstName, C1.Surname, C1.ContactId as DefenceSolicitorId,
		(SELECT MAX(Foo) FROM
			(
				SELECT Foo = CAST(CC1.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(C1.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(O.Timestamp AS BIGINT) 
			) AS MaxFoo
		) AS MaxTimestamp
        FROM [ClaimsManagement].[dbo].ClaimContact CC1 
        INNER JOIN [ClaimsManagement].[dbo].Contact C1 ON CC1.ContactId = C1.ContactId
        INNER JOIN [ClaimsManagement].[dbo].Organisation O ON O.OrganisationId = C1.OrganisationId
        WHERE C1.ContactTypeId = 2
        AND ISNULL(CC1.EndDate, CURRENT_TIMESTAMP) >= CURRENT_TIMESTAMP
        AND CC1.StartDate = (
            SELECT MAX (CC2.StartDate)
            FROM [ClaimsManagement].[dbo].ClaimContact CC2 
			INNER JOIN [ClaimsManagement].[dbo].Contact C2 ON CC2.ContactId = C2.ContactId
            WHERE C2.ContactTypeId = 2
            AND ISNULL(CC2.EndDate, CURRENT_TIMESTAMP) >= CURRENT_TIMESTAMP
            AND CC2.ClaimId = CC1.ClaimId
        )
    ) AS DefenceSolicitor ON C.ClaimId = DefenceSolicitor.ClaimId
    LEFT OUTER JOIN (
        SELECT CC1.ClaimId, O.OrganisationId, O.OrgName,
		(SELECT MAX(Foo) FROM
			(
				SELECT Foo = CAST(CC1.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(C1.Timestamp AS BIGINT) UNION ALL
				SELECT Foo = CAST(O.Timestamp AS BIGINT) 
			) AS MaxFoo
		) AS MaxTimestamp
        FROM [ClaimsManagement].[dbo].ClaimContact CC1 
        INNER JOIN [ClaimsManagement].[dbo].Contact C1 ON CC1.ContactId = C1.ContactId
        INNER JOIN [ClaimsManagement].[dbo].Organisation O ON O.OrganisationId = C1.OrganisationId
        WHERE C1.ContactTypeId = 3
        AND ISNULL(CC1.EndDate, CURRENT_TIMESTAMP) >= CURRENT_TIMESTAMP
        AND CC1.StartDate = (
            SELECT MAX (CC2.StartDate)
            FROM [ClaimsManagement].[dbo].ClaimContact CC2 
			INNER JOIN [ClaimsManagement].[dbo].Contact C2 ON CC2.ContactId = C2.ContactId
            WHERE C2.ContactTypeId = 3
            AND ISNULL(CC2.EndDate, CURRENT_TIMESTAMP) >= CURRENT_TIMESTAMP
            AND CC2.ClaimId = CC1.ClaimId
        )
    ) AS ClaimantSolicitor ON C.ClaimId = ClaimantSolicitor.ClaimId
    LEFT OUTER JOIN [Informatics_Reporting].[etl].[PortalClaim]PC 
        ON PC.ClaimId = C.ClaimId





GO


