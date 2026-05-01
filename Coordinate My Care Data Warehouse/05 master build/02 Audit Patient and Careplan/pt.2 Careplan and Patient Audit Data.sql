



 
 use ETL_Local_PROD
 
GO
/****** Object:  StoredProcedure [dbo].[NewCache]    Script Date: 07/01/2020 13:12:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






alter PROCEDURE [dbo].[AT_MASTERBUILD_AuditPatient_AuditCareplan] 
-- Amended for PD Upgrade
AS
BEGIN



 
	 if OBJECT_ID ('Tempdb..#AuditPatient') is not null
	drop table #AuditPatient
	select
	cast(ltrim(rtrim(RIGHT(GenusId,CHARINDEX('||',REVERSE(GenusId))-1)))as Nvarchar(75)) as GenusID,
	cast(REPLACE(ad.GenusId,'PatientSummary||','')as Nvarchar(75)) as CMC_ID,
	cast(REPLACE(FromVersionId,'PatientSummary','PS')as Nvarchar(75))  as FromPatientSummary,
	cast(REPLACE(ToVersionId,'PatientSummary','PS')as Nvarchar(75))  as ToPatientSummary,
	ItemId as PatAuditID,
	ActionTime,
	ActionType,
	(select splitdata from (select splitdata,ROW_NUMBER() over (order by offset) as rn from [ETL_Local_PROD].[dbo].[SplitString](Actor,'|')) sel1 where rn=1) as StaffRegistryId,
	(select splitdata from (select splitdata,ROW_NUMBER() over (order by offset) as rn from [ETL_Local_PROD].[dbo].[SplitString](Actor,'|')) sel1 where rn=3) as DeptPDRegistryId,
	cast((select splitdata from (select splitdata,ROW_NUMBER() over (order by offset) as rn from [ETL_Local_PROD].[dbo].[SplitString](Actor,'|')) sel1 where rn=5)as Nvarchar(100))  as Role,
	-- Add CMC_AuditLogin linkage info MS 4.7.16
	LoginRowId as LoginReference,
	Comment
	into #AuditPatient
 
	from ETL_PROD.dbo.CMC_AuditData ad 
 
	Where 
	 --not exists (select PatAuditID from [ETL_Local_PROD].[dbo].[AuditPatient_New] z where ad.ItemId = z.PatAuditID)
	--and  
	(Actor <> 'System' and ActionType not in ('Login','Logout'))
	and RecordName = 'PatientSummary'
	--and ltrim(rtrim(RIGHT(GenusId,CHARINDEX('||',REVERSE(GenusId))-1))) in (select distinct GenusId from [ETL_Local_PROD].[dbo].[AT_CarePlanVersion])
 
	--select top 50 * from #AuditPatient

--	ALTER TABLE [ETL_Local_PROD].[dbo].[AuditPatient_New] 
--DROP COLUMN AuditDataType;



	insert into [ETL_Local_PROD].[dbo].[AuditPatient_New] 

	SELECT
	GenusID,
	CMC_ID,
	FromPatientSummary,
	ToPatientSummary,
	PatAuditID,
	ActionTime,
	ActionType,
	StaffRegistryId,
	DeptPDRegistryId,
	Role,
	LoginReference,
	null as OverAllOrder,
	null as ActionTypeOrder,
	 Comment

 

  FROM #AuditPatient x

  Where not exists (select PatAuditID from [ETL_Local_PROD].[dbo].[AuditPatient_New] z where x.PatAuditID = z.PatAuditID)

 

 update r
		set r.OverAllOrder = b.OverAllOrder,
			r.ActionTypeOrder = b.ActionTypeOrder

 from  [ETL_Local_PROD].[dbo].[AuditPatient_New] r
 left join
 (
 select
 PatAuditID,
 CMC_ID,
 ActionTime,
 ActionType,
ROW_NUMBER() over (PARTITION by CMC_ID  order by ActionTime,PatAuditID)  as OverAllOrder,
ROW_NUMBER() over (PARTITION by CMC_ID,ActionType order by ActionTime,PatAuditID) as ActionTypeOrder
 from  [ETL_Local_PROD].[dbo].[AuditPatient_New] 
 )as b on b.PatAuditID = r.PatAuditID







 if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AuditPatient_CarePlan_New]') is not null
 drop table [ETL_Local_PROD].[dbo].[AuditPatient_CarePlan_New]
 select
REPLACE(ad.GenusId,'CarePlan||','') as CMC_ID,
FromVersionId as FromCarePlan,
ToVersionId as ToCarePlan,
ItemId as CPAuditID,
null as OverAllOrder,
null as ActionTypeOrder,
ActionTime,
ActionType,
(select splitdata from (select splitdata,ROW_NUMBER() over (order by offset) as rn from [ETL_Local_PROD].[dbo].[SplitString](Actor,'|')) sel1 where rn=1) as StaffRegistryId,
(select splitdata from (select splitdata,ROW_NUMBER() over (order by offset) as rn from [ETL_Local_PROD].[dbo].[SplitString](Actor,'|')) sel1 where rn=3) as DeptPDRegistryId,
(select splitdata from (select splitdata,ROW_NUMBER() over (order by offset) as rn from [ETL_Local_PROD].[dbo].[SplitString](Actor,'|')) sel1 where rn=5) as Role,
Comment

into [ETL_Local_PROD].[dbo].[AuditPatient_CarePlan_New]

from  ETL_PROD.dbo.CMC_AuditData ad where Actor <> 'System' and ActionType not in ('Login','Logout') and RecordName = 'CarePlan' 


--select top 500 * from [ETL_Local_PROD].[dbo].[AuditPatient_New] order by CMC_ID, overallOrder
--select top 500 * from [ETL_Local_PROD].[dbo].[AuditPatient_CarePlan_New] order by CMC_ID, overallOrder
 update r
		set r.OverAllOrder = b.OverAllOrder,
			r.ActionTypeOrder = b.ActionTypeOrder

 from  [AuditPatient_CarePlan_New] r
 left join
 (
 select
 CPAuditID,
 CMC_ID,
 ActionTime,
 ActionType,
ROW_NUMBER() over (PARTITION by CMC_ID  order by ActionTime,CPAuditID)  as OverAllOrder,
ROW_NUMBER() over (PARTITION by CMC_ID,ActionType order by ActionTime,CPAuditID) as ActionTypeOrder
 from  [ETL_Local_PROD].[dbo].[AuditPatient_CarePlan_New]
 )as b on b.CPAuditID = r.CPAuditID


--view [Reporting].[Cache-AccessDataDetail]

  if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_AccessDataDetail]') is not null
	drop table [ETL_Local_PROD].[dbo].[AT_AccessDataDetail] 
	select
	PatAuditID as ADAuditID,
	OverAllOrder,
	ActionTypeOrder,
	ap.cmc_id,
	ActionTime as ActivityDate,
	ActionType as [Access Type],
	s.StaffEnterpriseId,
	ISNULL(s.StaffForename + ' ','') + ISNULL(s.StaffSurname,'') as [User Name],
	d1.DeptEnterpriseId as ActivityEnterpriseId,
	d1.DeptName as name,
	d1.LocalCMCOrgTypeDescription as TeamType,
	s.StaffActiveDescription as Active_Status,
	s.StaffProviderTypeDescription as UserType,
	io.CMCRoleDescription,
	ap.Role,
	-- MS 11.3.16 add version information
	ap.FromPatientSummary, ap.ToPatientSummary,
	comment

	into [ETL_Local_PROD].[dbo].[AT_AccessDataDetail] 

	from [ETL_Local_PROD].[dbo].[AuditPatient_New]  ap
	join [ETL_Local_PROD].[dbo].[AT_Staff] s on s.StaffRegistryId = ap.StaffRegistryId
	join [ETL_Local_PROD].[dbo].[AT_PD_Dept] d1 on d1.DeptPDRegistryId = ap.DeptPDRegistryId
	join [ETL_Local_PROD].[dbo].[AT_IndToOrg] io on io.StaffEnterpriseId = s.StaffEnterpriseId and io.DeptEnterpriseId = d1.DeptEnterpriseId


 END
