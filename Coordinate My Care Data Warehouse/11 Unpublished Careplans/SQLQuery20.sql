/****** Script for SelectTopNRows command from SSMS  ******/


	if OBJECT_ID ('Tempdb..#AT_Patient_CMCID') is not null
 	drop table #AT_Patient_CMCID 
  select 
Patient, 
cast(n.PatientNumber as bigint) as CMC_ID 
into #AT_Patient_CMCID
from ETL_PROD.dbo.CMC_Patient_PatientNumbers pn
join ETL_PROD.dbo.CMC_PatientNumber n on n.ItemId = pn.PatientNumber
where AssigningAuthority = 'CMC'

 


	if OBJECT_ID ('Tempdb..#AT_Patient_NHs') is not null
 	drop table #AT_Patient_NHs 
select 
Patient, 
n.PatientNumber as NHS_Number 
into #AT_Patient_NHs
from ETL_PROD.dbo.CMC_Patient_PatientNumbers pn
join ETL_PROD.dbo.CMC_PatientNumber n on n.ItemId = pn.PatientNumber
where AssigningAuthority = 'NHS'


 
 
 


	if OBJECT_ID ('Tempdb..#AT_Patient_Numbers') is not null
 	drop table #AT_Patient_Numbers
	select 
	distinct
	--a.Patient, 
	CMC_ID,
	b.NHS_Number NHS_Number  
	into #AT_Patient_Numbers
	from #AT_Patient_CMCID a
	left join #AT_Patient_NHs b on b.Patient = a.Patient

 


	if OBJECT_ID ('Tempdb..#Published') is not null
		drop table #Published

 select cmc_id into #Published from [ETL_Local_PROD].[dbo].[AT_Patient_General] 

    	if OBJECT_ID ('Tempdb..#UnPublished') is not null
		drop table #UnPublished

		SELECT [CMC_ID]
		,max(OverAllOrder) as LAstActionOrder,
		DerivedActionType 

	   into #UnPublished

	   FROM [ETL_Local_PROD].[dbo].[AT_CarePlanData]
	   where cmc_id not in (select * from #Published)
	   group by cmc_id,DerivedActionType



	   if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_UnpublishedPLans]') is not null
		drop table [ETL_Local_PROD].[dbo].[AT_UnpublishedPLans]

		select

		a.CMC_ID,
		d.NHS_Number,
		c.actionTime,
		c.DerivedActionType,
		row_number() over (partition by a.[CMC_ID] order by LAstActionOrder)  as LastActionOverAllOrder, 
		case when b.LastActionOVerAll is not null then 1 else null end  as LastActionFlag

		into [ETL_Local_PROD].[dbo].[AT_UnpublishedPLans]
		from #UnPublished a
		left join
				(
				select
				CMC_ID,
				Max(LAstActionOrder) as LastActionOVerAll
		from #UnPublished 
		group by CMC_ID
		)b on b.CMC_ID = a.CMC_ID and b.LastActionOVerAll = a.LAstActionOrder
		left join [ETL_Local_PROD].[dbo].[AT_CarePlanData]c on c.CMC_ID = a.CMC_ID and c.OverAllOrder = a.LAstActionOrder and c.DerivedActionType = a.DerivedActionType
		left join #AT_Patient_Numbers d on d.CMC_ID = a.CMC_ID






		select * FROM #UnPublished  where cmc_id = 100075116 order by LAstActionOrder
		select * FROM [ETL_Local_PROD].[dbo].[AT_CarePlanData] where cmc_id = 100014568 order by actionTime
		 select * FROM [ETL_Local_PROD].[dbo].[AuditPatient_New] where cmc_id = 100000021   order by actionTime

		select * from [ETL_Local_PROD].[dbo].[AT_Patient_General] where CMC_ID = 100075116

		select * from [ETL_Local_PROD].[dbo].[AT_UnpublishedPLans]where LastActionFlag = 1 and DerivedActionType = 'discard'

		select * from [ETL_Local_PROD].[dbo].[AT_UnpublishedPLans]where CMC_ID = 100041536


		 --select * from  [ETL_Local_PROD].[dbo].[AT_CarePlanVersion] where GenusId =  100075116
	 

	 select
	 *
	 from
	 (
	 select 
	 * 
	 from [ETL_Local_PROD].[dbo].[AT_UnpublishedPLans] 
	 where CMC_ID not in (select CMC_id from [ETL_Local_PROD].[dbo].[AT_UnpublishedPLans] where   DerivedActionType  in ('publish','delete'))
	 and LastActionFlag = 1
	 --and actionTime >= '2019-04-01'
	 )d
	 where d.CMC_ID not in (
							(select CMC_id from [ETL_Local_PROD].[dbo].[AT_UnpublishedPLans] where   DerivedActionType  in ('discard'))
							)


 select * from #AT_Patient_Numbers

 		select count(*) from [ETL_Local_PROD].[dbo].[AT_Patient_General]


select * from #AT_Patient_Numbers
select * from ETL_PROD.dbo.CMC_Patient_PatientNumbers
select * from ETL_PROD.dbo.CMC_PatientNumber 
--select * from #AT_NHSNumbers
 
 





select
z.*,
cpr.*

from [ETL_Local_PROD].[dbo].[AT_UnpublishedPLans]	z
Left join  [ETL_Local_PROD].[dbo].[AT_CarePlanVersion] a on a.GenusId = z.CMC_ID
 
Left join ETL_PROD.dbo.CMC_PatientSummary ps on ps.ItemID =  a.LastPatientSummary  
Left join ETL_PROD.dbo.CMC_Patient p on p.ItemId = ps.Patient  
Left join ETL_PROD.dbo.CMC_Name n on n.itemid = p.Name  
 
left join ETL_PROD.dbo.CMC_CarePackage pkg on pkg.ItemId = ps.CarePackage
left join ETL_PROD.dbo.CMC_Consent c on c.ItemId = p.Consent
left join ETL_PROD.dbo.CMC_CPR cpr on ps.CPR = cpr.ItemId

where LastActionFlag = 1 and DerivedActionType <> 'publish'
and cpr.Decision is not null
and z.LastActionFlag = 1
and Decision = 'N'
order by actionTime

 