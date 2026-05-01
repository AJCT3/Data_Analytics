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
pn.Patient, 
n.PatientNumber as NHS_Number ,
o.CMC_ID
into #AT_Patient_NHs
from ETL_PROD.dbo.CMC_Patient_PatientNumbers pn
left join ETL_PROD.dbo.CMC_PatientNumber n on n.ItemId = pn.PatientNumber
left join #AT_Patient_CMCID o on o.Patient = pn.Patient
where AssigningAuthority = 'NHS'


 --select *from #AT_Patient_NHs
 
 


	if OBJECT_ID ('Tempdb..#AT_Patient_Numbers') is not null
 	drop table #AT_Patient_Numbers
	select 
	distinct
	--a.Patient, 
	CMC_ID,
	--b.NHS_Number  
	cast(null as nvarchar(20)) as NHS_Number
	into #AT_Patient_Numbers
	from #AT_Patient_CMCID a

	union

	select
	distinct
	CMC_ID,
	cast(null as nvarchar(20)) as NHS_Number
	FROM [ETL_Local_PROD].[dbo].[AT_CarePlanData]
	--left join #AT_Patient_NHs b on b.Patient = a.Patient


	update r
			set r.NHS_Number = b.NHS_Number

	from #AT_Patient_Numbers r
	left join #AT_Patient_NHs b on b.CMC_ID = r.CMC_ID


	--select * from #AT_Patient_Numbers where NHS_Number is null
 
 	if OBJECT_ID ('Tempdb..#AT_Patient_NumbersNotPub') is not null
 	drop table #AT_Patient_NumbersNotPub
 SELECT a.[ItemID]
      ,[Patient]
      ,[AssigningAuthorityName]
      ,[Extension] as NHS_Number
	  ,MRN as CMC_id
      ,[Root]
      ,[Status]
      ,[Type]
      ,[Usage]
	   ,[AssigningAuthority]
      ,[BirthDateTime]
      ,[BirthMonth]
      ,[BirthOrder]
      ,[BirthYear]
      ,[CommunicationPreference]
      ,[CreatedBy]
      ,[CreatedOn]
      ,[CustomClassName]
      ,[CustomXMLString]
      ,[DeathTime]
      ,[Facility]
      ,[FullName]
      ,[LastEnteredBy]
      ,[LastEnteredOn]
      ,[LastEnteredOnC]
      ,[UTCLastEnteredOn]
      ,[UTCLastEnteredOnC]
      ,[LastUpdated]
      ,[MothersMaidenSurname]
      ,[MPIID]
      --,[MRN]
      ,[SSN]
      ,[VIP]
	  into #AT_Patient_NumbersNotPub
  FROM [ETL_PROD].[dbo].[CMC_Registry_Patient_Identifier]a
  inner join [ETL_PROD].[dbo].[CMC_Registry_Patient]b on b.ItemID = a.Patient
  
  --GP PRactice


  --SELECT      g.CCG as OrganisationCCG
		--,g.STP as OrgSTP
		-- ,g.[NHS Region] as OrgNHSRegion
		-- ,e.Name
  --    ,[Active]
  --    ,[CreatedTime]
	 --, row_number() over (partition by a.[PatientGenusID] order by [CreatedTime])  as LastActionOverAllOrder
  --    ,[ExpiresAt]
  --    ,[OrganizationRegistryID]
	 -- ,[PatientGenusID]
  --    , convert(int, ltrim(rtrim(SUBSTRING([PatientGenusID],CHARINDEX('|',[PatientGenusID])+2,LEN([PatientGenusID])) ))) as CMC_ID
  --    ,[ProviderRegistryID]
  --    ,[PatientRelationshipType]
  --FROM [ETL_PROD].[dbo].[CMC_Relationship]a																			-- this table lists all legitimate relationships made wit hthe care plan
  --  	  inner join [ETL_PROD].[dbo].[CMC_Organization]d with (nolock) on d.PDRegistryID  = a.OrganizationRegistryID
		--  inner join [ETL_PROD].[dbo].[CMC_OrganizationName]e with (nolock) on e.ItemID = d.name
 
		--  left join [ETL_Local_PROD].[dbo].[AT_ODS_Data]g on g.[Organisation Code] = ODSCode

		--  where a.PatientGenusID = 'PS||100054655'
		--  and Active = 1

     --select top 500  * from  #AT_Patient_NumbersNotPub
 
  -- select top 500  * from [ETL_Local_PROD].[dbo].[AT_Patient_General] where cmc_id = 100054655
 
 	-- select * FROM [ETL_Local_PROD].[dbo].[AuditPatient_New] where cmc_id = 100054655   order by OverallOrder








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




	    if OBJECT_ID ('Tempdb..#UnpublishedPLans') is not null
		drop table #UnpublishedPLans
		select

		a.CMC_ID,
		coalesce(d.NHS_Number,dd.nhs_number) as NHS_Number,
		dd.FullName,
		convert(Date,dd.BirthDateTime) as DOB,
	 
		c.actionTime,
		c.DerivedActionType,
		--c. ,
		row_number() over (partition by a.[CMC_ID] order by LAstActionOrder)  as LastActionOverAllOrder, 
		case  when c.DerivedActionType in ('create','publish') then  row_number() over (partition by a.[CMC_ID],c.DerivedActionType order by LAstActionOrder) else null end  as CreatePublishOrder, 
		case when b.LastActionOVerAll is not null then 1 else null end  as LastActionFlag,
		f.Description as WithdrawnReason, 
		c.StaffFullName,
		c.StaffUserId,
		c.StaffProviderTypeDescription,
		c.Team,
		c.OrgCCG as TeamCCG,
		c.TeamType,
		c.App,
		c.[Parent Org] as TeamParentOrg 

		into #UnpublishedPLans
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
		left join #AT_Patient_NumbersNotPub dd on dd.CMC_id = a.CMC_ID
		left join [ETL_PROD].[dbo].[CMC_ConsentWithdrawn]e on e.CMCID = a.CMC_ID
		left join [ETL_PROD].[dbo].[Coded_DeleteType]f on f.code = e.DeleteType
		left join AT_Staff g on g.StaffRegistryId = c.StaffRegistryId




		--select * FROM #UnPublished  where cmc_id = 100075116 order by LAstActionOrder
		--select * from [ETL_Local_PROD].[dbo].[AT_CarePlanData]where CMC_ID = 100075116

		--select * from [ETL_Local_PROD].[dbo].[AT_Patient_General] where CMC_ID = 100075116

		--select * from [ETL_Local_PROD].[dbo].[AT_UnpublishedPLans]where LastActionFlag = 1 and DerivedActionType = 'discard'

		--select * from [ETL_Local_PROD].[dbo].[AT_UnpublishedPLans]where CMC_ID = 100041536


		 --select * from  [ETL_Local_PROD].[dbo].[AT_CarePlanVersion] where GenusId =  100075116
	 
	 --select top 5 * from [ETL_Local_PROD].[dbo].[AT_Staff]
	 
	--select * from [ETL_Local_PROD].[dbo].[AT_UnpublishedPLans] where CMC_ID =  100002625 -- 
	
	
	if OBJECT_ID ('Tempdb..#DEleted') is not null
		drop table #DEleted
select
*
into #DEleted
from
(

select 
* 
from #UnpublishedPLans
where cmc_ID in (select CMC_id from #UnpublishedPLans where   DerivedActionType  in ('delete'))
--and LastActionFlag = 1

) d where d.LastActionFlag = 1

order by cmc_id, LastActionOverAllOrder


	if OBJECT_ID ('Tempdb..#NeverPublished') is not null
		drop table #NeverPublished

 
			select
			 *
			 into #NeverPublished
			from
			(
				 select 
				 * 
				 from #UnpublishedPLans
				 where CMC_ID not in (select CMC_id from #UnpublishedPLans where   DerivedActionType  in ('publish'))
				 --and LastActionFlag = 1
				 and WithdrawnReason is null
				 --and actionTime >= '2019-04-01'
			)d
			where d.CMC_ID not in	(
									(select CMC_id from #DEleted)
									)
					order by cmc_id, LastActionOverAllOrder
		




 


  	   if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_UnpublishedPLans]') is not null
		drop table [ETL_Local_PROD].[dbo].[AT_UnpublishedPLans]

		select
		a.CMC_ID,
		a.NHS_Number,
		a.FullName as [Patient Name],
		a.DOB as [Patient DOB],
		a.team as [Created by Team],
		a.TeamType as [Created by TeamType],
		a.TeamCCG as [Created by TEam CCG],
		convert(date,a.actionTime) as [Date Care Plan Created],
		a.staffFullName as [Created By],
		a.StaffProviderTypeDescription as [Created Staff-Type],
		case when c.CMC_ID is not null then 1 else null end as 'NEver Published Flag'
 

		into [ETL_Local_PROD].[dbo].[AT_UnpublishedPLans]

		from #UnpublishedPLans a
		left join #DEleted b on b.CMC_ID = a.CMC_ID
		left join #NeverPublished c on c.CMC_ID = a.CMC_ID and c.LastActionFlag = 1

		where 
		a.DerivedActionType = 'create'
		and a.WithdrawnReason is null
		and b.CMC_ID is null


 --and a.cmc_id = 100001018
 

  	--select * FROM  #UnpublishedPLans  where cmc_id = 100001018 order by LastActionOverAllOrder
 	--select * FROM [ETL_Local_PROD].[dbo].[AT_UnpublishedPLans]  where cmc_id = 100001018 order by LastActionOverAllOrder








	--order by a.CMC_ID, LastActionOverAllOrder




SELECT TOP (1000) [ItemId]
      ,[Screen]
      ,[ConsumerId]
      ,[LastSaved]
      ,[LastSavedUser]
      ,[LastPublished]
      ,[LastPublishedUser]
      ,[TimeModified]
  FROM [ETL_PROD].[dbo].[CMC_LastScreenAction]



				select * FROM [ETL_Local_PROD].[dbo].[AT_CarePlanData] where cmc_id = 100001853 order by OverAllOrder
		 select 
		 a.*,
		 b.StaffForename+' '+ b.StaffSurname as StaffName,
		 b.StaffUserId,
		 b.StaffProviderTypeDescription
		 FROM [ETL_Local_PROD].[dbo].[AuditPatient_New] a
		 left join [ETL_Local_PROD].[dbo].[AT_Staff]b on b.StaffRegistryId = a.StaffRegistryId
		 where cmc_id = 100001853   
		 order by OverAllOrder


		 	 select top 5 * from [ETL_Local_PROD].[dbo].[AT_Staff]

	select * FROM [ETL_Local_PROD].[dbo].[AT_UnpublishedPLans]  where cmc_id = 100001018 order by LastActionOverAllOrder


	
 select * from #NeverPublished



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

 