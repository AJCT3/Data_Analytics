select * from OPENQUERY (CMCMIRROR, 'SELECT top 10 ID, EventDateTime, LocalDateTime, UserName, EventCodes, SourceURI, Roles, MRNs, EventType FROM HS_IHE_ATNA_Repository.Aggregation where ID = 1707805701 order by ID desc')


--R94!fh$Ep

select top 10 * from ETL_PROD.dbo.CMC_AuditData where ItemId = 'Registry1707805701'

select 
* from OPENQUERY 
(
CMCMIRROR, 
'
SELECT top 10 ID, 
''Registry'', 
EventDateTime, 
LocalDateTime, 
UserName, 
EventCodes, 
SourceURI, 
Roles, 
MRNs, 
EventType 
FROM HS_IHE_ATNA_Repository.Aggregation 

where ID = 1707805701 
order by ID desc'
)

--if OBJECT_ID ('[ETL_Local_PROD].[dbo].[ServiceSearch_TEST]') is not null
--			drop table [ETL_Local_PROD].[dbo].[ServiceSearch_TEST]
select 
* 
--into [ETL_Local_PROD].[dbo].[ServiceSearch_TEST]
from OPENQUERY 
(
CMCMIRROR, 
'SELECT 
top 10000 ID, 
EventDateTime, 
LocalDateTime, 
UserName, 
EventCodes, 
SourceURI, 
Roles, 
MRNs, 
EventType 
FROM HS_IHE_ATNA_Repository.Aggregation 
 
order by ID desc'
)

select top 10 * from [ETL_PROD].[dbo].[CMC_AuditData] where ItemId = 'Registry1707805701'
select top 10 * from [ETL_Local_PROD].[dbo].[ServiceSearch]
select * from [ETL_Local_PROD].[dbo].[ServiceSearch_LastDateStamp]




				 if OBJECT_ID ('Tempdb..#MaxServiceSearchId') is not null
					drop table #MaxServiceSearchId
					select convert( int,max(RowItemID )) as LastRow
					into #MaxServiceSearchId
					from [ETL_Local_PROD].[dbo].[ServiceSearch]

					--declare @LastRowID  numeric(18,0)
					--set @LastRowID = (select lastrow from #MaxServiceSearchId)


			if OBJECT_ID ('Tempdb..#REgHolder') is not null
			drop table #REgHolder 
			select * into #REgHolder from #REgHolder1 where 1=2



			DECLARE @TSQL varchar(8000), @LastRowID  varchar(36)
			set @LastRowID = (select convert(varchar(36),lastrow) from #MaxServiceSearchId)
			print @LastRowID
 
			SELECT @TSQL = 
			'
			select 
			* 
			
			from OPENQUERY 
			(
			CMCMIRROR, 
			''SELECT 
			top 1000000 ID, 
			EventDateTime, 
			LocalDateTime, 
			UserName, 
			EventCodes, 
			SourceURI, 
			Roles, 
			MRNs, 
			EventType 
			FROM HS_IHE_ATNA_Repository.Aggregation 
			where id >'   + @LastRowID + '
			order by ID desc''
			)

			'
		insert into #REgHolder  EXEC (@TSQL)



		  select * from #REgHolder
		  select lastrow from #MaxServiceSearchId
		  select * from [ETL_Local_PROD].[dbo].[ServiceSearch_LastDateStamp]

				if OBJECT_ID ('Tempdb..#REgHolder') is not null
			drop table #REgHolder 
			select 
			* 
			into #REgHolder
			from OPENQUERY 
			(
			CMCMIRROR, 
			'SELECT top 150000 ID, 
			EventDateTime, 
			LocalDateTime, 
			UserName, 
			EventCodes, 
			SourceURI, 
			Roles, 
			MRNs, 
			EventType 
			FROM HS_IHE_ATNA_Repository.Aggregation 
			 where EventDateTime >= ''2021-09-01 13:50:05.000''
	
			order by ID asc'
			)

			

		--select * from  #REgHolder order by eventdatetime desc
		select count(*) from #REgHolder

		--truncate table [ETL_Local_PROD].[dbo].[ServiceSearch]
		insert into [ETL_Local_PROD].[dbo].[ServiceSearch]

		select 
		CAST(ID as numeric(18,0)) as RowItemID,
		null as OverAllOrder,
		'Registry' + Cast(ID as varchar(36))as ItemId,
		EventDateTime as ActionTime,
		case when MRNS is not null then 1 else 0 end as MRNs,
		UserName as Actor,
		Roles as REGISTRYRoles,
		---- add stripped actor for performance MS 21.5.16
		 ltrim(rtrim(REPLACE(REPLACE(replace(replace(UserName,'AvailabilityGeneralRoyalFree',''),'AvailabilityGeneralMarsden',''),'AvailGen',''),'EMISAvail',''))) as ODS
 
		from #REgHolder a
		
		where EventType = 'SearchPatient'
		and Roles like '%HSCC_Service_Search%'
		and not exists (select RowitemID from  [ETL_Local_PROD].[dbo].[ServiceSearch] where rowitemid = a.id )
		--and convert(date,EventDateTime)  >=  (select convert(date,lastDate) from [ETL_Local_PROD].[dbo].[ServiceSearch_LastDateStamp])

		--select * from [ETL_Local_PROD].[dbo].[ServiceSearch] order by ActionTime desc



select 
* 
from OPENQUERY 
(
CMCMIRROR, 
'SELECT 
top 10 ID, 
''Registry'', 
EventDateTime, 
LocalDateTime, 
UserName, 
EventCodes, 
SourceURI, 
Roles, 
MRNs, 
EventType 
FROM HS_IHE_ATNA_Repository.Aggregation 
where ID = 1707805701 
order by ID desc'
)