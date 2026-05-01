 

	
		
	 IF OBJECT_ID('tempdb..#TempOrg') IS NOT NULL 
		dROP TABLE #TempOrg
		select 
		distinct
		cast(null as varchar(300)) as  ParentOrg,
		cast(null as varchar(300)) as  Team,
		ROW_NUMBER() OVER(partition by Actor,ActionTime,RegistryRoles,ActionType ORDER BY ItemId,ActionTime)  as RNumber,
		ActionType,
		ActionTime,
		MRNs,
		Actor,
		REGISTRYRoles,
		---- add stripped actor for performance MS 21.5.16
		case 
		when left(Ltrim(Actor),19) = 'AvailabilityGeneral' then right(rtrim(actor),3)
		when left(Ltrim(Actor),8) = 'AvailGen' then rtrim(REPLACE(Actor,'AvailGen','')) 
		else rtrim(REPLACE(Actor,'EMISAvail','')) 
		end as ODS
		into #TempORg	
		from etl_PROD.dbo.cmc_auditdata a with (nolock)
 
		where ActionType = 'SearchPatient'
		and charindex('avail',a.Actor)>0
 
		and convert(date,a.actionTime) >= '2020-04-01'



			select top 50 * from etl_PROD.dbo.cmc_auditdata	where ActionType = 'SearchPatient'
		and charindex('avail',Actor)>0

		--select * from #TempORg where team = 'IMPERIAL COLLEGE HEALTHCARE NHS TRUST' order by actiontime

		delete   from #TempORg where RNumber > 1

		select DISTINCT TEAM,ODS from #TempORg WHERE ParentOrg IS NULL
 
		
		update f
			set f.ODS = 	rtrim(REPLACE(ODS,'AvailGen','')) 	
		from #TempORg f
		where left(Ltrim(ODS),8) = 'AvailGen' 

		b 

		update r
		set r.Team = b.[Team or GP],
			r.ParentOrg = ltrim(rtrim(REplace(b.Parent, ' (CCG)', ''))) 
		from #TempORg r

	  inner join [ETL_Local_PROD].[ODSData].[searchods]b on b.ODS = r.ods	and b.Type = 'General Medical Practice'

	  	update r
		set r.Team = b.[Team or GP],
		    r.ParentOrg = case when b.Parent is null and b.type = 'NHS Trust' then r.Team else
							ltrim(
									rtrim(
											replace(REplace(b.Parent, ' (CCG)', ''),' (NHS Trust)','')
										  )
								) 
								end
		from #TempORg r
	   left join [ETL_Local_PROD].[ODSData].[searchods]b on b.ODS = r.ods	 
	  where r.Team is null

 
	   
  SELECT   [Type]
      ,[Team or GP]
      ,[Parent]
      ,[GP CCG]
      ,[ODS]
      ,[Address]
      ,[Postcode]
      ,[Open Date]
      ,[Close Date]
      ,[Join Parent Date]
      ,[Left Parent Date]
      ,[ParentODS]
      ,[ParentTeam]
      ,[ParentType]
      ,[Telephone]
  FROM [ETL_Local_PROD].[ODSData].[searchods]
  where charindex('RJ1',[ODS])>0