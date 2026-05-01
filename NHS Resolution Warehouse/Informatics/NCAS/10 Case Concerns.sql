USE [Informatics_Reporting]
GO
/****** Object:  StoredProcedure [NCAS].[05_Create_Suspensions_Exclusions_Base_Data]    Script Date: 29/08/2018 17:03:16 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



alter PROCEDURE  [NCAS].[10_Create_Case_Concern_Data]
 

	AS

	SET NOCOUNT ON


	If object_ID('tempdb..#Temp1') is not null
	drop table #Temp1
	  select 
	  distinct
	  ROW_NUMBER() OVER (partition by x.caseid ORDER BY  x.caseid,bb.Description ) as Rowid,
	  x.caseid,
	  x.CaseConcernId,
	  bb.Description as ConcernSectionDesc,
 
	   case when c.ConcernGroupId = 20 then 'Identify misconduct' else d.[Summary Concern Detail Item]  end as ConcernGroup,
	  c.DisplayText as [Sub Concern Group],
 
	   b.DisplayText as ConCernItemDesc,
	  case 
	  when a.State1 = 1 and c.ConcernGroupId = 20 then 'At Work'
	  when a.State1 = 1 and c.ConcernGroupId <> 20 then 'Now'
	  when a.State2 = 1 and c.ConcernGroupId = 20 then 'Outside work'
	  when a.State2 = 1 and c.ConcernGroupId <> 20 then 'Being Considered'
	  else null end as State

 
	 into #Temp1
	 FROM [EKS_Archive2].[Core].[CaseConcern]x
	 inner join [EKS_Archive2].[Core].[CaseConcernItem]a on a.CaseConcernId = x.CaseConcernId
	  inner join [EKS_Archive2].[Core].[CaseConcernSection]aa on aa.CaseConcernId = a.CaseConcernId and aa.State = 1
	  inner join [EKS_Archive2].[Core].[ConcernSection]bb on bb.ConcernSectionId = aa.SectionId 
	  inner join [EKS_Archive2].[Core].[ConcernItem]b on b.ConcernItemId = a.ItemId
	  inner join [EKS_Archive2].[Core].[ConcernGroup]c on c.ConcernGroupId = b.ConcernGroupId   and c.ConcernSectionId = aa.SectionId 
		left join
				(

				SELECT [CaseId]
		  ,[Description] as [Summary Concern]
		  ,[DisplayText] as [Summary Concern Detail Item] 
 
  
	  FROM [EKS_Archive2].[Rpt].[CaseConcernReportHeader]
	   where 
 
	   state <> -1
	   and [Description] <> 'What actions are being taken locally'




				)d on d.CaseId = x.CaseId
					and d.[Summary Concern] = bb.Description
	  where 
 
	  (State1 = 1 OR State2 = 1)
	  and x.SnapshotDate is null
	  and bb.Description <> 'What actions are being taken locally'

  	
	If object_ID('tempdb..#Temp2') is not null
	drop table #Temp2

	select
	distinct
	null as Rowid,
	Caseid,
	ConcernSectionDesc
	into #Temp2
	from #Temp1


	--select * from #Temp2


	update g
			set g.Rowid = b.Rowid
	from [#Temp2]g
	left join
			(
			select
			ROW_NUMBER() OVER (partition by caseid ORDER BY   caseid,ConcernSectionDesc ) as Rowid,
			Caseid,
			ConcernSectionDesc
			from #Temp2
			) b on b.CaseId = g.caseid 
			and b.ConcernSectionDesc = g.ConcernSectionDesc


  	
	If object_ID('tempdb..#Temp3') is not null
	drop table #Temp3

	select
	distinct
	null as Rowid,
	b.Rowid as ConcernSectionRowID,
	a.Caseid,
	ConCernItemDesc
	into #Temp3
	from #Temp1 a
	left join [#Temp2]b on b.CaseId = a.CaseId and b.ConcernSectionDesc = a.ConcernSectionDesc
	order by ConCernItemDesc

 

	update g
			set g.Rowid = b.Rowid
	from [#Temp3]g
	left join
			(
			select
			ROW_NUMBER() OVER (partition by caseid  ORDER BY   caseid,ConcernSectionRowID,ConCernItemDesc ) as Rowid,
			Caseid,
			ConCernItemDesc,
			ConcernSectionRowID
			from #Temp3
 
			) b on b.CaseId = g.caseid 
			and b.ConCernItemDesc = g.ConCernItemDesc
			and b.ConcernSectionRowID = g.ConcernSectionRowID

	 	IF OBJECT_ID('[Informatics_Reporting].[NCAS].[10_CaseConcerns_Summary]') IS NOT NULL 
		   dROP TABLE [Informatics_Reporting].[NCAS].[10_CaseConcerns_Summary]

	;WITH CTE_Concatenated1 AS
		(
		SELECT  
		RowID 
	   ,caseid  

	 ,cast(cast(RowID as varchar(2))+') '+  ConcernSectionDesc  as varchar(800))  as ConcernSectionDesc

		FROM    [#Temp2]a
		WHERE   RowID = 1
		UNION ALL
		SELECT  b. RowID
				,b.caseid  

			 ,cast(a.ConcernSectionDesc +' '+ cast((a.RowID+1) as varchar(2))+') '+   b.ConcernSectionDesc as varchar(800))  
		FROM    CTE_Concatenated1 a
		JOIN    [#Temp2]b
		ON      b. RowID = cast(a.RowID as int) + 1
		and b.CaseId = a.CaseId
 
	
		)
 
		,
 
	CTE_Concatenated2 AS
		(
		SELECT  
		RowID 
	   ,caseid  
	 ,cast( '1) '+  ConCernItemDesc  as varchar(max))  as ConCernItemDesc

		FROM    [#Temp3]a
		WHERE   RowID = 1
		UNION ALL
		SELECT  b. RowID
				,b.caseid  
			 ,cast(a.ConCernItemDesc +'; '+ 
		 
											case 
												when b.ConcernSectionRowID = (select ConcernSectionRowID from [#Temp3]x where x.caseid = b.caseid and x.rowid = a.Rowid ) 
													then ''
													else cast((b.ConcernSectionRowID ) as varchar(2))+') ' end
													 +   b.ConCernItemDesc as varchar(max))  

		FROM    CTE_Concatenated2 a
		JOIN    [#Temp3]b
		ON      b. RowID = cast(a.RowID as int) + 1
		and b.CaseId = a.CaseId
	
	
		)
  

	 select
	a.Caseid,
	ConcernSectionDesc as SummaryConcernSection,
	b.ConCernItemDesc as SectionDetail

	into [Informatics_Reporting].[NCAS].[10_CaseConcerns_Summary]

	 from CTE_Concatenated1 a
	 left join
			(
			select 
			caseid,
			ConCernItemDesc
			FROM    CTE_Concatenated2
			WHERE   RowID = (SELECT MAX(RowID) FROM [#Temp3] where caseid = CTE_Concatenated2.caseid ) 
			) b on b.CaseId = a.CaseId
	 WHERE  a.RowID = (SELECT MAX(RowID) FROM [#Temp2] where caseid = a.CaseId) 




	 	IF OBJECT_ID('[Informatics_Reporting].[NCAS].[10_CaseConcerns_All_Data]') IS NOT NULL 
		   dROP TABLE [Informatics_Reporting].[NCAS].[10_CaseConcerns_All_Data]

		   select
		   Rowid as RowNumber,
		   CaseID,
		   ConcernSectionDesc,
		   ConcernGroup,
		   [Sub Concern Group],
		   ConCernItemDesc,
		   State

		   into [Informatics_Reporting].[NCAS].[10_CaseConcerns_All_Data]

		   from #Temp1
		   order by caseid, Rowid

--select * from [EKS_Archive 1].[Core].[10_CaseConcerns_All_Data]order by caseid, RowNumber
--select * from  [Informatics_Reporting].[NCAS].[10_CaseConcerns_Summary]

--select * from [Informatics_Reporting].[NCAS].[10_CaseConcerns_All_Data]order by caseid, RowNumber


  	IF OBJECT_ID('[Informatics_Reporting].[NCAS].[10_CaseConcerns_All_Data_Main_Concerns]') IS NOT NULL 
	drop table [Informatics_Reporting].[NCAS].[10_CaseConcerns_All_Data_Main_Concerns]
	  
	  
	  declare @ColumnNames  Nvarchar(Max) = '' 
	  Declare @RoleName Table (Rolename  Varchar(200) not null)
	  Declare @SQL2 NVarchar(Max) = ''
	  insert into @RoleName  select distinct ConcernSectionDesc from [Informatics_Reporting].[NCAS].[10_CaseConcerns_All_Data]
							 					


		select @ColumnNames  +=   QuoteName(RoleName) +',' from @RoleName
		set @ColumnNames = LEft(@ColumnNames,Len(@ColumnNames)-1)
		print @ColumnNames

		set @SQL2 =

		'
 
	
	
		SELECT
		*
		into [Informatics_Reporting].[NCAS].[10_CaseConcerns_All_Data_Main_Concerns]
			FROM
			(
					select
					distinct
					caseid,
					ConcernSectionDesc
					from
					(
					select * from [Informatics_Reporting].[NCAS].[10_CaseConcerns_All_Data]
					)d

			) AS P
		PIVOT
		(
			count(ConcernSectionDesc) FOR ConcernSectionDesc IN ('+ @ColumnNames + ')
			) AS pv '

			EXEC SP_Executesql @SQL2

	 --select * from [Informatics_Reporting].[NCAS].[10_CaseConcerns_All_Data_Main_Concerns] where caseid = 18377
	 --select * from [Informatics_Reporting].[NCAS].[10_CaseConcerns_All_Data]where caseid = 18377

 
