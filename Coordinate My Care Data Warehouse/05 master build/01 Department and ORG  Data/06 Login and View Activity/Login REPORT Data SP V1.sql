 
 
 IF OBJECT_ID('tempdb..#TempOrg') IS NOT NULL 
	dROP TABLE #TempOrg
 
 SELECT 
  
		distinct
		 null as RowNumber
		, Cast(null as varchar(max)) as ParentORg
		,[StaffUserId]
      ,[StaffEnterpriseId]
      ,[StaffRegistryId]
      ,[StaffName]
      ,a.[DeptName] as OldName
	  ,case
	 when a.[DeptName] = 'ST GEORGES UNIVERSITY HOSPITALS EMERGENCY DEPARTMENT' then 1
	 when a.[DeptName] = 'BARKING, HAVERING AND REDBRIDGE UNIVERSITY HOSPITALS NHS TRUST' then 1
	 when a.[DeptName] = 'CNWL Community Independence Service K and C' then 1
	 when a.[DeptName] = 'ROYAL BROMPTON & HAREFIELD NHS FOUNDATION TRUST'then 1
	 when a.[DeptName] in ('LCW','LCW OOH GPs') then 1
	 when a.[DeptName] = 'MARIE CURIE CANCER CARE' then 1
	 when a.[DeptName] = 'MICHAEL SOBELL HOUSE/MVCC' then 1
	 when a.[DeptName] in ('ST JOSEPHS HOSPICE ( )','ST JOSEPHS HOSPICE (DO NOT USE)','ST JOSEPHS HOSPICE') then 1
	 when a.[DeptName] = 'ST LUKES HOSPICE CMC USER (HARROW)' then 1
	 when a.[DeptName] = 'TRINITY HOSPICE - CMC User' then 1
	 when a.[DeptName] = 'The Pines Care Home with Nursing' then 1
	 when a.[DeptName] in ('UNIVERSITY COLLEGE HOSPITAL EMERGENCY DEPARTMENT','University College London Hospital A & E Dept') then 1
	 when a.[DeptName] = 'SOUTH LONDON DOCTORS URGENT CARE' then 1
	 when a.[DeptName] = '111 LAS' then 1
	 when a.[DeptName] = 'LONDON CENTRAL AND WEST UNSCHEDULED CARE COLLABORATIVE' then 1
	 else Null
 end as  OldNameFlag
	   ,case
	 when a.[DeptName] = 'ST GEORGES UNIVERSITY HOSPITALS EMERGENCY DEPARTMENT' then 'ST GEORGE''S UNIVERSITY HOSPITALS EMERGENCY DEPARTMENT'
	 when a.[DeptName] = 'BARKING, HAVERING AND REDBRIDGE UNIVERSITY HOSPITALS NHS TRUST' then 'BARKING HAVERING AND REDBRIDGE UNIVERSITY HOSPITALS NHS TRUST'
	 when a.[DeptName] = 'CNWL Community Independence Service K and C' then 'CNWL Community Independence Service K & C'
	 when a.[DeptName] = 'ROYAL BROMPTON & HAREFIELD NHS FOUNDATION TRUST'then 'ROYAL BROMPTON and HAREFIELD NHS FOUNDATION TRUST' 
	 when a.[DeptName] in ('LCW','LCW OOH GPs') then 'LCW 111'
	 when a.[DeptName] = 'MARIE CURIE CANCER CARE' then 'MARIE CURIE'
	 when a.[DeptName] = 'MICHAEL SOBELL HOUSE/MVCC' then 'MICHAEL SOBELL HOUSE HOSPICE'
	 when a.[DeptName] in ('ST JOSEPHS HOSPICE ( )','ST JOSEPHS HOSPICE (DO NOT USE)','ST JOSEPHS HOSPICE') then 'ST.JOSEPH''S HOSPICE'
	 when a.[DeptName] = 'ST LUKES HOSPICE CMC USER (HARROW)' then 'ST LUKES HOSPICE (HARROW)'
	 when a.[DeptName] = 'TRINITY HOSPICE - CMC User' then 'THE ROYAL TRINITY HOSPICE - CMC User'
	 when a.[DeptName] = 'The Pines Care Home with Nursing' then 'THE PINES NURSING HOME'
	 when a.[DeptName] in ('UNIVERSITY COLLEGE HOSPITAL EMERGENCY DEPARTMENT','University College London Hospital A & E Dept') then 'UCLH EMERGENCY DEPARTMENT'
	 when a.[DeptName] = 'SOUTH LONDON DOCTORS URGENT CARE' then 'South London Doctors Urgent Care 111'
	 when a.[DeptName] in ('111 LAS') then '111 LAS SEL CAS'
	 when a.[DeptName] = 'LONDON CENTRAL AND WEST UNSCHEDULED CARE COLLABORATIVE' then 'LCW 111'
	 else a.[DeptName] 
 end as  DeptName
 ,coalesce(
		case 
		--When TeamType in ('A&E','999 PROVIDER','111 PROVIDER','OUT OF HOURS') Then 'URGENT CARE'
		when (charindex('mental',TeamType)>0 or charindex('Community',TeamType)>0)  then 'COMMUNITY PROVIDER'
		when TeamType is null and charindex('CCG',[Parent Org]) > 0 and  charindex('GP Practices',a.deptname)> 0 then 'PRIMARY CARE'
		When TeamType = 'UNKNOWN' then 'Not recorded'
			else TeamType
			end,'Not recorded'
		)   as [OrganizationTypeDescription]
      ,a.[DeptODSCode]
      ,case
	 when a.[DeptName] = 'ST GEORGES UNIVERSITY HOSPITALS EMERGENCY DEPARTMENT' then 100133407
	 when a.[DeptName] = 'BARKING, HAVERING AND REDBRIDGE UNIVERSITY HOSPITALS NHS TRUST' then 100072787
	 when a.[DeptName] = 'CNWL Community Independence Service K and C' then 100099203
	 when a.[DeptName] = 'ROYAL BROMPTON & HAREFIELD NHS FOUNDATION TRUST'then 100072833
	 when a.[DeptName] in ('LCW','LCW OOH GPs') then 100099168
	 when a.[DeptName] = 'MARIE CURIE CANCER CARE' then 100043269
	 when a.[DeptName] in ('ST JOSEPHS HOSPICE ( )','ST JOSEPHS HOSPICE (DO NOT USE)','ST JOSEPHS HOSPICE') then 100064004
	 when a.[DeptName] = 'The Pines Care Home with Nursing' then 100022323
	 when a.[DeptName] in ('UNIVERSITY COLLEGE HOSPITAL EMERGENCY DEPARTMENT','University College London Hospital A & E Dept') then 100099190

	 else a.deptenterpriseid
 end as   deptenterpriseid
      ,a.[DeptPDRegistryID]
      ,[Created]
      ,[Removed]
      ,[CMCRoleDescription]
      ,[StaffEmail]
      ,[StaffJobTitle]
      ,[StaffActiveDescription]
		,ltrim(rtrim(REplace( d.[GP CCG] , ' CCG', '')) ) as CCG
		,ltrim(rtrim(REplace(coalesce(b.[Team specified ccg],e.ccg), ' CCG', '')) ) as OriginalCCG
		,e.[NHS England REgion] as  [NHS Region]

		,coalesce(c.DeptODSCode,a.DeptODSCode) as ODSCode
		,d.PostCode
		,Cast(null as Varchar(300)) as PCNName
		,Cast(null as Varchar(300)) as Ward
		,cast(null as date) as CloseDate


	  into #TempOrg

  FROM [ETL_Local_PROD].[dbo].[Logins]a
  inner join [ETL_Local_PROD].[dbo].[AT_Commissioner_Team_Org_Lookup]b on b.ActivityTeam = a.DeptName
  inner join [ETL_Local_PROD].[dbo].[AT_PD_Dept]c on c.DeptEnterpriseID = a.deptenterpriseid
left join [ETL_Local_PROD].[ODSData].[searchods]d on d.ODS = c.DeptODSCode  
 left join  [ETL_Local_PROD].[dbo].[AT_ENGLISH_POSTCODE_DATA]e on e.PCDS = d.Postcode

  --where Removed is null
 

/**
 
 
 **/


  update a
		set a.ParentORg = b.[Parent Org]
			
 from #tempOrg a
 inner join [ETL_Local_PROD].[dbo].[AT_Commissioner_Team_Org_Lookup]b on b.ActivityTeam = a.DeptName


 update f
		set f.CloseDate = coalesce(g.[Close Date],c.DEptclosedate)
 from #TempOrg f
 left join [ETL_Local_PROD].[ODSData].[searchods]g on g.ODS = f.ODSCode
 inner join  [ETL_Local_PROD].[dbo].[AT_PD_Dept]c on c.DeptEnterpriseID = f.deptenterpriseid
 
  
update r
		set DeptODSCode = 'DL0'

from #TempOrg r
where DeptODSCode in ( 'F84712','RQX44')

		delete from #TempOrg where DeptName = 'THE GOLBORNE MEDICAL CENTRE - DATHI' and ODSCode is null
		delete from #TempOrg where DeptName = 'HOMERTON UNIVERSITY HOSPITAL GP OOH' and ODSCode is null
 
		delete from #TempOrg where DeptName = 'YOUR HEALTHCARE' and ODSCode is null
		delete from #TempOrg where DeptName = 'LCW 111' and PostCode is null
		delete from #TempOrg where DeptName in ('111 HARMONI<CARE UK>','CARE UK - CMC USER')
		 --select * from #TempOrg where   ODSCode ='F81062'
		  --select * from #TempOrg where  charindex('chelsea',DeptName)>0
		 --select top 5*   from  [ETL_Local_PROD].[dbo].[AT_Login_Details] where  charindex('chigwell',OrganizationName)>0
		 -- select *  from [ETL_Local_PROD].[dbo].[AT_PD_Dept] where  charindex('chigwell',DeptName)>0
		 --		 select top 5*   from  [ETL_Local_PROD].[dbo].[AT_Login_Details] where  deptenterpriseid =  100062440


 update g
		set g.ParentORg = g.DeptName
 from #TempOrg g
 Where ParentORg is null


delete a from #TempOrg a left join [ETL_Local_PROD].[dbo].[AT_ENGLISH_POSTCODE_DATA]b on b.PCDS = a.Postcode where b.[NHS England REgion] <> 'London'




 update r

		set r.RowNumber = g.RNumber

 from #TempOrg r
 left join
	 
		(
		select
		DeptName,
		ROW_NUMBER() OVER(partition by DeptName,CCG ORDER BY OldNameFlag,closedate)  as RNumber,
		OldName,
		ODSCode,
		deptenterpriseid,
		CloseDate
		from #TempOrg
		)g  on g.DeptName = r.DeptName
		and g.ODSCode = r.ODSCode
		/**


 /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
 \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

		 --select *  from #TempOrg where RowNumber > 1

		 update r
				set DeptName = DeptName +' '+ PostCode

		 from #TempOrg r
		 where rowNumber >1 

 /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
 \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

 **/


 update r
				
				set r.PCNName = s.PCNName

				from #TempOrg r
				left join  [ETL_Local_PROD].[dbo].[AT_Commissioners_Report_Primary_Care_Network_Core_Partners]s on s.PartnerOrganisationCode = r.ODSCode



				update r
				
				set r.ODSCode = t.ODS

				from #TempOrg r

				left join [ETL_Local_PROD].[ODSData].[searchods]t on t.Postcode = r.PostCode
																
				WHere r.ODSCode is null
		 

		 update r
				
				set r.PCNName = coalesce(s.PCNName,'Unknown')

				from #TempOrg r
				left join  [ETL_Local_PROD].[dbo].[AT_Commissioners_Report_Primary_Care_Network_Core_Partners]s on s.PartnerOrganisationCode = r.ODSCode
				WHere r.PCNName is null

 
			update war

					set war.Ward = b.[Health Authority]

			from #TempOrg war
			left join [ETL_Local_PROD].[dbo].[AT_ENGLISH_POSTCODE_DATA]b on b.PCDS = war.Postcode


			--delete from #TempORg where StaffActiveDescription in ('closed','Retired')


			select top 10 * from #TempOrg where DeptName = 'IMPERIAL COLLEGE HEALTHCARE  CHARING CROSS A and E DEPT'
			select top 10 * from [ETL_Local_PROD].[dbo].[AT_TEam_Activity_Report_Data_Table] where team = 'IMPERIAL COLLEGE HEALTHCARE  CHARING CROSS A and E DEPT'
			select top 10 * from #TempOrg where charindex('( DON NOT USE ) Canino',StaffName)>0
			 
			 
				SELECT
				  SUBSTRING(StaffName, 1, CHARINDEX(' ', StaffName) - 1) AS Forename,
				  SUBSTRING(StaffName, CHARINDEX(' ', StaffName) + 1, LEN(StaffName)) AS Surname,
				  ParentORg as [Organisation/Trust Name],
				  a.DeptName as [Current Team Name],
				  cast(null as varchar(255)) as [ORGANISATION SUB-GROUP],
				  cast(null as varchar(255)) as [ORGANISATION SUB-SUB GROUP],
				  cast(null as varchar(255)) as [TYPE OF SERVICE],

				  b.ODS as [ORGANISATION ODS CODE],
				  StaffEmail as [STAFF WORK EMAIL ADDRESS],
				  cast(null as nvarchar(255)) as [STAFF WORK EMAIL ADDRESS],
				  StaffJobTitle as [JOB TITLE],
				  CMCRoleDEscription as [CURRENT ROLE BASED ACCESS],
				  cast(null as varchar(255))  as [REQUIRED ROLE BASED ACCESS],
				  cast(null as nvarchar(255)) as [TRAINING],
				  cast(null as nvarchar(255)) as [SMARTCARD NUMBER]

				FROM #TempORg a
				left join  [ETL_Local_PROD].[ODSData].[searchods] b on b.[Team or GP] = a.ParentORg and b.Type = 'NHS Trust'
				where ParentORg in
				(
				'IMPERIAL COLLEGE HEALTHCARE NHS TRUST',
				'LONDON NORTH WEST UNIVERSITY HEALTHCARE NHS TRUST',
				'CHELSEA AND WESTMINSTER HOSPITAL NHS FOUNDATION TRUST'
				)
				and StaffActiveDescription not in ('closed','Retired')
				and Removed is  null
				order by ParentORg, [Current Team Name], Surname



			 
				SELECT
				  SUBSTRING(StaffName, 1, CHARINDEX(' ', StaffName) - 1) AS Forename,
				  SUBSTRING(StaffName, CHARINDEX(' ', StaffName) + 1, LEN(StaffName)) AS Surname,
				  StaffEmail as [STAFF WORK EMAIL ADDRESS],
				  Cast(null as varchar(300)) as [COMMENT],
				  ParentORg as [Organisation/Trust Name] 

				FROM #TempORg a
				left join  [ETL_Local_PROD].[ODSData].[searchods] b on b.[Team or GP] = a.ParentORg and b.Type = 'NHS Trust'
				where ParentORg in
				(
				'IMPERIAL COLLEGE HEALTHCARE NHS TRUST',
				'LONDON NORTH WEST UNIVERSITY HEALTHCARE NHS TRUST',
				'CHELSEA AND WESTMINSTER HOSPITAL NHS FOUNDATION TRUST'
				)
				and
				(
				StaffActiveDescription  in ('closed','Retired')
				or 
				Removed is  not null
				)
				order by ParentORg

				--select * from [ETL_Local_PROD].[ODSData].[searchods] where ODS = 'CMC048'
				select StaffJobTitle from
				(
				select distinct StaffJobTitle,COunt(*) as Total FROM #TempORg where staffjobtitle is not null group by StaffJobTitle 
				
				)d
				order by Total desc


						select CMCRoleDEscription from
				(
				select distinct CMCRoleDEscription,COunt(*) as Total FROM #TempORg where CMCRoleDEscription is not null group by CMCRoleDEscription 
				
				)d
				order by Total desc




select 
Distinct
ltrim(rtrim(replace(Parent,'(NHS TRUST)',''))) as [Parent Trust],
[Team or GP] as [Trudt Site],
ODS
--,
--Postcode
from [ETL_Local_PROD].[ODSData].[searchods]a 
left join [ETL_Local_PROD].[dbo].[AT_ENGLISH_POSTCODE_DATA]b on b.PCDS = a.Postcode
where type = 'NHS Trust Site'
and b.[NHS England REgion] = 'London'

 select  * from [ETL_Local_PROD].[dbo].[AT_ENGLISH_POSTCODE_DATA]
 select  *  from [ETL_Local_PROD].[ODSData].[searchods]
 
		DECLARE 
		@EndDate date,  
		@StartDate Date,
		@StartDateThisYear date,
		@MonthEnd date, 
		@MonthStart date,
		@TotalMonths int,
		@MonthCounter int = 0,
		@SnapDateStart Date,
		@SnapDateEnd Date


	set @StartDate = dateadd(year,-2,convert(datetime, cast(year(dateadd(month, -3, getdate())) as varchar(10)) + '-04-01', 120))
	set @StartDateThisYear = convert(datetime, cast(year(dateadd(month, -3, getdate())) as varchar(10)) + '-04-01', 120)
	set @EndDate =  DATEADD(DAY, -(DAY(getdate())), getdate())
	
	set @MonthStart =  dateadd(d,-(day(dateadd(m,-1,getdate()-2))),dateadd(m,-1,getdate()-1)) 
	set @MonthEnd = EOMONTH(@MonthStart)


	 


	IF OBJECT_ID('tempdb..#Temp_CTE_Date') IS NOT NULL 
	dROP TABLE #Temp_CTE_Date



	;WITH cte AS 
	(
	SELECT 
	DATEPART(Day,@StartDateThisYear)as RowNumb,
	CASE WHEN DATEPART(Day,@StartDateThisYear) = 1 THEN @StartDateThisYear 
				ELSE DATEADD(Month,DATEDIFF(Month,0,@StartDateThisYear)+1,0) END AS myDate
	            
	UNION ALL
	SELECT
	RowNumb+1 as RowNumb,--this is a field to be used with the for loop below
	 DATEADD(Month,1,myDate)

	FROM cte
	WHERE DATEADD(Month,1,myDate) <=  @EndDate
	)

	--populate temp table with results so they can be used below...
	SELECT RowNumb,myDate,[Year End Date],[Year Start Date],[Financial Year]
	into #Temp_CTE_Date		
	FROM cte a
	inner join [ETL_Local_PROD].[dbo].[DIM_Date] b on b.[Calendar Day] = convert(date,a.myDate)
	OPTION (MAXRECURSION 0)


			 

 DECLARE 
		@EndDate1 date,  
		@StartDate1 Date,
		@MonthEnd1 date, 
		@MonthStart1 date,
		@TotalMonths1 int,
		@MonthCounter1 int = 0,
		@SnapDateStart1 Date,
		@SnapDateEnd1 Date


	set @StartDate1 = convert(datetime, cast(year(dateadd(month, -3, getdate())) as varchar(10)) + '-04-01', 120)
	set @EndDate1 =  DATEADD(DAY, -(DAY(getdate())), getdate())
	
	set @MonthStart1 =  dateadd(d,-(day(dateadd(m,-1,getdate()-2))),dateadd(m,-1,getdate()-1)) 
	set @MonthEnd1 = EOMONTH(@MonthStart1)



 
	set @TotalMonths1 = (select MAX(Rownumb) from #Temp_CTE_Date where myDate < convert(date,DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0))) 

 

		
	WHILE @MonthCounter1 <= @TotalMonths1 BEGIN
		SET @MonthCounter1 = @MonthCounter1 + 1

		set @SnapDateStart1 = (select myDate from  #Temp_CTE_Date where RowNumb = @MonthCounter1)
		set @SnapDateEnd1 = DATEADD(d, -1, DATEADD(m, DATEDIFF(m, 0, @SnapDateStart1) + 1, 0))

		IF OBJECT_ID('tempdb..#Temp33') IS NOT NULL 
		dROP TABLE #Temp33
		select 
		@SnapDateStart1 as ReportingMonth
		,@SnapDateEnd1 as MonthEnd
		,@MonthCounter1 as MonthCounter
		,d.[Financial Year] as ReportingFinYear
	    ,D.Quarter as FinQuarter
		,s.STP
		,ParentORg
		,DeptName
		,[StaffName]
    
		,[Created]
		,[StaffJobTitle]
		,[StaffActiveDescription]
		,x.DeptEnterpriseId
		,x.ODSCode
		,x.Ward as [HEalth Authority]
		,x.OriginalCCG as Burrough
		,case
			when coalesce(x.OrganizationTypeDescription,'Not Recorded') = 'PRIMARY CARE' then x.CCG
			else ''
			end as CCG
		,case
			when coalesce(x.OrganizationTypeDescription,'Not Recorded') = 'PRIMARY CARE' then PCNName
			else ''
			end as [Primary Care Network]
		,coalesce(x.OrganizationTypeDescription,'Not Recorded') as OrganizationTypeDescription
 
		,(  
			Select  
			count(distinct staffenterpriseid )  from [ETL_Local_PROD].[dbo].[AT_Logins_Daily] e 
			Where Cast(e.LoginDate as Date)>= @SnapDateStart1 AND Cast(e.LoginDate as Date)<= @SnapDateEnd1 and e.deptenterpriseid = x.deptenterpriseid   
			and e.UserRegistryID = x.StaffRegistryId
			--group by e.deptenterpriseid 
		  ) as UserLoginAtLeastOnceInMonth,
		  (
			Select  Count(*)  from [ETL_Local_PROD].[dbo].[AT_Logins_Hourly] e
			Where Cast(e.LoginDate as Date)>= @SnapDateStart1 AND Cast(e.LoginDate as Date)<= @SnapDateEnd1
			and e.deptenterpriseid = x.deptenterpriseid
			and e.UserRegistryID = x.StaffRegistryId
			--group by e.deptenterpriseid
		 ) as TotalLoginsByUser 



	    into #Temp33
	    FROM #TempOrg x
		left join [ETL_Local_PROD].[dbo].[DIM_Date]d on d.[Calendar Day] = @SnapDateStart1
		left join [ETL_Local_PROD].[Reference].[STP] s on s.CCGLONG_TRUNC = x.OriginalCCG
	    group by
		ParentORg,
		DeptName,
		x.DeptEnterpriseId,
		[StaffName],
		x.StaffRegistryId,
		[Created],
		[StaffJobTitle],
		[StaffActiveDescription],
		 ODSCode,
		 s.STP,
		Ward,
		x.OriginalCCG ,
		x.ccg,
		PCNName,
		OrganizationTypeDescription,
		d.[Financial Year] ,
	   D.Quarter 

	IF @MonthCounter1 = 1
	begin
		IF OBJECT_ID('[ETL_Local_PROD].[dbo].[AT_Logins_Report_Data]') IS NOT NULL 
		dROP TABLE [ETL_Local_PROD].[dbo].[AT_Logins_Report_Data]

	select * into [ETL_Local_PROD].[dbo].[AT_Logins_Report_Data] from #Temp33
	end
	ELSE
	begin
	insert into [ETL_Local_PROD].[dbo].[AT_Logins_Report_Data]
	select * from #Temp33
	end


	END

	 delete from [ETL_Local_PROD].[dbo].[AT_Logins_Report_Data] where ReportingMonth is null


	select * from [ETL_Local_PROD].[dbo].[AT_Logins_Report_Data] where DeptName = 'IMPERIAL COLLEGE HEALTHCARE  CHARING CROSS A and E DEPT' and ReportingMonth = '2020-09-01'
	
	--select * from #Temp44 
	--delete v from #TempLogins v
	--inner join
	--(

	--select
	--Deptname,
	--DeptEnterpriseId,
	--sum(ActiveLogins) as AL,
	--sum(UsersLoginAtLeastOnceInMonth) as UIM,
	--sum(TotalLoginsByOrg) as TLB,
	--sum(Over10LoginUsers) as OLU
	--from #TempLogins

	--group by
	--Deptname,
	--DeptEnterpriseId
	--) a on a.Deptname = v.DeptName
	--and a.DeptEnterpriseId = v.DeptEnterpriseId
	--where (a.AL = 0 and a.OLU = 0 and a.TLB = 0 and a.UIM = 0)

	 
		
 --  delete from [ETL_Local_PROD].[dbo].[AT_Commissioner_Report_Worksheet_4] where ReportingMonth is null
   
 --  delete from [ETL_Local_PROD].[dbo].[AT_Commissioner_Report_Worksheet_4] where OrganizationTypeDescription = 'exclude'