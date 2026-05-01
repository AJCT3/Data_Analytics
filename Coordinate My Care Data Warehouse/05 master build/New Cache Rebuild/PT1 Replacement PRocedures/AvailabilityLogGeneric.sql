USE [ETL_Local_PROD]
GO
/****** Object:  StoredProcedure [dbo].[AT__Activity_Data]    Script Date: 17/05/2020 12:40:18 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



alter PROCEDURE [dbo].[AT_AvailabilityLogGeneric_SP] 
-- Amended for PD Upgrade
AS
BEGIN


--ALTER view [dbo].[Cache-AvailabilityLogGeneric] as

		IF OBJECT_ID('tempdb..#MissingStuff') IS NOT NULL 
		dROP TABLE #MissingStuff
		select 
		distinct
		deptenterpriseID,
		OrganizationName,
		TeamType
		into #MissingStuff
		FROM [ETL_Local_PROD].[dbo].[AT_Login_Details]
		where deptenterpriseid is not null

			IF OBJECT_ID('tempdb..#MaxDate') IS NOT NULL 
		dROP TABLE #MaxDate
		select convert(date,max(eventdatetime))as MaxDate into #MaxDate from[ETL_Local_PROD].[dbo].[AvailabilityLogGeneric]


		insert into [ETL_Local_PROD].[dbo].[AvailabilityLogGeneric]

		select 
		distinct
		ItemId as id,
		cast(actiontime as datetime) as EventDateTime,
		cast(actiontime as date) as EventDate,
		case when MRNs='0' then 'N' else 'Y' end as match,
		'Generic' as Service, d.DeptEnterpriseID,
		coalesce(at.OrganizationName,d.DEptName) as  Team

		from ServiceSearchGeneric a
		-- logic here heeds to take close dates into account and also to follow Gareth's instructions re filtering
		left join (select distinct DEptName, deptodscode,deptenterpriseid,ROW_NUMBER() over (PARTITION by deptodscode order by deptodscode) as rn from PDDepartment where DeptODSCode is not null) d on ltrim(rtrim(a.ODS2)) = ltrim(rtrim(DeptODSCode)) and d.rn=1
		-- enforce uniqueness MS 29.5.16
		left join #MissingStuff at on at.deptenterpriseid = ltrim(rtrim(d.DeptEnterpriseID))
		where d.DeptEnterpriseID is not null
			and not exists (select id from [ETL_Local_PROD].[dbo].[AvailabilityLogGeneric] z with (nolock) where a.ItemId = z.id)
			and convert(date,actionTIme) >= (select dateadd(day,-1,maxDate) from #MaxDate)


	 END