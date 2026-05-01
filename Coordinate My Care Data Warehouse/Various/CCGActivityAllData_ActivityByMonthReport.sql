USE [ETL_Local_PROD]
GO

/****** Object:  View [Reporting].[Cache-CCGActivityAllData_ActivityByMonthReport]    Script Date: 10/10/2019 15:50:10 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO











ALTER view [Reporting].[Cache-CCGActivityAllData_ActivityByMonthReport] as

with creations as
(select top 5
CCG,
replace(Original_Prof_Group,'CCG','Community Trust') as Original_Prof_group,
year(add_date) as Year,
month(add_date) as Month,
DATENAME(month,add_date) as Month_Name,
originalworkbaseeid,
COUNT(*) as NumCreatedAndPublished
from PatientDetailSpan pd
where OriginalAssessmentStatus = 'Completed'
group by
CCG,
Original_Prof_Group,
year(add_date),MONTH(Add_Date),DATENAME(month,add_date),
originalworkbaseeid),









views as
(select
ccg,
year,
month,
[activity month name],
replace(teamtype,'CCG','Community Trust') as teamtype,
activityenterpriseid,
COUNT(*) as NumViews
from reporting.TeamAudit
where [Access Type] like 'view%'
group by
ccg,
year,
month,
[activity month name],
teamtype,
activityenterpriseid
),

recupdates as
(
select
ccg,
year,
month,
[activity month name],
replace(teamtype,'CCG','Community Trust') as teamtype,
activityenterpriseid,
COUNT(*) as NumUpdates
from reporting.TeamAudit
where ([Access Type]='clinical_event_updated' OR [Access Type]='revise')
group by
ccg,
year,
month,
[activity month name],
teamtype,
activityenterpriseid
),


months as 
(
-- 2010-08-10 is the first date on PatientDetailSpan

select enddate  as endmonth from dbo.MonthEndDate('2012-01-01 00:00:00')
),

regcounts as
(select year(endmonth) as year,month(endmonth) as month, datename(month,endmonth) as month_name,
CCG,PracticeEnterpriseId,practiceteamtype,
  (select COUNT(*)
   from Reporting.ActivityCareplans c2
   where c1.PracticeEnterpriseId = c2.PracticeEnterpriseId
   and add_date<=endmonth
   and (DoD_PDS is null or DoD_PDS>endmonth) and (DoD_Demographics is null or DoD_Demographics>endmonth)
   and (onnewsystem = 'Yes' or endmonth < CAST('2015-11-24' as DATE))) as NumRegistered
from months m left join Reporting.ActivityCareplans c1 on 1=1
group by CCG,PracticeEnterpriseId,practiceteamtype, endmonth),

regmax as (select PracticeEnterpriseId,MAX(NumRegistered) as maxRegistered from regcounts group by PracticeEnterpriseId),

registered as (select r.* from regcounts r join regmax m on r.PracticeEnterpriseId = m.PracticeEnterpriseId and m.maxRegistered>0),

logons as
(select deptenterpriseid,
year(LoginDate) as Year,
month(LoginDate) as Month,
DATENAME(month,LoginDate) as Month_Name,
COUNT(*) as NumLogonsByDay from
(select deptenterpriseid,logindate,'' as userid from LogonsByDay
union all
select distinct deptenterpriseid,[logon date] as logindate,[user id] from protocol.accuratelogins l join PDDepartment d on cast(l.logondepartmentid as varchar(25)) = d.DeptLocalCMCId) l
group by deptenterpriseid,
year(LoginDate),
month(LoginDate),
DATENAME(month,LoginDate)),

logins as
(select deptenterpriseid, COUNT(*) as NumLogins from StaffDepartment
where EndDate is null
group by deptenterpriseid),

stage0 as
(select
ISNULL(c.ccg,v.ccg) as CCG,
ISNULL(c.OriginalWorkbaseEId,v.ActivityEnterpriseId) as EnterpriseId,
ISNULL(c.year,v.year) as year, 
ISNULL(c.month,v.month) as month, 
ISNULL(c.month_name,v.[activity month name]) as month_name, 
ISNULL(c.[Original_Prof_Group],v.TeamType) as TeamType,
NumCreatedAndPublished,
NumViews
from creations c full outer join views v on c.CCG = v.CCG 
and c.OriginalWorkbaseEId = v.ActivityEnterpriseId 
and c.[Original_Prof_Group]= v.TeamType
and c.Year=v.Year 
and c.Month=v.month
),

stage1 as
(
select
ISNULL(c.ccg,ru.ccg) as CCG,
ISNULL(c.EnterpriseId,ru.ActivityEnterpriseId) as EnterpriseId,
ISNULL(c.year,ru.year) as year, 
ISNULL(c.month,ru.month) as month, 
ISNULL(c.month_name,ru.[activity month name]) as month_name, 
ISNULL(c.TeamType,ru.TeamType) as TeamType,
NumCreatedAndPublished,
NumViews,
NumUpdates
from stage0 c full outer join recupdates ru on c.CCG = ru.CCG and c.EnterpriseId = ru.ActivityEnterpriseId and c.Year=ru.Year and c.Month=ru.month
),


stage2 as
(select
ISNULL(s.ccg,r.ccg) as CCG,
ISNULL(s.EnterpriseId,r.PracticeEnterpriseId) as EnterpriseId,
ISNULL(s.year,r.year) as year, 
ISNULL(s.month,r.month) as month, 
ISNULL(s.month_name,r.month_name) as month_name, 
ISNULL(s.TeamType,r.PracticeTeamType) as TeamType,
NumCreatedAndPublished,
NumViews,
NumUpdates,
NumRegistered
from stage1 s full outer join registered r on s.CCG = r.CCG and s.EnterpriseId = r.PracticeEnterpriseId and s.Year=r.Year and s.Month=r.month
where ISNULL(s.TeamType,r.PracticeTeamType) <> 'CMC Team'),

-- MS 30.5.16 add in any Care Landscape services that have managed not to be there already due to 
-- total lack of engagement on their part (I'm looking at you CHUHSE)
stage3 as
(select * from stage2
union all
select distinct c.CCG, serviceeid, YEAR(endmonth), MONTH(endmonth), DATENAME(month,endmonth), LocalCMCOrgTypeDescription, NULL, NULL, NULL, NULL
from CareLandscape c
join PDDepartment d on c.serviceeid = d.deptenterpriseid
join months m on 1=1
left join stage2 s on c.serviceeid = s.enterpriseid and s.month = month(m.endmonth) and s.year = year(m.endmonth) and s.CCG = c.ccg
where s.CCG is null)

select t.*, Team, NumLogonsByDay, NumLogins,
case when i.ISAEnterpriseId is null then 'No' else 'Yes' end as ISA_Status
from stage3 t
left join
(select distinct * from
-- enforce uniqueness MS 29.5.16
(select team,original_workbase_id from
(select *,
row_number() over (partition by original_workbase_id order by team) rn 
from reporting.DisambiguatedOriginatingTeams) sel3 where rn=1
union all
select team,surgeryeid from
(select *,
row_number() over (partition by surgeryeid order by team) rn 
from reporting.DisambiguatedPractices) sel4 where rn=1
union all
select team,ActivityDepartmentID from
(select *,
row_number() over (partition by activitydepartmentid order by team) rn 
from Reporting.DisambiguatedActivityTeams) sel1 where rn=1) sel2) u on t.EnterpriseId = u.original_workbase_id
left join ISAList i on t.EnterpriseId = i.ISAEnterpriseId
left join logons lo on t.EnterpriseId = lo.DeptEnterpriseId and t.year = lo.Year and t.month = lo.Month and t.month_name = lo.Month_Name
left join logins li on t.EnterpriseId = li.DeptEnterpriseId 










GO


