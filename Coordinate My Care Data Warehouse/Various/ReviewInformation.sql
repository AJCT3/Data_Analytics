with reviewinformation as
(
select
 add_date as [Date Added to CMC]
,cmc_id as [Internal CMC ID]
,[CCG]
,surgery as [Practice]
,PracticeEnterpriseId
,case when DoD_PDS is not null OR DoD_Demographics IS not null then 'Deceased' else 'Living' end as [Deceased or Living]
,OriginalWorkbaseEId
,ot.Team as [Inputting Team]
,stuff(cast(replace(replace(replace(stuff((
  select '*SEP*' + convert(varchar(11),activitydate,106) as Activity
  from Reporting.TeamAudit aud
  where aud.CMC_ID = demo.cmc_id
  and aud.ActivityEnterpriseID = demo.OriginalWorkbaseEId
  and [Access Type] like 'view%'
  and ActivityDate between @startdate and @enddate
  order by activitydate desc
  for xml path('')
  ),1,0,''),'<Activity>',''),'</Activity>',''),'*SEP*',CHAR(0x0D)+CHAR(0x0A))
  as varchar(max)),1,2,'') as [Subsequent dates on which record viewed by inputting team]
,stuff(cast(replace(replace(replace(stuff((
  select '*SEP*' + convert(varchar(11),activitydate,106) as Activity
  from Reporting.TeamAudit aud
  where aud.CMC_ID = demo.cmc_id
  and aud.ActivityEnterpriseID = demo.OriginalWorkbaseEId
  and [Access Type] = 'revise'
  and ActivityDate between @startdate and @enddate
  order by activitydate desc
  for xml path('')
  ),1,0,''),'<Activity>',''),'</Activity>',''),'*SEP*',CHAR(0x0D)+CHAR(0x0A))
  as varchar(max)),1,2,'') as [Subsequent dates on which record revised by inputting team]
,stuff(cast(replace(replace(replace(stuff((
  select '*SEP*' + convert(varchar(11),activitydate,106) + ' (' + at.team + ')' as Activity
  from Reporting.TeamAudit aud
  join (select *,
-- force uniqueness MS 29.5.16
  row_number() over (partition by activitydepartmentid order by activitydepartmentid) rn 
  from Reporting.DisambiguatedActivityTeams) at on ActivityEnterpriseId=at.ActivityDepartmentID and rn=1
  where aud.CMC_ID = demo.cmc_id
  and aud.ActivityEnterpriseID <> demo.OriginalWorkbaseEId
  and [Access Type] like 'view%'
  and ActivityDate between @startdate and @enddate
  order by activitydate desc
  for xml path('')
  ),1,0,''),'<Activity>',''),'</Activity>',''),'*SEP*',CHAR(0x0D)+CHAR(0x0A))
  as varchar(max)),1,2,'') as [Subsequent dates on which record viewed by another team]
,stuff(cast(replace(replace(replace(stuff((
  select '*SEP*' + convert(varchar(11),activitydate,106) + ' (' + at.team + ')' as Activity
  from Reporting.TeamAudit aud
  join (select *,
  row_number() over (partition by activitydepartmentid order by activitydepartmentid) rn 
  from Reporting.DisambiguatedActivityTeams) at on ActivityEnterpriseId=at.ActivityDepartmentID and rn=1
  where aud.CMC_ID = demo.cmc_id
  and aud.ActivityEnterpriseID <> demo.OriginalWorkbaseEId
  and [Access Type] = 'revise'
  and ActivityDate between @startdate and @enddate
  order by activitydate desc
  for xml path('')
  ),1,0,''),'<Activity>',''),'</Activity>',''),'*SEP*',CHAR(0x0D)+CHAR(0x0A))
  as varchar(max)),1,2,'') as [Subsequent dates on which record revised by another team]
,DOD_Demographics
,DoD_PDS as [Date of Death]
from PatientDetailSpan demo
join (select *,
row_number() over (partition by original_workbase_id order by original_workbase_id) rn 
from Reporting.DisambiguatedoriginatingTeams) ot on OriginalWorkbaseEId = ot.original_workbase_id and rn=1
)

select
       [Date Added to CMC]
      ,[Internal CMC ID]
      ,[CCG]
      ,[Practice]
      ,[Deceased or Living]
      ,[Inputting Team]
      ,[Subsequent dates on which record viewed by inputting team]
      ,[Subsequent dates on which record revised by inputting team]
      ,[Subsequent dates on which record viewed by another team]
      ,[Subsequent dates on which record revised by another team]
,'Date of death - PDS: ' + isnull([date of death],'(Living)') + ', Date of death - CMC: ' + isnull(r.dod_demographics +
 case ppd_recorded when 1 then ', PPD recorded' + 
 case apd_recorded when 1 then ', Actual place of death recorded' +
 case match when 1 then ', PPD met' else ', PPD not met' end
 else ', Actual place of death not recorded' end
 else ', PPD not recorded' end,'(Living)') as DeathInfo
from reviewinformation r
join [DeathsInfoByCMCID] d
on [Internal CMC ID]  = d.cmc_id
where 
(CCG = @CCG or @CCG='*')
and
(PracticeEnterpriseId = @Practice or @Practice = -1)

and
(@InputtingTeam = '*' or [Inputting Team] = @InputtingTeam)

and
([Deceased or Living] = 'Living' or @IncludeDeceased = 'Yes')

and
([Subsequent dates on which record viewed by inputting team] is not null
or [Subsequent dates on which record revised by inputting team] is not null
or [Subsequent dates on which record viewed by another team] is not null
or [Subsequent dates on which record revised by another team] is not null)