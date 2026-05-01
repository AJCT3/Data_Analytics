USE [ETL_Local_PROD]
GO

/****** Object:  View [dbo].[Cache-PatientRegisteredGP]    Script Date: 21/10/2019 09:49:57 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER view [dbo].[Cache-PatientRegisteredGP] as 
select CMC_ID, PatientSummary, RegisteredGP, CCG,
-- Add new CCG ODS field for all, not just London MS 7.3.16
case when CCG = 'NHS SURREY DOWNS CCG' then NULL else DeptODSCode end as London_CCG_ODS,
-- This field populated only if not Unknown/Cross-Border
DeptODSCode as CCG_ODS,
case
  when CCG = 'NHS SURREY DOWNS CCG' then 'Surrey Downs'
  when CCG like 'Cross Border%' then 'Cross Border'
  when CCG like 'Unknown%' then 'Unknown'
  else 'London'
  end as CommissioningArea,
SurgeryEId,GPEId,Surgery
from
(select
cmc_id,
c.PatientSummary,
Provider as RegisteredGP,
case
-- workaround for West London
  when h.name6 = 'NHS WEST LONDON CCG' then h.name6
  when h.name5 = 'NHS WEST LONDON CCG' then h.name5
  when h.typedesc6 = 'CCG' then h.name6
  when h.typedesc5 = 'CCG' then h.name5
  when h.name6 like '%CCG' then 'Cross Border: ' + h.name6
  when h.name5 like '%CCG' then 'Cross Border: ' + h.name5
-- workaround for org to org links that have gone walkies
  when m.dept_parent like '%CCG' then
    case m.dept_parent
      when 'NHS WEST LONDON (K&C & QPP) CCG' then 'NHS WEST LONDON CCG'
      else m.dept_parent end 
  else 'Unknown/Practice not a Practice: ' + isnull(h.name6,'not given') end as CCG,
ROW_NUMBER() over (partition by c.patientsummary
-- MS 3.11.16 proper sequencing
order by
case when h.typedesc6='CCG' then 0 when h.typedesc5='CCG' then 0 else 1 end,
h.name6,h.name5) as prn,
DeptEnterpriseId as SurgeryEId,
StaffEnterpriseId as GPEId,
DeptName as Surgery
from PatientHSCContacts c
left join DeptHierarchy h on c.DeptEnterpriseID = h.eid7
left join Protocol.ODSReconciliationCandidates m on c.DeptLocalCMCId = m.dept_id
where role = 'REG'
-- take end date into account MS 21.3.16
and (c.ToTime is null or cast(c.ToTime as date) > CAST(getdate() as DATE))
and (c.FromTime is null or cast(c.FromTime as date) <= CAST(getdate() as DATE))
) sel1
-- Remove any possible duplicates MS 21.3.16
left join (select *,ROW_NUMBER() over (PARTITION by deptname
-- MS 3.11.16 proper sequencing
order by deptenterpriseid) as drn from PDDepartment) d
  on sel1.CCG = d.DeptName
where (drn=1 or drn is null) and prn=1
GO


