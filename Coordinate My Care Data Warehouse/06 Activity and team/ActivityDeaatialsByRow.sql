
select top 10 * from [ETL_Local_PROD].[dbo].[AuditPatient_New]
select top 1000 * from [ETL_Local_PROD].[dbo].[AT_Staff]
select top 10 * from [ETL_Local_PROD].[dbo].[AT_PD_Dept]
select top 10000 * from [ETL_Local_PROD].[dbo].[AT_IndToOrg]


	if OBJECT_ID ('Tempdb..#CacheO') is not null
	drop table #CacheO

select
top 10000
CMC_ID,
ActionType,
ActionTime as ActionDateTime,
b.StaffTitle,
b.StaffForename + ' ' + b.StaffSurname as StaffName,
b.StaffActiveDescription,
b.StaffProviderTypeDescription as StaffProviderType,
b.StaffUserId,
c.DeptName,
c.DeptOpenDate,
c.DeptCloseDate,
b.StaffRegistryId as Staff_StaffRegistryId,
c.DeptPDRegistryID as DeptDP_DeptPDRegistryID,
d.StaffEnterpriseID as Ind2Org_StaffEnterpriseID

into #CacheO

from [ETL_Local_PROD].[dbo].[AuditPatient_New]a
left join [ETL_Local_PROD].[dbo].[AT_Staff]b on b.StaffRegistryId = a.StaffRegistryId
left join [ETL_Local_PROD].[dbo].[AT_PD_Dept]c on c.DeptPDRegistryID = a.DeptPDRegistryId
left join [ETL_Local_PROD].[dbo].[AT_IndToOrg]d on d.DeptEnterpriseID = c.DeptEnterpriseID
												and d.StaffEnterpriseID = b.StaffEnterpriseID
where b.StaffForename + ' ' + b.StaffSurname  = 'J CHALMERS-WATSON'


select top 1000 * from [ETL_Local_PROD].[dbo].[AT_Staff] where StaffForename + ' ' + StaffSurname = 'J CHALMERS-WATSON'




select * from #CacheO