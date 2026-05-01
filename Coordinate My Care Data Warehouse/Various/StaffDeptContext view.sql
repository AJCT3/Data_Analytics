USE [ETL_Local_PROD]
GO

/****** Object:  View [dbo].[StaffDeptContext]    Script Date: 05/03/2020 16:57:57 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




ALTER view [dbo].[StaffDeptContext] as
select poc.ItemId as ProviderOrgContext, s.*, d.*
from ETL_PROD.dbo.CMC_ProviderOrgContext poc
left join ETL_PROD.dbo.CMC_IndividualProvider ip on poc.Provider = ip.ItemId
left join ETL_PROD.dbo.CMC_Individual i on i.PDRegistryID = ip.RegistryID
left join AT_Staff s on s.Individual = i.ItemID
left join AT_Dept d on d.Organization = poc.Organization
-- Exclude LastClinicalApprover rows introduced in 15.1 release 
where ip.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
AND ip.ItemId not like 'PS|%|%|%|%|%|LCA'
AND poc.ItemId not like 'CarePlan|%|%|%|%|%|LCA'
AND poc.ItemId not like 'PS|%|%|%|%|%|LCA'


GO


