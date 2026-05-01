USE [ETL_Local_PROD]
GO
/****** Object:  StoredProcedure [dbo].[AT_MASTERBUILD_AuditPatient_AuditCareplan]    Script Date: 19/03/2021 14:48:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






create PROCEDURE [dbo].[AT_REbuild_AuditData_RowID_Index] 
-- Amended for PD Upgrade
AS
BEGIN

DROP INDEX [itemRow] ON [dbo].[AT_CMC_AuditData_RowID]

 CREATE NONCLUSTERED INDEX [itemRow] ON [dbo].[AT_CMC_AuditData_RowID]
(
	[AuditID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
 

 end

