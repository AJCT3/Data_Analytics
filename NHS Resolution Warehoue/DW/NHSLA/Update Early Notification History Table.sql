

USE [NHR Data Mart Test]
GO
/****** Object:  StoredProcedure [KPI].[sp_Update_Open_Book_and_Litigation_Reports]    Script Date: 22/06/2018 12:02:03 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [SL].[sp_Update_Early_Notification_History_Table]

 AS
 


	if OBJECT_ID('[NHR Data Mart Test].[SL].[Early_Notification_History]') is Not Null
 

	drop table [NHR Data Mart Test].[SL].[Early_Notification_History]

	select
	*
	into [NHR Data Mart Test].[SL].[Early_Notification_History]
	FROM [ldndw1\reporting].[Informatics_Reporting].[dbo].[Inf_SL_01_Early_Notification]


 