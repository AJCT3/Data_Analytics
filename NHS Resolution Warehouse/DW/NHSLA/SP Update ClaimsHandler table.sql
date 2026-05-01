			USE [NHR Data Mart Test]
			GO
			/****** Object:  StoredProcedure [dbo].[sp_Update_ClaimGeneral_Archive]    Script Date: 30/07/2018 10:03:37 ******/
			SET ANSI_NULLS ON
			GO
			SET QUOTED_IDENTIFIER ON
			GO

			Alter procedure [dbo].[sp_Update_ClaimsHandler_Tables]

			 AS
			 BEGIN
			
			declare @SnapShot Datetime
			set @SnapShot = getdate()
			
			 if OBJECT_ID('[NHR Data Mart Test].[ref].[ClaimsHandler]') is not null
			drop table [NHR Data Mart Test].[ref].[ClaimsHandler]


			select 
			a.* ,
			@SnapShot as SnapShotDateTime
			into [NHR Data Mart Test].[ref].[ClaimsHandler]
			from [ldndw1\reporting].[Informatics_Reporting].[dbo].[ClaimsTeams_Andrew]a

			End