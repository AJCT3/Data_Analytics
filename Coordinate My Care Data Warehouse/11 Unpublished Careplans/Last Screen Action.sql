/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (1000) [ItemId]
      ,[Screen]
	  ,b.Description as ScreenName
      ,[ConsumerId]
      ,[LastSaved]
      ,[LastSavedUser]
      ,[LastPublished]
      ,[LastPublishedUser]
      --,[TimeModified]
  FROM [ETL_PROD].[dbo].[CMC_LastScreenAction]a
  inner join [ETL_PROD].[dbo].[Coded_UIScreen]b on b.Code = a.Screen
  where ConsumerId = 100074369