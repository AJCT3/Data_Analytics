

IF OBJECT_ID('[ETL_Local_PROD].[dbo].[Team_Department]') IS NOT NULL 
	dROP TABLE [ETL_Local_PROD].[dbo].[Team_Department]
 select 
 distinct 
 a.DeptName,
 a.[DeptODSCode],
 b.OrganizationTypeDescription,
a.DeptEnterpriseId
 into [ETL_Local_PROD].[dbo].[Team_Department] 
 from Logins a
 left join
			(

			SELECT distinct DeptEnterpriseID
      ,[OrganizationTypeDescription]
    
  FROM [ETL_Local_PROD].[dbo].[Department]
			)b on b.DeptEnterpriseID = a.DeptEnterpriseId


				select top 5* FROM [ETL_Local_PROD].[dbo].[Department]
				select top 5* FROM [ETL_Local_PROD].[dbo].[Team_Department]
				select top 5* from Logins


				
IF OBJECT_ID('[ETL_Local_PROD].[dbo].[Team_Department]') IS NOT NULL 
	dROP TABLE [ETL_Local_PROD].[dbo].[Team_Department]
	select 
	distinct  
	DeptName,
	DeptEnterpriseID,
	DeptODSCode, 
	DeptPDRegistryID, 
	cast(null as varchar(200)) as OrganizationTypeDescription 
	into [ETL_Local_PROD].[dbo].[Team_Department] 
	from Logins


	select * from [ETL_Local_PROD].[dbo].[Team_Department]
 