/****** Script for SelectTopNRows command from SSMS  ******/
SELECT  [CMC_ID]
      ,[NHS_Number]
     
      ,[OriginalApprovedBy]
      ,[Date_Original_Approval]
      ,[OriginalApprover]
      ,[OriginalApproverJobTitle]
      ,[OriginalApproverWorkbase]
      --,[OriginalApproverWorkbaseODS]
      --,[Original_Approver_Prof_Group]
      --,[OriginalApproverWorkbaseEId]
      ,[OriginalApproverODS]
      ,[Original_Approver_Role_Description]
	  ,b.CCG
	  ,b.DerivedTeamType

  FROM [ETL_Local_PROD].[dbo].[AT_Patient_General]a
  inner join [AT_Organisation_to_Department_Provider_Directory]b on b.EnterpriseID = a.[OriginalApproverWorkbaseEId]
  where 
  DerivedTeamType = 'PRIMARY CARE'
  and
  b.CCG = 'NHS Bromley'
  and Date_Original_Approval >= '2020-08-01' and Date_Original_Approval <= '2020-12-31'
  order by CMC_ID