/****** Script for SelectTopNRows command from SSMS  ******/
SELECT [SnapShotDate]
      ,[ReportingMonth]
      ,[ReportMonthDate]
      ,[MonthEnd]
      ,[MonthCounter]
      ,[ReportingFinYear]
      ,[FinQuarter]
      ,[CommissioningArea]
      ,[CCG]
      ,[STP]
      ,[GPPractice]
      ,[Local Delivery Unit (Borough)]
      ,[Unique Records Alive at Month End]
      ,[Unique record published In Month]
      ,[Unique Records Alive in month]
      ,[Live CMC with review in last 6 months]
      ,[Live CMC with review in last 12 Months]
      ,[Unique records viewed By Urgent Care Services]
      ,[Total Views by Urgent Care Services]
      ,[Total Recorded as Deceased In-Month]
      ,[Unique Records Viewed By Urgent Care Services within 1 Month Prior to Death Date]
      ,[Total Views by Urgent Care Services within 1 month prior to Death Date]
      ,[Unique Records Viewed by Urgent Care Services within 3 Months Prior to Death Date]
      ,[Total Views by Urgent Care Services within 3 Months prior to Death Date]
      ,[Death Audit Completed]
      ,[Preferred Place of Death Achieved]
      ,[Count of unique number of CMC plans in DRAFT, requiring clinician approval]
      ,[myCMC plans finalised and requiring clinician approval]
      ,[myCMC plans approved by a clinicians and published]
      ,[Unique number of published CMC records where the review date is earlier than the current date]
      ,[Total Deceased In-Month]
      ,[Deceased - Place of Death Recorded]
      ,[Deceased - Preferred Place of Death Expressed]
      ,[Primary Care Network]
      ,[ODSCode]
      ,[GP practice registered population]
      ,[Overall GP practice weighted population]
      ,[Burrough]
  FROM [ETL_Local_PROD].[dbo].[AT_Commissioner_Report_Archive_RecordAudit_DeathAudit_New]
  where ReportingMonth = '2021-10-01'
  and ODSCode = 'H85086'
  --and ODSCode = 'H85693'

  update f

  set [Unique Records Alive at Month End] = 14

    FROM [ETL_Local_PROD].[dbo].[AT_Commissioner_Report_Archive_RecordAudit_DeathAudit]f
  where ReportingMonth = '2021-10-01'
  --and ODSCode = 'H85086'
  and ODSCode = 'H85113'





   update f

  set [Unique Records Alive at Month End] = 34

    FROM [ETL_Local_PROD].[dbo].[AT_Commissioner_Report_Archive_RecordAudit_DeathAudit]f
  where ReportingMonth = '2021-10-01'
  --and ODSCode = 'H85086'
  and ODSCode = 'H85693'



  delete
    FROM [ETL_Local_PROD].[dbo].[AT_Commissioner_Report_Archive_RecordAudit_DeathAudit]
  where  ODSCode = 'H85086'
  and ReportingMonth = '2021-10-01'