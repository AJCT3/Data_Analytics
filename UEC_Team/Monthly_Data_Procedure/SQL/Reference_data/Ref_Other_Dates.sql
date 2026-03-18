-- 1. Drop & re-create the target table
IF OBJECT_ID('[PATLondon].[Ref_Other_Dates]','U') IS NOT NULL  
    DROP TABLE [PATLondon].[Ref_Other_Dates];  
GO  

CREATE TABLE [PATLondon].[Ref_Other_Dates] (  
    UniqMonthID       INT          NOT NULL PRIMARY KEY,  
    FinYear_YYYY_YY   VARCHAR(7)   NOT NULL,  
    MonthStartDate    DATE         NOT NULL,  
    MonthEndDate      DATE         NOT NULL  
);  
GO  

-- 2. Declare all vars up front  
DECLARE  
    @Today       DATE = CAST(GETDATE() AS DATE),  
    @EndDate     DATE,  
    @StartDate   DATE = '20000101',  -- Jan 2000  
    @BaseID      INT  = 1198,        -- UniqMonthID for Jan 2000  
    @MonthsCount INT;  

-- 3. Compute end‐of‐month logic  
IF @Today = EOMONTH(@Today)  
    SET @EndDate = @Today;            -- include this month  
ELSE  
    SET @EndDate = EOMONTH(@Today, -1); -- back up one month  

-- 4. How many months from Jan 2000 → @EndDate  
SET @MonthsCount = DATEDIFF(MONTH, @StartDate, @EndDate) + 1;  

-- 5. Build offsets using sys.all_objects as a tally source  
;WITH  
  Tally AS (  
    SELECT TOP (@MonthsCount)  
      ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS MonthOffset  
    FROM sys.all_objects a  
    CROSS JOIN sys.all_objects b  
  ),  
  Calendar AS (  
    SELECT  
      MonthOffset,  
      DATEADD(MONTH, MonthOffset, @StartDate)          AS MonthStartDate,  
      EOMONTH(DATEADD(MONTH, MonthOffset, @StartDate)) AS MonthEndDate  
    FROM Tally  
  )  
-- 6. Populate Ref_Other_Dates  
INSERT INTO [PATLondon].[Ref_Other_Dates]  
  (UniqMonthID, FinYear_YYYY_YY, MonthStartDate, MonthEndDate)  
SELECT  
    @BaseID + MonthOffset                              AS UniqMonthID,  
    -- Apr→Mar financial year  
    CAST(  
      CASE  
        WHEN MONTH(MonthStartDate) >= 4 THEN YEAR(MonthStartDate)  
        ELSE YEAR(MonthStartDate) - 1  
      END  
      AS VARCHAR(4)  
    )  
    + '/' +  
    RIGHT(  
      CAST(  
        CASE  
          WHEN MONTH(MonthStartDate) >= 4 THEN YEAR(MonthStartDate) + 1  
          ELSE YEAR(MonthStartDate)  
        END  
        AS VARCHAR(4)  
      ), 2  
    )                                                 AS FinYear_YYYY_YY,  
    MonthStartDate,  
    MonthEndDate  
FROM Calendar  
ORDER BY UniqMonthID;  
GO  

-- 7. Quick checks  
SELECT TOP(5) * FROM [PATLondon].[Ref_Other_Dates] ORDER BY UniqMonthID;  
SELECT TOP(5) * FROM [PATLondon].[Ref_Other_Dates] ORDER BY UniqMonthID DESC;  
GO  
