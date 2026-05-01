

	--print @StartDate2
---------------------------------------------------------------------------------------------------------------------------------------------------------------------
			IF OBJECT_ID('tempdb..#TempLogs') IS NOT NULL 
		dROP TABLE #TempLogs

		select
		--RN2,
		----RowBeforlaterFlag,
		--'x89889'UserRegistryID,
		LoginTime as datetime_Data
		--,
		--LogoutTime,
		--'' as FinalLoginTime,
		--'' as FinalLogoutTime

		into #TempLogs

	from [ETL_Local_PROD].[dbo].[AT_Login_Details]
	
	where  UserRegistryID = 575202 
	select * from #TempLogs where   loginTime >= '2019-10-12' and LoginTime <= '2019-10-15' and rn2 < 19 order by RN2



WITH CTE_DATETIME_DATA AS (
    SELECT
        datetime_data,
        LAG(datetime_data) 
            OVER (ORDER BY datetime_data) AS previous_datetime,
        LEAD(datetime_data) 
            OVER (ORDER BY datetime_data) AS next_datetime,
        ROW_NUMBER() OVER (ORDER BY #TempLogs.datetime_data) 
        AS island_location 
    FROM #TempLogs),
CTE_ISLAND_START AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY datetime_data) AS island_number,
        datetime_data AS island_start_datetime,
        island_location AS island_start_location
    FROM CTE_DATETIME_DATA
    WHERE DATEDIFF(MINUTE, previous_datetime, datetime_data) > 5
        OR CTE_DATETIME_DATA.previous_datetime IS NULL),
CTE_ISLAND_END AS (
    SELECT
        ROW_NUMBER() 
            OVER (ORDER BY datetime_data) AS island_number,
        datetime_data AS island_end_datetime,
        island_location AS island_end_location
    FROM CTE_DATETIME_DATA
    WHERE DATEDIFF(MINUTE, datetime_data, next_datetime) > 5
        OR CTE_DATETIME_DATA.next_datetime IS NULL)
SELECT
    CTE_ISLAND_START.island_start_datetime,
    CTE_ISLAND_END.island_end_datetime,
    (SELECT COUNT(*) 
     FROM CTE_DATETIME_DATA 
     WHERE CTE_DATETIME_DATA.datetime_data BETWEEN 
        CTE_ISLAND_START.island_start_datetime AND 
        CTE_ISLAND_END.island_end_datetime) 
    AS island_row_count
FROM CTE_ISLAND_START
INNER JOIN CTE_ISLAND_END
ON CTE_ISLAND_END.island_number = CTE_ISLAND_START.island_number;


