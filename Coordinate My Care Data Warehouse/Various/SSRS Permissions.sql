
Use ReportServer


;WITH 
catalog_type_description
AS
(
    SELECT tbl.* FROM (VALUES
      ( 1, 'Folder')
    , ( 2, 'Report')
    , ( 3, 'Resource')
    , ( 4, 'Linked Report')
    , ( 5, 'Data Source')
    , ( 6, 'Report Model')
    , ( 8, 'Shared Dataset')
    , ( 9, 'Report Part')
    ) tbl ([TypeID], [TypeDescription]) 
    WHERE 
        TypeID = 1
)
, 
nonreport_folders
AS
(
    SELECT tbl.* FROM (VALUES
      ( 'Images')
    , ( 'SharedDataSets')
    , ( 'Data Sources')
    , ( '')
    ) tbl ([FolderName]) 
)
, 
reporting_role_names -- added roles to the report server
AS
(
    SELECT tbl.* FROM (VALUES
      ( 'Browser Group')
    , ( 'Functional Owner')
    ) tbl ([RoleName]) 
)
, 
user_list
AS
(
    SELECT 
          usr.UserID
        , usr.UserName
        , UserNameFormat = 
            CASE 
                WHEN CHARINDEX('\', usr.UserName) > 0 THEN UPPER(SUBSTRING(usr.UserName ,CHARINDEX('\', usr.UserName) + 1, LEN(usr.UserName)))
                ELSE usr.UserName 
            END 
    FROM 
        dbo.Users AS usr
)
, 
reporting_roles
AS
(
    SELECT 
          cat.Name
        , rol.RoleName
        , usr.UserNameFormat
        , ReportingRoleName = rpt.RoleName
    FROM 
        dbo.[Catalog] AS cat
        INNER JOIN catalog_type_description AS tpd ON cat.[Type] = tpd.TypeID   
        LEFT JOIN dbo.PolicyUserRole AS urol ON urol.PolicyID = cat.PolicyID
        LEFT JOIN dbo.Roles AS rol ON urol.RoleID = rol.RoleID
        LEFT JOIN reporting_role_names AS rpt ON rpt.RoleName = rol.RoleName
        LEFT JOIN dbo.Policies AS pol ON urol.PolicyID = pol.PolicyID
        LEFT JOIN user_list AS usr ON urol.UserID = usr.UserID
        LEFT JOIN nonreport_folders AS nrf ON nrf.FolderName = cat.Name
    WHERE 
        1=1
        AND nrf.FolderName IS NULL
)
SELECT DISTINCT
      FolderName = rpt.Name
    , rpt.RoleName
    , UserNameFormat = STUFF((SELECT '; ' + rol.UserNameFormat FROM reporting_roles rol WHERE rol.RoleName = rpt.RoleName AND rol.Name = rpt.Name FOR XML PATH('')),1,1,'')
    , ReportingRoleName
FROM 
    reporting_roles AS rpt
	where rpt.UserNameFormat in (' TURNERAND','SALMANA','UWAEZEV')
	--where rpt.RoleName = 'Publisher'
	order by  rpt.RoleName

