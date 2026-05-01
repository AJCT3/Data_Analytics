USE [ETL_Local_PROD]
GO

/****** Object:  View [dbo].[Cache-DeptHierarchy]    Script Date: 21/10/2019 10:44:11 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER VIEW [dbo].[Cache-DeptHierarchy]
AS
SELECT     d1.DeptName AS name7, d1.LocalCMCOrgType AS type7, d1.LocalCMCOrgTypeDescription AS typedesc7, d1.DeptODSCode AS ods7, 
                      d1.DeptLocalCMCId AS cmcdeptid7, d1.DeptEnterpriseID AS eid7, d1.DeptCloseDate AS close7, d1.OrganizationType AS odstype7, 
                      d1.OrganizationTypeDescription AS odstypedescription7, d2.DeptName AS name6, d2.LocalCMCOrgType AS type6, d2.LocalCMCOrgTypeDescription AS typedesc6, 
                      d2.DeptODSCode AS ods6, d2.DeptLocalCMCId AS cmcdeptid6, d2.DeptEnterpriseID AS eid6, d2.DeptCloseDate AS close6, d2.OrganizationType AS odstype6, 
                      d2.OrganizationTypeDescription AS odstypedescription6, d3.DeptName AS name5, d3.LocalCMCOrgType AS type5, d3.LocalCMCOrgTypeDescription AS typedesc5, 
                      d3.DeptODSCode AS ods5, d3.DeptLocalCMCId AS cmcdeptid5, d3.DeptEnterpriseID AS eid5, d3.DeptCloseDate AS close5, d3.OrganizationType AS odstype5, 
                      d3.OrganizationTypeDescription AS odstypedescription5, d4.DeptName AS name4, d4.LocalCMCOrgType AS type4, d4.LocalCMCOrgTypeDescription AS typedesc4, 
                      d4.DeptODSCode AS ods4, d4.DeptLocalCMCId AS cmcdeptid4, d4.DeptEnterpriseID AS eid4, d4.DeptCloseDate AS close4, d4.OrganizationType AS odstype4, 
                      d4.OrganizationTypeDescription AS odstypedescription4, d5.DeptName AS name3, d5.LocalCMCOrgType AS type3, d5.LocalCMCOrgTypeDescription AS typedesc3, 
                      d5.DeptODSCode AS ods3, d5.DeptLocalCMCId AS cmcdeptid3, d5.DeptEnterpriseID AS eid3, d5.DeptCloseDate AS close3, d5.OrganizationType AS odstype3, 
                      d5.OrganizationTypeDescription AS odstypedescription3, d6.DeptName AS name2, d6.LocalCMCOrgType AS type2, d6.LocalCMCOrgTypeDescription AS typedesc2, 
                      d6.DeptODSCode AS ods2, d6.DeptLocalCMCId AS cmcdeptid2, d6.DeptEnterpriseID AS eid2, d6.DeptCloseDate AS close2, d6.OrganizationType AS odstype2, 
                      d6.OrganizationTypeDescription AS odstypedescription2, d7.DeptName AS name1, d7.LocalCMCOrgType AS type1, d7.LocalCMCOrgTypeDescription AS typedesc1, 
                      d7.DeptODSCode AS ods1, d7.DeptLocalCMCId AS cmcdeptid1, d7.DeptEnterpriseID AS eid1, d7.DeptCloseDate AS close1, d7.OrganizationType AS odstype1, 
                      d7.OrganizationTypeDescription AS odstypedescription1
FROM         dbo.PDDepartment AS d1 WITH (nolock) LEFT OUTER JOIN
                      dbo.PDOrgToOrg AS o1 WITH (nolock) ON d1.DeptEnterpriseID = o1.ChildOrganizationEID AND (o1.EndDate IS NULL OR
                      CAST(o1.EndDate AS date) > '2015-05-01') AND (o1.StartDate IS NULL OR
                      CAST(o1.StartDate AS date) <= CAST(GETDATE() AS DATE)) LEFT OUTER JOIN
                      dbo.PDDepartment AS d2 WITH (nolock) ON d2.DeptEnterpriseID = o1.ParentOrganizationEID LEFT OUTER JOIN
                      dbo.PDOrgToOrg AS o2 WITH (nolock) ON o1.ParentOrganizationEID = o2.ChildOrganizationEID AND (o2.EndDate IS NULL OR
                      CAST(o2.EndDate AS date) > '2015-05-01') AND (o2.StartDate IS NULL OR
                      CAST(o2.StartDate AS date) <= CAST(GETDATE() AS DATE)) LEFT OUTER JOIN
                      dbo.PDDepartment AS d3 WITH (nolock) ON d3.DeptEnterpriseID = o2.ParentOrganizationEID LEFT OUTER JOIN
                      dbo.PDOrgToOrg AS o3 WITH (nolock) ON o2.ParentOrganizationEID = o3.ChildOrganizationEID AND (o3.EndDate IS NULL OR
                      CAST(o3.EndDate AS date) > '2015-05-01') AND (o3.StartDate IS NULL OR
                      CAST(o3.StartDate AS date) <= CAST(GETDATE() AS DATE)) LEFT OUTER JOIN
                      dbo.PDDepartment AS d4 WITH (nolock) ON d4.DeptEnterpriseID = o3.ParentOrganizationEID LEFT OUTER JOIN
                      dbo.PDOrgToOrg AS o4 WITH (nolock) ON o3.ParentOrganizationEID = o4.ChildOrganizationEID AND (o4.EndDate IS NULL OR
                      CAST(o4.EndDate AS date) > '2015-05-01') AND (o4.StartDate IS NULL OR
                      CAST(o4.StartDate AS date) <= CAST(GETDATE() AS DATE)) LEFT OUTER JOIN
                      dbo.PDDepartment AS d5 WITH (nolock) ON d5.DeptEnterpriseID = o4.ParentOrganizationEID LEFT OUTER JOIN
                      dbo.PDOrgToOrg AS o5 WITH (nolock) ON o4.ParentOrganizationEID = o5.ChildOrganizationEID AND (o5.EndDate IS NULL OR
                      CAST(o5.EndDate AS date) > '2015-05-01') AND (o5.StartDate IS NULL OR
                      CAST(o5.StartDate AS date) <= CAST(GETDATE() AS DATE)) LEFT OUTER JOIN
                      dbo.PDDepartment AS d6 WITH (nolock) ON d6.DeptEnterpriseID = o5.ParentOrganizationEID LEFT OUTER JOIN
                      dbo.PDOrgToOrg AS o6 WITH (nolock) ON o5.ParentOrganizationEID = o6.ChildOrganizationEID AND (o6.EndDate IS NULL OR
                      CAST(o6.EndDate AS date) > '2015-05-01') AND (o6.StartDate IS NULL OR
                      CAST(o6.StartDate AS date) <= CAST(GETDATE() AS DATE)) LEFT OUTER JOIN
                      dbo.PDDepartment AS d7 WITH (nolock) ON d7.DeptEnterpriseID = o6.ParentOrganizationEID
GO


