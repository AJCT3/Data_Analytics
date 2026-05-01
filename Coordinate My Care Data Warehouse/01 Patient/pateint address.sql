
SELECT
no.PatientNumber as CMC_ID,
a.StreetLine, a.Line2, a.City, a.County, a.PostalCode,
a.CCAddressUse, 
a.CCDwellingType,
a.CCFromTime, a.CCToTime,
a.CCLivingConditions,
a.CCKeySafeDetails, a.CCResidenceNotes,
ISNULL(a.StreetLine,'') +
  case when a.StreetLine is not null and rtrim(a.StreetLine) <> '' 
       and a.Line2 is not null and rtrim(a.Line2) <> ''
       then ', ' else '' end + 
ISNULL(a.Line2,'') + case when a.City is not null and rtrim(a.City) <> '' then ', ' else '' end +
ISNULL(a.City,'') + case when a.County is not null and rtrim(a.County) <> '' then ', ' else '' end +
ISNULL(a.County,'') as CombinedAddress,
ps.Patient,
ROW_NUMBER() over (partition by ps.Patient order by Address) as AddressNo,
au.Description as CCAddressUseDescription,
dt.Description as CCDwellingTypeDescription,
-- MS 19.2.16 add living conditions description
lc.Description as CCLivingConditionsDescription
from ETL_PROD.dbo.CMC_PatientSummary ps
join ETL_PROD.dbo.CMC_Patient p on ps.Patient = p.ItemId
join ETL_PROD.dbo.CMC_Patient_PatientNumbers po on po.Patient = ps.ItemID
join ETL_PROD.dbo.CMC_PatientNumber no on no.ItemId = po.PatientNumber and no.AssigningAuthority = 'CMC'
join ETL_PROD.dbo.CMC_Patient_Addresses pa on p.itemid = pa.Patient
join ETL_PROD.dbo.CMC_Address a on pa.Address = a.ItemID
left join ETL_PROD.dbo.Coded_AddressUse au on a.CCAddressUse = au.code
left join ETL_PROD.dbo.Coded_DwellingType dt on a.CCDwellingType = dt.code
left join ETL_PROD.dbo.Coded_LivingConditions lc on a.CCLivingConditions = lc.code

WHERE no.PatientNumber IN (
							100015671, 
							100015678
							)


						 select Patient,CombinedAddress,PostalCode,ROW_NUMBER() over (partition by Patient order by CCFromTime desc) as rn from PatientAddresses where CCAddressUse = 'MAIN'
						 and patient in(	'PS||100015671||3','PS||100015671||4')
						 and (CCToTime is null or CAST(getdate() as date) < CAST(cctotime as date))
						 and (CCFromTime is null or CAST(getdate() as date) >= CAST(ccfromtime as date)) 

