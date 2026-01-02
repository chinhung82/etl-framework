SELECT 
EntityID,
EntityName,
EntityType,
RegistrationNumber,
IncorporationDate,
Country,
CountryCode,
State,
StateCode,
Status,
{{industry_mapping_placeholder}},
--CASE WHEN TRIM(Industry) = '' THEN NULL
--WHEN TRIM(Industry) = 'NULL' THEN NULL ELSE TRIM(Industry) END as Industry,
ContactEmail,
LastUpdate 
FROM entities_status;