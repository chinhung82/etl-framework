SELECT 
EntityID,
EntityName,
EntityType,
RegistrationNumber,
IncorporationDate,
Country,
{{country_mapping_placeholder}},
State,
StateCode,
Status,
Industry,
ContactEmail,
LastUpdate 
FROM entities_dates;