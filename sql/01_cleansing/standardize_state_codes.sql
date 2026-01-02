SELECT 
EntityID,
EntityName,
EntityType,
RegistrationNumber,
IncorporationDate,
Country,
CountryCode,
State,
{{state_mapping_placeholder}},
Status,
Industry,
ContactEmail,
LastUpdate 
FROM entities_countries;
