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
Industry,
{{email_regex_placeholder}},
LastUpdate 
FROM entities_industry;