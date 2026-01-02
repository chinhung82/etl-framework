SELECT 
    EntityID,
    EntityName,
    EntityType,
    RegistrationNumber,
    {{IncorporationDate_parsing_placeholder}} AS IncorporationDate,
    Country,CountryCode,
    State,
    StateCode,
    Status,
    Industry,
    ContactEmail,
    {{LastUpdate_parsing_placeholder}} AS LastUpdate
FROM entities_clean_strings;