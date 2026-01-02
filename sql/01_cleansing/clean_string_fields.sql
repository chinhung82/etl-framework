SELECT
  CASE WHEN TRIM(EntityID) = '' THEN NULL ELSE TRIM(EntityID) END AS EntityID,
  CASE WHEN TRIM(EntityName) = '' THEN NULL ELSE TRIM(EntityName) END AS EntityName,
  CASE WHEN TRIM(EntityType) = '' THEN NULL ELSE TRIM(EntityType) END AS EntityType,
  CASE WHEN TRIM(RegistrationNumber) = '' THEN NULL ELSE TRIM(RegistrationNumber) END AS RegistrationNumber,
  CASE WHEN TRIM(IncorporationDate) = '' THEN NULL ELSE TRIM(IncorporationDate) END AS IncorporationDate,
  CASE WHEN TRIM(Country) = '' THEN NULL ELSE TRIM(Country) END AS Country,
  CASE WHEN TRIM(CountryCode) = '' THEN NULL ELSE TRIM(CountryCode) END AS CountryCode,
  CASE WHEN TRIM(State) = '' THEN NULL ELSE TRIM(State) END AS State,
  CASE WHEN TRIM(StateCode) = '' THEN NULL ELSE TRIM(StateCode) END AS StateCode,
  CASE WHEN TRIM(Status) = '' THEN NULL ELSE TRIM(Status) END AS Status,
  CASE WHEN TRIM(Industry) = '' THEN NULL ELSE TRIM(Industry) END AS Industry,
  CASE WHEN TRIM(ContactEmail) = '' THEN NULL ELSE TRIM(ContactEmail) END AS ContactEmail,
  CASE WHEN TRIM(LastUpdate) = '' THEN NULL ELSE TRIM(LastUpdate) END AS LastUpdate
FROM entities_raw;