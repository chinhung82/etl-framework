-- ============================================================================
-- PURPOSE: Map cleaned fields to target MySQL schema
-- ============================================================================

SELECT
    EntityName AS entity_name,
    EntityType AS entity_type,
    RegistrationNumber AS registration_number,
    IncorporationDate AS incorporation_date,
    CountryCode AS country_code,
    StateCode AS state_code,
    Status AS status,
    Industry AS industry,
    ContactEmail AS contact_email,
    LastUpdate AS last_update
    
FROM entities_validated
WHERE is_valid = true;