-- ============================================================================
-- PURPOSE: Prepare quarantine data with rejection reasons
-- ============================================================================

SELECT
    EntityID,
    EntityName,
    EntityType,
    RegistrationNumber,
    IncorporationDate,
    CountryCode,
    StateCode,
    Status,
    Industry,
    ContactEmail,
    LastUpdate,
    
    -- Build rejection reasons string
    CONCAT_WS('; ',
        CASE WHEN NOT valid_required_fields THEN 'Missing required fields' END,
        CASE WHEN NOT valid_entity_type THEN 'Invalid entity type' END,
        CASE WHEN NOT valid_status THEN 'Invalid status value' END,
        CASE WHEN NOT valid_field_lengths THEN 'Field length exceeded' END--,
   --     CASE WHEN NOT valid_date_logic THEN 'Date logic error' END,
   --     CASE WHEN NOT valid_country_state THEN 'Country-State inconsistency' END
    ) AS rejection_reasons
    
FROM entities_validated
WHERE is_valid = false;