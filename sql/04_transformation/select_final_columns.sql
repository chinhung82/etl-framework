-- ============================================================================
-- PURPOSE: Select final columns in correct order
-- ============================================================================

SELECT
    entity_name,
    entity_type,
    registration_number,
    incorporation_date,
    country_code,
    state_code,
    status,
    industry,
    contact_email,
    last_update
    
FROM entities_transformed
WHERE entity_name IS NOT NULL;