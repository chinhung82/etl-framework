-- ============================================================================
-- PURPOSE: Add validation flag columns for each business rule
-- ============================================================================

SELECT
    *,
    
    -- Rule 1: Required fields EntityName and EntityType
    (EntityName IS NOT NULL AND TRIM(EntityName) != '' AND EntityType IS NOT NULL AND TRIM(EntityType) != '') AS valid_required_fields,
    
    -- Rule 2: Valid entity type
    (EntityType IN ('Company', 'Partnership', 'Trust', 'Nonprofit')) AS valid_entity_type,
    
    -- Rule 3: Valid status
    (Status IS NULL OR Status IN ('Active', 'Inactive', 'Pending')) AS valid_status,
    
    -- Rule 4: Field lengths
    (LENGTH(EntityName) <= 150 AND
     LENGTH(COALESCE(EntityType, '')) <= 30 AND
     LENGTH(COALESCE(RegistrationNumber, '')) <= 50 AND
     LENGTH(COALESCE(CountryCode, '')) <= 3 AND
     LENGTH(COALESCE(StateCode, '')) <= 50 AND
     LENGTH(COALESCE(Status, '')) <= 30 AND
     LENGTH(COALESCE(Industry, '')) <= 100 AND
     LENGTH(COALESCE(ContactEmail, '')) <= 100
    ) AS valid_field_lengths,
    
    -- Rule 5: Country-State consistency
    --(StateCode IS NULL OR CountryCode IS NOT NULL) AS valid_country_state,
    
    -- Aggregate: Is record valid?
    (EntityName IS NOT NULL AND EntityName != '' AND
     EntityType IS NOT NULL AND 
     EntityType IN ('Company', 'Partnership', 'Trust', 'Nonprofit') AND
     (Status IS NULL OR Status IN ('Active', 'Inactive', 'Pending')) AND
     LENGTH(EntityName) <= 150 --AND
     --(StateCode IS NULL OR CountryCode IS NOT NULL)
    ) AS is_valid
    
FROM entities_validation_flag;