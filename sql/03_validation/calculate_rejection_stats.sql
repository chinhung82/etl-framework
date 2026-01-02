-- ============================================================================
-- FILE: sql/03_validation/calculate_rejection_stats.sql
-- PURPOSE: Calculate validation statistics for reporting
-- ============================================================================

SELECT
    COUNT(*) AS total_records,
    SUM(CASE WHEN is_valid = true THEN 1 ELSE 0 END) AS valid_count,
    SUM(CASE WHEN is_valid = false THEN 1 ELSE 0 END) AS invalid_count,
    
    -- Rejection reason counts
    SUM(CASE WHEN NOT valid_required_fields THEN 1 ELSE 0 END) AS missing_required_count,
    SUM(CASE WHEN NOT valid_entity_type THEN 1 ELSE 0 END) AS invalid_entity_type_count,
    SUM(CASE WHEN NOT valid_status THEN 1 ELSE 0 END) AS invalid_status_count,
    SUM(CASE WHEN NOT valid_field_lengths THEN 1 ELSE 0 END) AS field_length_exceeded_count--,
--    SUM(CASE WHEN NOT valid_date_logic THEN 1 ELSE 0 END) AS date_logic_error_count,
--    SUM(CASE WHEN NOT valid_country_state THEN 1 ELSE 0 END) AS country_state_error_count
    
FROM entities_validated;