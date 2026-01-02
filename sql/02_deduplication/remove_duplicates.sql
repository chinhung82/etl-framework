WITH scored AS (
    -- Calculate completeness score
    SELECT 
        *,
        (CASE WHEN EntityName IS NOT NULL AND EntityName != '' THEN 1 ELSE 0 END +
         CASE WHEN EntityType IS NOT NULL AND EntityType != '' THEN 1 ELSE 0 END +
         CASE WHEN RegistrationNumber IS NOT NULL AND RegistrationNumber != '' THEN 1 ELSE 0 END +
         CASE WHEN IncorporationDate IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN CountryCode IS NOT NULL AND CountryCode != '' THEN 1 ELSE 0 END +
         CASE WHEN StateCode IS NOT NULL AND StateCode != '' THEN 1 ELSE 0 END +
         CASE WHEN Status IS NOT NULL AND Status != '' THEN 1 ELSE 0 END +
         CASE WHEN Industry IS NOT NULL AND Industry != '' THEN 1 ELSE 0 END +
         CASE WHEN ContactEmail IS NOT NULL AND ContactEmail != '' THEN 1 ELSE 0 END +
         CASE WHEN LastUpdate IS NOT NULL THEN 1 ELSE 0 END
        ) AS completeness_score,
        CASE WHEN EntityName IS NOT NULL THEN LENGTH(EntityName) ELSE 0 END AS name_length
    FROM entities_clean
),
exact_dedup AS (
    -- Remove exact duplicates (RegNum + Name)
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY RegistrationNumber, EntityName
            ORDER BY completeness_score DESC, LastUpdate DESC NULLS LAST, 
                     name_length DESC, EntityID ASC
        ) AS rank_exact
    FROM scored
    WHERE RegistrationNumber IS NOT NULL AND EntityName IS NOT NULL
),
reg_dedup AS (
    -- Remove registration duplicates
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY RegistrationNumber
            ORDER BY completeness_score DESC, LastUpdate DESC NULLS LAST, 
                     name_length DESC, EntityID ASC
        ) AS rank_reg
    FROM scored
    WHERE RegistrationNumber IS NOT NULL
),
name_dedup AS (
    -- Remove name duplicates (no RegNum)
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY EntityName
            ORDER BY completeness_score DESC, LastUpdate DESC NULLS LAST, 
                     name_length DESC, EntityID ASC
        ) AS rank_name
    FROM scored
    WHERE RegistrationNumber IS NULL AND EntityName IS NOT NULL
)
-- Combine results
SELECT EntityID, EntityName, EntityType, RegistrationNumber, IncorporationDate,
       Country, CountryCode, State, StateCode, Status, Industry, ContactEmail, LastUpdate
FROM (
    SELECT * FROM exact_dedup WHERE rank_exact = 1
    UNION ALL
    SELECT s.*, 1 as rank_exact FROM scored s
    WHERE s.RegistrationNumber IS NULL OR s.EntityName IS NULL
) t1
WHERE EntityID IN (
    SELECT EntityID FROM reg_dedup WHERE rank_reg = 1
    UNION ALL
    SELECT EntityID FROM name_dedup WHERE rank_name = 1
    UNION ALL
    SELECT EntityID FROM scored 
    WHERE RegistrationNumber IS NULL AND EntityName IS NULL
)
ORDER BY EntityID