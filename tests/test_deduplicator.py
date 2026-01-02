import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.transformers.deduplicator import DataDeduplicator


class TestDataDeduplicator:
    """Test suite for DataDeduplicator class"""
    
    def test_exact_duplicate_removal(self, spark_session, test_config, mock_metrics):
        """Test removal of exact duplicates (same RegNum + Name)"""
        # Arrange
        data = [
            ('1', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-01-01'),
            ('2', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-06-01'),  # Exact duplicate, later date
            ('3', 'Beta Inc', 'Company', 'REG002', '2020-02-01', 'GB', 'GB', 'ENG', 'ENG', 
             'Active', 'Finance', 'contact@beta.com', '2022-01-01'),
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("entities_clean")
        
        # Act - Execute deduplication logic
        result_df = spark_session.sql("""
            WITH scored AS (
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
                    ROW_NUMBER() OVER (
                        PARTITION BY RegistrationNumber, EntityName
                        ORDER BY LastUpdate DESC NULLS LAST, EntityID ASC
                    ) AS rank_exact
                FROM entities_clean
                WHERE RegistrationNumber IS NOT NULL AND EntityName IS NOT NULL
            )
            SELECT EntityID, EntityName, RegistrationNumber, LastUpdate
            FROM scored
            WHERE rank_exact = 1
        """)
        
        results = result_df.collect()
        
        # Assert
        assert len(results) == 2  # Should keep only 2 records (1 duplicate removed)
        
        # Should keep the record with later LastUpdate (EntityID 2)
        acme_records = [r for r in results if r['RegistrationNumber'] == 'REG001']
        assert len(acme_records) == 1
        assert acme_records[0]['EntityID'] == '2'  # Later date
        assert acme_records[0]['LastUpdate'] == '2022-06-01'
    
    def test_registration_number_duplicates(self, spark_session, test_config, mock_metrics):
        """Test removal of duplicates with same RegNum but different names"""
        # Arrange
        data = [
            ('1', 'Beta Inc', 'Company', 'REG002', '2020-02-01', 'US', 'US', 'NY', 'NY', 
             'Active', 'Finance', '', '2022-01-01'),  # Less complete
            ('2', 'Beta Incorporated', 'Company', 'REG002', '2020-02-01', 'US', 'US', 'NY', 'NY', 
             'Active', 'Finance', 'contact@beta.com', '2022-01-01'),  # More complete (has email)
            ('3', 'Gamma LLC', 'Company', 'REG003', '2020-03-01', 'CA', 'CA', 'ON', 'ON', 
             'Active', 'Retail', 'info@gamma.com', '2022-01-01'),
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("entities_clean")
        
        # Act
        result_df = spark_session.sql("""
            WITH scored AS (
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
                    ROW_NUMBER() OVER (
                        PARTITION BY RegistrationNumber
                        ORDER BY 
                            (CASE WHEN ContactEmail IS NOT NULL AND ContactEmail != '' THEN 1 ELSE 0 END) DESC,
                            LastUpdate DESC NULLS LAST,
                            LENGTH(EntityName) DESC,
                            EntityID ASC
                    ) AS rank_reg
                FROM entities_clean
                WHERE RegistrationNumber IS NOT NULL
            )
            SELECT EntityID, EntityName, RegistrationNumber, ContactEmail, completeness_score
            FROM scored
            WHERE rank_reg = 1
        """)
        
        results = result_df.collect()
        
        # Assert
        assert len(results) == 2  # Should keep 2 unique registration numbers
        
        # Should keep "Beta Incorporated" (has email, more complete)
        beta_records = [r for r in results if r['RegistrationNumber'] == 'REG002']
        assert len(beta_records) == 1
        assert beta_records[0]['EntityID'] == '2'
        assert beta_records[0]['EntityName'] == 'Beta Incorporated'
        assert beta_records[0]['ContactEmail'] == 'contact@beta.com'
    
    def test_name_only_duplicates(self, spark_session, test_config, mock_metrics):
        """Test removal of duplicates with same name but no RegNum"""
        # Arrange
        data = [
            ('1', 'Gamma LLC', 'Partnership', '', '2020-03-01', 'CA', 'CA', 'ON', 'ON', 
             'Active', 'Retail', 'info@gamma.com', '2022-01-01'),  # Complete
            ('2', 'Gamma LLC', 'Partnership', '', '2020-03-01', 'CA', 'CA', '', '', 
             'Active', '', '', '2022-01-01'),  # Incomplete
            ('3', 'Delta Co', 'Company', '', '2020-04-01', 'GB', 'GB', 'ENG', 'ENG', 
             'Pending', 'Healthcare', 'info@delta.co.uk', '2022-01-01'),
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("entities_clean")
        
        # Act
        result_df = spark_session.sql("""
            WITH scored_base AS (
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
                    ) AS completeness_score
                FROM entities_clean
                WHERE (RegistrationNumber IS NULL OR RegistrationNumber = '')
                AND EntityName IS NOT NULL
            ),
            scored AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY EntityName
                        ORDER BY completeness_score DESC,
                                LastUpdate DESC NULLS LAST,
                                EntityID ASC
                    ) AS rank_name
                FROM scored_base
            )
            SELECT EntityID, EntityName, completeness_score, StateCode, Industry, ContactEmail
            FROM scored
            WHERE rank_name = 1
        """)
        
        results = result_df.collect()
        print(results)
        
        # Assert
        assert len(results) == 2  # Should keep 2 unique names
        
        # Should keep more complete Gamma LLC record (EntityID 1)
        gamma_records = [r for r in results if r['EntityName'] == 'Gamma LLC']
        assert len(gamma_records) == 1
        assert gamma_records[0]['EntityID'] == '1'
        assert gamma_records[0]['ContactEmail'] == 'info@gamma.com'
        assert gamma_records[0]['StateCode'] == 'ON'
        assert gamma_records[0]['Industry'] == 'Retail'
    
    def test_completeness_score_calculation(self, spark_session, test_config, mock_metrics):
        """Test that completeness score is calculated correctly"""
        # Arrange
        data = [
            # All fields complete (score: 10)
            ('1', 'Complete Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@complete.com', '2022-01-01'),
            # Missing some fields (score: 7)
            ('2', 'Incomplete Inc', 'Company', 'REG002', '2020-02-01', 'US', 'US', '', '', 
             'Active', '', '', '2022-01-01'),
            # Minimal fields (score: 4)
            ('3', 'Minimal Ltd', 'Company', '', '', 'GB', 'GB', '', '', 
             '', '', '', ''),
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("entities_clean")
        
        # Act
        result_df = spark_session.sql("""
            SELECT 
                EntityID,
                EntityName,
                (CASE WHEN EntityName IS NOT NULL AND EntityName != '' THEN 1 ELSE 0 END +
                 CASE WHEN EntityType IS NOT NULL AND EntityType != '' THEN 1 ELSE 0 END +
                 CASE WHEN RegistrationNumber IS NOT NULL AND RegistrationNumber != '' THEN 1 ELSE 0 END +
                 CASE WHEN IncorporationDate IS NOT NULL AND IncorporationDate != ''THEN 1 ELSE 0 END +
                 CASE WHEN CountryCode IS NOT NULL AND CountryCode != '' THEN 1 ELSE 0 END +
                 CASE WHEN StateCode IS NOT NULL AND StateCode != '' THEN 1 ELSE 0 END +
                 CASE WHEN Status IS NOT NULL AND Status != '' THEN 1 ELSE 0 END +
                 CASE WHEN Industry IS NOT NULL AND Industry != '' THEN 1 ELSE 0 END +
                 CASE WHEN ContactEmail IS NOT NULL AND ContactEmail != '' THEN 1 ELSE 0 END +
                 CASE WHEN LastUpdate IS NOT NULL AND LastUpdate != '' THEN 1 ELSE 0 END
                ) AS completeness_score
            FROM entities_clean
        """)
        
        results = {row['EntityID']: row['completeness_score'] for row in result_df.collect()}
        print(results)
        
        # Assert
        assert results['1'] == 10  # All fields complete
        assert results['2'] == 7   # Missing StateCode, Industry, ContactEmail
        assert results['3'] == 3   # Only Name, Type, CountryCode
    
    def test_duplicate_removal_keeps_most_recent(self, spark_session, test_config, mock_metrics):
        """Test that duplicates with same score keep the most recent record"""
        # Arrange
        data = [
            ('1', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-01-01'),
            ('2', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'contact@acme.com', '2022-06-01'),  # Later date, different email
            ('3', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'hello@acme.com', '2022-03-01'),  # Middle date
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("entities_clean")
        
        # Act
        result_df = spark_session.sql("""
            WITH scored AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY RegistrationNumber, EntityName
                        ORDER BY LastUpdate DESC NULLS LAST, EntityID ASC
                    ) AS rank_exact
                FROM entities_clean
            )
            SELECT EntityID, EntityName, ContactEmail, LastUpdate
            FROM scored
            WHERE rank_exact = 1
        """)
        
        results = result_df.collect()
        
        # Assert
        assert len(results) == 1
        assert results[0]['EntityID'] == '2'  # Most recent
        assert results[0]['LastUpdate'] == '2022-06-01'
        assert results[0]['ContactEmail'] == 'contact@acme.com'
    
    def test_full_deduplication_process(self, spark_session, duplicate_test_data, test_config, mock_metrics):
        """Integration test for full deduplication process"""
        # Arrange
        deduplicator = DataDeduplicator(spark_session, test_config, mock_metrics)
        original_count = duplicate_test_data.count()
        
        # Act
        result_df = deduplicator.deduplicate(duplicate_test_data)
        final_count = result_df.count()

        print(original_count, final_count)
        
        # Assert
        assert original_count == 7  # Started with 7 records
        assert final_count == 4     # Should end with 4 unique records
        assert mock_metrics.duplicate_records == 3  # 3 duplicates removed
        
        # Verify specific results
        results = result_df.collect()
        entity_ids = [row['EntityID'] for row in results]
        
        # Should keep: 1002 (Acme - later date), 1004 (Beta - more complete), 1005 (Gamma - complete), 1007 (Delta - unique)
        assert '1002' in entity_ids   # One Acme record
        assert '1004' in entity_ids   # One Beta record
        assert '1005' in entity_ids   # One Gamma record
        assert '1007' in entity_ids  # Delta (unique)

    def test_no_duplicates_scenario(self, spark_session, test_config, mock_metrics):
        """Test that deduplication handles data with no duplicates correctly"""
        # Arrange
        data = [
            ('1', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-01-01'),
            ('2', 'Beta Inc', 'Company', 'REG002', '2020-02-01', 'GB', 'GB', 'ENG', 'ENG', 
             'Active', 'Finance', 'contact@beta.com', '2022-01-01'),
            ('3', 'Gamma LLC', 'Partnership', 'REG003', '2020-03-01', 'CA', 'CA', 'ON', 'ON', 
             'Active', 'Retail', 'info@gamma.com', '2022-01-01'),
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        deduplicator = DataDeduplicator(spark_session, test_config, mock_metrics)
        
        # Act
        result_df = deduplicator.deduplicate(input_df)
        
        # Assert
        assert result_df.count() == 3  # All 3 records preserved
        assert mock_metrics.duplicate_records == 0  # No duplicates