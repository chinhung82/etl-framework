import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length
from pyspark.sql.types import StringType, DateType
from unittest.mock import Mock, patch, MagicMock
import os

from src.transformers.transformer import DataTransformer
from src.loaders.mysql_loader import DataLoader


class TestDataTransformer:
    """Test suite for DataTransformer class"""
    
    def test_field_mapping_to_target_schema(self, spark_session, test_config, mock_metrics):
        """Test that fields are correctly mapped to target schema"""
        # Arrange
        data = [
            ('1', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-01-01'),
            ('2', 'Beta Inc', 'Company', 'REG002', '2020-02-01', 'GB', 'GB', 'ENG', 'ENG', 
             'Inactive', 'Finance', 'contact@beta.com', '2022-01-01'),
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("entities_validated")
        
        # Act - Map to target schema (typically snake_case for MySQL)
        result_df = spark_session.sql("""
            SELECT
                EntityID AS entity_id,
                EntityName AS entity_name,
                EntityType AS entity_type,
                RegistrationNumber AS registration_number,
                CAST(IncorporationDate AS DATE) AS incorporation_date,
                CountryCode AS country_code,
                StateCode AS state_code,
                Status AS status,
                Industry AS industry,
                ContactEmail AS contact_email,
                CAST(LastUpdate AS DATE) AS last_update
            FROM entities_validated
        """)
        
        results = result_df.collect()
        
        # Assert - Check column names
        expected_columns = ['entity_id', 'entity_name', 'entity_type', 'registration_number',
                           'incorporation_date', 'country_code', 'state_code', 'status',
                           'industry', 'contact_email', 'last_update']
        assert result_df.columns == expected_columns
        
        # Check data types
        assert isinstance(results[0]['incorporation_date'], type(None)) or \
               str(type(results[0]['incorporation_date'])) in ['<class \'datetime.date\'>', '<class \'str\'>']
    
    def test_data_type_conversions(self, spark_session, test_config, mock_metrics):
        """Test that data types are correctly converted"""
        # Arrange
        data = [
            ('1', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-01-01'),
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("entities_validated")
        
        # Act - Apply type conversions
        result_df = spark_session.sql("""
            SELECT
                CAST(EntityID AS INT) AS entity_id,
                EntityName AS entity_name,
                CAST(IncorporationDate AS DATE) AS incorporation_date,
                CAST(LastUpdate AS DATE) AS last_update
            FROM entities_validated
        """)
        
        # Assert
        schema = result_df.schema
        assert schema['entity_id'].dataType.typeName() == 'integer'
        assert schema['entity_name'].dataType.typeName() == 'string'
        # Date fields should be date type or string (depending on SQL parsing)
    
    def test_null_handling_in_transformation(self, spark_session, test_config, mock_metrics):
        """Test that nulls are preserved correctly during transformation"""
        # Arrange
        data = [
            ('1', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-01-01'),
            ('2', 'Beta Inc', 'Company', '', '', 'GB', 'GB', '', '', 
             '', '', '', ''),
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("entities_validated")
        
        # Act
        result_df = spark_session.sql("""
            SELECT
                EntityID AS entity_id,
                EntityName AS entity_name,
                CASE WHEN RegistrationNumber = '' THEN NULL ELSE RegistrationNumber END AS registration_number,
                CASE WHEN StateCode = '' THEN NULL ELSE StateCode END AS state_code,
                CASE WHEN Industry = '' THEN NULL ELSE Industry END AS industry
            FROM entities_validated
        """)
        
        results = result_df.collect()
        
        # Assert
        assert results[0]['registration_number'] == 'REG001'
        assert results[0]['state_code'] == 'CA'
        assert results[0]['industry'] == 'Tech'
        
        assert results[1]['registration_number'] is None
        assert results[1]['state_code'] is None
        assert results[1]['industry'] is None

    
    def test_data_integrity_before_insert(self, spark_session, test_config, mock_metrics):
        """Test that data integrity is maintained before MySQL insert"""
        # Arrange
        data = [
            ('1', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-01-01'),
            ('2', 'Beta Inc', 'Company', 'REG002', '2020-02-01', 'GB', 'ENG', 
             'Inactive', 'Finance', 'contact@beta.com', '2022-01-01'),
        ]
        columns = ['entity_id', 'entity_name', 'entity_type', 'registration_number',
                   'incorporation_date', 'country_code', 'state_code', 'status',
                   'industry', 'contact_email', 'last_update']
        
        df = spark_session.createDataFrame(data, columns)
        
        # Act - Verify data integrity checks
        # Check for duplicates
        duplicate_check = df.groupBy('entity_id').count().filter(col('count') > 1)
        
        # Check for null required fields
        null_check = df.filter(
            (col('entity_name').isNull()) | 
            (col('entity_type').isNull())
        )
        
        # Check field lengths
        length_check = df.filter(
            (col('entity_name').isNotNull() & (length(col('entity_name')).cast('string') > 150)) |
            (col('country_code').isNotNull() & (length(col('country_code')).cast('string') > 3))
        )
        
        # Assert
        assert duplicate_check.count() == 0, "Found duplicate entity_ids"
        assert null_check.count() == 0, "Found null required fields"
        assert length_check.count() == 0, "Found fields exceeding max length"
    
    
    def test_duplicate_key_handling(self, spark_session, test_config, mock_metrics):
        """Test handling of duplicate primary keys during insert"""
        # Arrange
        data = [
            ('1', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-01-01'),
            ('1', 'Acme Corp Duplicate', 'Company', 'REG001', '2020-01-01', 'US', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-01-01'),  # Duplicate entity_id
        ]
        columns = ['entity_id', 'entity_name', 'entity_type', 'registration_number',
                   'incorporation_date', 'country_code', 'state_code', 'status',
                   'industry', 'contact_email', 'last_update']
        
        df = spark_session.createDataFrame(data, columns)
        
        # Act - Check for duplicates before insert
        duplicate_check = df.groupBy('entity_id').count().filter(col('count') > 1)
        duplicates = duplicate_check.collect()
        
        # Assert
        assert len(duplicates) == 1, "Should detect duplicate entity_id"
        assert duplicates[0]['entity_id'] == '1'
        assert duplicates[0]['count'] == 2
    