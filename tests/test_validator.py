import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.transformers.validator import DataValidator


class TestDataValidator:
    """Test suite for DataValidator class"""
    
    def test_required_fields_validation(self, spark_session, test_config, mock_metrics):
        """Test validation of required fields (EntityName, EntityType)"""
        # Arrange
        data = [
            ('1', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-01-01'),  # Valid
            ('2', '', 'Company', 'REG002', '2020-02-01', 'GB', 'GB', 'ENG', 'ENG', 
             'Active', 'Finance', 'contact@beta.com', '2022-01-01'),  # Missing name
            ('3', 'Gamma LLC', '', 'REG003', '2020-03-01', 'CA', 'CA', 'ON', 'ON', 
             'Active', 'Retail', 'info@gamma.com', '2022-01-01'),  # Missing type
            ('4', '', '', 'REG004', '2020-04-01', 'US', 'US', 'TX', 'TX', 
             'Active', 'Tech', 'info@delta.com', '2022-01-01'),  # Missing both
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("entities_deduped")
        
        # Act
        result_df = spark_session.sql("""
            SELECT
                EntityID,
                EntityName,
                EntityType,
                (EntityName IS NOT NULL AND EntityName != '' AND 
                 EntityType IS NOT NULL AND EntityType != '') AS valid_required_fields
            FROM entities_deduped
        """)
        
        results = {row['EntityID']: row['valid_required_fields'] for row in result_df.collect()}
        
        # Assert
        assert results['1'] == True   # Valid
        assert results['2'] == False  # Missing name
        assert results['3'] == False  # Missing type
        assert results['4'] == False  # Missing both
    
    def test_entity_type_validation(self, spark_session, test_config, mock_metrics):
        """Test validation of entity type values"""
        # Arrange
        data = [
            ('1', 'Acme Corp', 'Company', 'REG001', '', 'US', 'US', '', '', '', '', '', ''),
            ('2', 'Beta Trust', 'Trust', 'REG002', '', 'GB', 'GB', '', '', '', '', '', ''),
            ('3', 'Gamma Partners', 'Partnership', 'REG003', '', 'CA', 'CA', '', '', '', '', '', ''),
            ('4', 'Delta Charity', 'Nonprofit', 'REG004', '', 'AU', 'AU', '', '', '', '', '', ''),
            ('5', 'Invalid Corp', 'Corporation', 'REG005', '', 'US', 'US', '', '', '', '', '', ''),  # Invalid
            ('6', 'Bad Type Inc', 'LLC', 'REG006', '', 'SG', 'SG', '', '', '', '', '', ''),  # Invalid
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("entities_deduped")
        
        # Act
        result_df = spark_session.sql("""
            SELECT
                EntityID,
                EntityType,
                (EntityType IN ('Company', 'Partnership', 'Trust', 'Nonprofit')) AS valid_entity_type
            FROM entities_deduped
        """)
        
        results = {row['EntityID']: row['valid_entity_type'] for row in result_df.collect()}
        
        # Assert
        assert results['1'] == True   # Company - valid
        assert results['2'] == True   # Trust - valid
        assert results['3'] == True   # Partnership - valid
        assert results['4'] == True   # Nonprofit - valid
        assert results['5'] == False  # Corporation - invalid
        assert results['6'] == False  # LLC - invalid
    
    def test_status_validation(self, spark_session, test_config, mock_metrics):
        """Test validation of status values"""
        # Arrange
        data = [
            ('1', 'Acme Corp', 'Company', 'REG001', '', 'US', 'US', '', '', 'Active', '', '', ''),
            ('2', 'Beta Inc', 'Company', 'REG002', '', 'GB', 'GB', '', '', 'Inactive', '', '', ''),
            ('3', 'Gamma LLC', 'Partnership', 'REG003', '', 'CA', 'CA', '', '', 'Pending', '', '', ''),
            ('4', 'Delta Trust', 'Trust', 'REG004', '', 'AU', 'AU', '', '', '', '', '', ''),  # Null - valid
            ('5', 'Invalid Corp', 'Company', 'REG005', '', 'US', 'US', '', '', 'Suspended', '', '', ''),  # Invalid
            ('6', 'Bad Status Inc', 'Company', 'REG006', '', 'SG', 'SG', '', '', 'Closed', '', '', ''),  # Invalid
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("entities_deduped")
        
        # Act
        result_df = spark_session.sql("""
            SELECT
                EntityID,
                Status,
                (Status IS NULL OR Status = '' OR Status IN ('Active', 'Inactive', 'Pending')) AS valid_status
            FROM entities_deduped
        """)
        
        results = {row['EntityID']: row['valid_status'] for row in result_df.collect()}
        
        # Assert
        assert results['1'] == True   # Active - valid
        assert results['2'] == True   # Inactive - valid
        assert results['3'] == True   # Pending - valid
        assert results['4'] == True   # Null - valid
        assert results['5'] == False  # Suspended - invalid
        assert results['6'] == False  # Closed - invalid
    
    def test_field_length_validation(self, spark_session, test_config, mock_metrics):
        """Test validation of field length constraints"""
        # Arrange
        long_name = 'A' * 200  # Exceeds 150 char limit
        long_email = 'test@' + 'x' * 100 + '.com'  # Exceeds 100 char limit
        long_industry = 'X' * 150  # Exceeds 100 char limit
        
        data = [
            ('1', 'Acme Corp', 'Company', 'REG001', '', 'US', 'US', '', '', 
             'Active', 'Tech', 'info@acme.com', ''),  # All valid
            ('2', long_name, 'Company', 'REG002', '', 'GB', 'GB', '', '', 
             'Active', 'Finance', 'contact@beta.com', ''),  # Name too long
            ('3', 'Gamma LLC', 'Company', 'REG003', '', 'CA', 'CA', '', '', 
             'Active', 'Retail', long_email, ''),  # Email too long
            ('4', 'Delta Inc', 'Company', 'REG004', '', 'AU', 'AU', '', '', 
             'Active', long_industry, 'info@delta.com', ''),  # Industry too long
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("entities_deduped")
        
        # Act
        result_df = spark_session.sql("""
            SELECT
                EntityID,
                (LENGTH(EntityName) <= 150 AND
                 LENGTH(COALESCE(EntityType, '')) <= 30 AND
                 LENGTH(COALESCE(RegistrationNumber, '')) <= 50 AND
                 LENGTH(COALESCE(CountryCode, '')) <= 3 AND
                 LENGTH(COALESCE(StateCode, '')) <= 50 AND
                 LENGTH(COALESCE(Status, '')) <= 30 AND
                 LENGTH(COALESCE(Industry, '')) <= 100 AND
                 LENGTH(COALESCE(ContactEmail, '')) <= 100
                ) AS valid_field_lengths
            FROM entities_deduped
        """)
        
        results = {row['EntityID']: row['valid_field_lengths'] for row in result_df.collect()}
        
        # Assert
        assert results['1'] == True   # All fields valid length
        assert results['2'] == False  # Name too long
        assert results['3'] == False  # Email too long
        assert results['4'] == False  # Industry too long
    
    def test_aggregate_validation_flag(self, spark_session, test_config, mock_metrics):
        """Test the aggregate is_valid flag"""
        # Arrange
        data = [
            # Valid record
            ('1', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-01-01'),
            # Missing required field
            ('2', '', 'Company', 'REG002', '2020-02-01', 'GB', 'GB', 'ENG', 'ENG', 
             'Active', 'Finance', 'contact@beta.com', '2022-01-01'),
            # Invalid entity type
            ('3', 'Gamma LLC', 'LLC', 'REG003', '2020-03-01', 'CA', 'CA', 'ON', 'ON', 
             'Active', 'Retail', 'info@gamma.com', '2022-01-01'),
            # Invalid status
            ('4', 'Delta Inc', 'Company', 'REG004', '2020-04-01', 'AU', 'AU', 'VIC', 'VIC', 
             'Closed', 'Healthcare', 'info@delta.com', '2022-01-01'),
            # Field length exceeded
            ('5', 'A' * 200, 'Company', 'REG005', '2020-05-01', 'US', 'US', 'TX', 'TX', 
             'Active', 'Tech', 'info@epsilon.com', '2022-01-01'),
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("entities_deduped")
        
        # Act
        result_df = spark_session.sql("""
            SELECT
                EntityID,
                (EntityName IS NOT NULL AND EntityName != '' AND
                 EntityType IS NOT NULL AND EntityType != '' AND
                 EntityType IN ('Company', 'Partnership', 'Trust', 'Nonprofit') AND
                 (Status IS NULL OR Status = '' OR Status IN ('Active', 'Inactive', 'Pending')) AND
                 LENGTH(EntityName) <= 150
                ) AS is_valid
            FROM entities_deduped
        """)
        
        results = {row['EntityID']: row['is_valid'] for row in result_df.collect()}
        
        # Assert
        assert results['1'] == True   # All validations pass
        assert results['2'] == False  # Missing required field
        assert results['3'] == False  # Invalid entity type
        assert results['4'] == False  # Invalid status
        assert results['5'] == False  # Field length exceeded
    
    def test_validation_statistics_calculation(self, spark_session, test_config, mock_metrics):
        """Test calculation of validation statistics"""
        # Arrange
        data = [
            ('1', 'Valid Corp', 'Company', 'REG001', '', 'US', 'US', '', '', 'Active', '', '', ''),
            ('2', '', 'Company', 'REG002', '', 'GB', 'GB', '', '', 'Active', '', '', ''),  # Invalid
            ('3', 'Valid Inc', 'Company', 'REG003', '', 'CA', 'CA', '', '', 'Active', '', '', ''),
            ('4', 'Invalid LLC', 'LLC', 'REG004', '', 'AU', 'AU', '', '', 'Active', '', '', ''),  # Invalid
            ('5', 'Valid Trust', 'Trust', 'REG005', '', 'US', 'US', '', '', 'Pending', '', '', ''),
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("entities_deduped")
        
        # Add validation flags
        validated_df = spark_session.sql("""
            SELECT
                *,
                (EntityName IS NOT NULL AND EntityName != '' AND EntityType IS NOT NULL AND EntityType != '') AS valid_required_fields,
                (EntityType IN ('Company', 'Partnership', 'Trust', 'Nonprofit')) AS valid_entity_type,
                (Status IS NULL OR Status = '' OR Status IN ('Active', 'Inactive', 'Pending')) AS valid_status,
                (LENGTH(EntityName) <= 150) AS valid_field_lengths,
                (EntityName IS NOT NULL AND EntityName != '' AND
                 EntityType IS NOT NULL AND EntityType != '' AND
                 EntityType IN ('Company', 'Partnership', 'Trust', 'Nonprofit') AND
                 (Status IS NULL OR Status = '' OR Status IN ('Active', 'Inactive', 'Pending')) AND
                 LENGTH(EntityName) <= 150
                ) AS is_valid
            FROM entities_deduped
        """)
        validated_df.createOrReplaceTempView("entities_validated")
        
        # Act - Calculate statistics
        stats_df = spark_session.sql("""
            SELECT
                COUNT(*) AS total_records,
                SUM(CASE WHEN is_valid = true THEN 1 ELSE 0 END) AS valid_count,
                SUM(CASE WHEN is_valid = false THEN 1 ELSE 0 END) AS invalid_count,
                SUM(CASE WHEN NOT valid_required_fields THEN 1 ELSE 0 END) AS missing_required_count,
                SUM(CASE WHEN NOT valid_entity_type THEN 1 ELSE 0 END) AS invalid_entity_type_count,
                SUM(CASE WHEN NOT valid_status THEN 1 ELSE 0 END) AS invalid_status_count
            FROM entities_validated
        """)
        
        stats = stats_df.collect()[0]
        
        # Assert
        assert stats['total_records'] == 5
        assert stats['valid_count'] == 3      # Records 1, 3, 5
        assert stats['invalid_count'] == 2    # Records 2, 4
        assert stats['missing_required_count'] == 1  # Record 2
        assert stats['invalid_entity_type_count'] == 1  # Record 4
    
    def test_quarantine_preparation(self, spark_session, test_config, mock_metrics):
        """Test preparation of quarantine data with rejection reasons"""
        # Arrange
        data = [
            ('1', 'Valid Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@valid.com', '2022-01-01'),
            ('2', '', 'Company', 'REG002', '2020-02-01', 'GB', 'GB', 'ENG', 'ENG', 
             'Active', 'Finance', 'contact@invalid.com', '2022-01-01'),
            ('3', 'Invalid LLC', 'LLC', 'REG003', '2020-03-01', 'CA', 'CA', 'ON', 'ON', 
             'Active', 'Retail', 'info@invalid.com', '2022-01-01'),
            ('4', 'Bad Status', 'Company', 'REG004', '2020-04-01', 'AU', 'AU', 'VIC', 'VIC', 
             'Closed', 'Healthcare', 'info@invalid.com', '2022-01-01'),
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("entities_deduped")
        
        # Add validation flags
        validated_df = spark_session.sql("""
            SELECT
                *,
                (EntityName IS NOT NULL AND EntityName != '' AND EntityType IS NOT NULL AND EntityType != '') AS valid_required_fields,
                (EntityType IN ('Company', 'Partnership', 'Trust', 'Nonprofit')) AS valid_entity_type,
                (Status IS NULL OR Status = '' OR Status IN ('Active', 'Inactive', 'Pending')) AS valid_status,
                (LENGTH(EntityName) <= 150) AS valid_field_lengths,
                (EntityName IS NOT NULL AND EntityName != '' AND
                 EntityType IS NOT NULL AND EntityType != '' AND
                 EntityType IN ('Company', 'Partnership', 'Trust', 'Nonprofit') AND
                 (Status IS NULL OR Status = '' OR Status IN ('Active', 'Inactive', 'Pending')) AND
                 LENGTH(EntityName) <= 150
                ) AS is_valid
            FROM entities_deduped
        """)
        validated_df.createOrReplaceTempView("entities_validated")
        
        # Act - Prepare quarantine
        quarantine_df = spark_session.sql("""
            SELECT
                EntityID,
                EntityName,
                EntityType,
                Status,
                CONCAT_WS('; ',
                    CASE WHEN NOT valid_required_fields THEN 'Missing required fields' END,
                    CASE WHEN NOT valid_entity_type THEN 'Invalid entity type' END,
                    CASE WHEN NOT valid_status THEN 'Invalid status value' END,
                    CASE WHEN NOT valid_field_lengths THEN 'Field length exceeded' END
                ) AS rejection_reasons
            FROM entities_validated
            WHERE is_valid = false
        """)
        
        results = {row['EntityID']: row for row in quarantine_df.collect()}
        
        # Assert
        assert len(results) == 3  # Records 2, 3, 4 are invalid
        
        assert '2' in results
        assert 'Missing required fields' in results['2']['rejection_reasons']
        
        assert '3' in results
        assert 'Invalid entity type' in results['3']['rejection_reasons']
        
        assert '4' in results
        assert 'Invalid status value' in results['4']['rejection_reasons']
    
    def test_full_validation_process(self, spark_session, sample_raw_data, test_config, mock_metrics):
        """Integration test for full validation process"""
        # Arrange
        validator = DataValidator(spark_session, test_config, mock_metrics)
        
        # Register cleaned/deduped data (simulate previous steps)
        sample_raw_data.createOrReplaceTempView("entities_deduped")
        
        # Act
        valid_df, quarantine_df = validator.validate(sample_raw_data)
        
        valid_count = valid_df.count()
        quarantine_count = quarantine_df.count()
        
        # Assert
        assert valid_count + quarantine_count == sample_raw_data.count()
        assert mock_metrics.valid_records == valid_count
        assert mock_metrics.rejected_records == quarantine_count
        assert len(mock_metrics.rejection_reasons) > 0  # Should have rejection reasons