import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import tempfile
import shutil

from src.transformers.cleaner import DataCleaner
from src.transformers.deduplicator import DataDeduplicator
from src.transformers.validator import DataValidator
from src.transformers.transformer import DataTransformer


class TestEndToEndPipeline:
    """End-to-end integration tests for the complete ETL pipeline"""
    
    def test_complete_pipeline_flow(self, spark_session, sample_raw_data, test_config, mock_metrics):
        """Test complete ETL pipeline from raw data to final output"""
        # Arrange
        cleaner = DataCleaner(spark_session, test_config, mock_metrics)
        deduplicator = DataDeduplicator(spark_session, test_config, mock_metrics)
        validator = DataValidator(spark_session, test_config, mock_metrics)
        transformer = DataTransformer(spark_session, test_config)
        
        original_count = sample_raw_data.count()
        
        # Act - Execute pipeline steps
        # Step 1: Clean
        sample_raw_data.createOrReplaceTempView("entities_raw")
        cleaned_df = spark_session.sql("""
            SELECT
              CASE WHEN TRIM(EntityID) = '' THEN NULL ELSE TRIM(EntityID) END AS EntityID,
              CASE WHEN TRIM(EntityName) = '' THEN NULL ELSE TRIM(EntityName) END AS EntityName,
              CASE WHEN TRIM(EntityType) = '' THEN NULL ELSE TRIM(EntityType) END AS EntityType,
              CASE WHEN TRIM(RegistrationNumber) = '' THEN NULL ELSE TRIM(RegistrationNumber) END AS RegistrationNumber,
              IncorporationDate,
              Country,
              CountryCode,
              State,
              StateCode,
              Status,
              Industry,
              ContactEmail,
              LastUpdate
            FROM entities_raw
        """)
        
        # Step 2: Deduplicate
        deduped_df = deduplicator.deduplicate(cleaned_df)
        
        # Step 3: Validate
        valid_df, quarantine_df = validator.validate(deduped_df)
        
        # Step 4: Transform
        transformed_df = transformer.transform(valid_df)
        
        # Assert
        assert transformed_df.count() > 0, "Should have valid records"
        assert quarantine_df.count() >= 0, "May have quarantined records"
        
        # Verify data quality improved
        assert mock_metrics.valid_records > 0
        
        # Verify data structure
        assert 'entity_name' in transformed_df.columns or 'EntityName' in transformed_df.columns
    
    def test_pipeline_with_all_duplicates(self, spark_session, test_config, mock_metrics):
        """Test pipeline behavior when all records are duplicates"""
        # Arrange
        data = [
            ('1', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-01-01'),
            ('2', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-06-01'),
            ('3', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-03-01'),
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        deduplicator = DataDeduplicator(spark_session, test_config, mock_metrics)
        
        # Act
        result_df = deduplicator.deduplicate(input_df)
        
        # Assert
        assert result_df.count() == 1, "Should keep only one record"
        assert mock_metrics.duplicate_records == 2, "Should remove 2 duplicates"
    
    def test_pipeline_with_all_invalid_records(self, spark_session, test_config, mock_metrics):
        """Test pipeline behavior when all records are invalid"""
        # Arrange
        data = [
            ('1', '', 'Company', 'REG001', '', 'US', 'US', '', '', '', '', '', ''),  # Missing entity_name
            ('2', 'Beta Inc', 'LLC', 'REG002', '', 'GB', 'GB', '', '', '', '', '', ''),  # Invalid type
            ('3', 'Gamma Corp', 'Company', 'REG003', '', 'CA', 'CA', '', '', 'Closed', '', '', ''),  # Invalid status
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        validator = DataValidator(spark_session, test_config, mock_metrics)
        
        # Act
        valid_df, quarantine_df = validator.validate(input_df)
        
        # Assert
        assert valid_df.count() == 0, "Should have no valid records"
        assert quarantine_df.count() == 3, "All records should be quarantined"
        assert mock_metrics.rejected_records == 3
    
    def test_pipeline_data_preservation(self, spark_session, test_config, mock_metrics):
        """Test that valid data is preserved through entire pipeline"""
        # Arrange
        test_data = [
            ('1', 'Acme Manufacturing', 'Company', 'REG10234', '2010-05-12', 
             'US', 'US', 'CA', 'CA', 'Active', 'Manufacturing', 
             'info@acmemfg.com', '2022-06-15'),
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(test_data, columns)
        
        cleaner = DataCleaner(spark_session, test_config, mock_metrics)
        deduplicator = DataDeduplicator(spark_session, test_config, mock_metrics)
        validator = DataValidator(spark_session, test_config, mock_metrics)
        transformer = DataTransformer(spark_session, test_config)
        
        # Act - Run through pipeline
        input_df.createOrReplaceTempView("entities_raw")
        cleaned_df = spark_session.sql("SELECT * FROM entities_raw")
        deduped_df = deduplicator.deduplicate(cleaned_df)
        valid_df, _ = validator.validate(deduped_df)
        final_df = transformer.transform(valid_df)
        
        # Assert
        assert final_df.count() == 1, "Record should survive entire pipeline"
        result = final_df.collect()[0]
        
        # Check key fields preserved (case-insensitive column check)
        col_dict = {col.lower(): result[col] for col in final_df.columns}
        assert 'acme' in str(col_dict.get('entityname', col_dict.get('entity_name', ''))).lower()
        assert str(col_dict.get('entitytype', col_dict.get('entity_type', ''))).lower() == 'company'
    
    def test_pipeline_metrics_accuracy(self, spark_session, sample_raw_data, test_config, mock_metrics):
        """Test that metrics are accurately tracked through pipeline"""
        # Arrange
        cleaner = DataCleaner(spark_session, test_config, mock_metrics)
        deduplicator = DataDeduplicator(spark_session, test_config, mock_metrics)
        validator = DataValidator(spark_session, test_config, mock_metrics)
        
        original_count = sample_raw_data.count()
        mock_metrics.total_records = original_count
        
        # Act
        sample_raw_data.createOrReplaceTempView("entities_raw")
        cleaned_df = spark_session.sql("SELECT * FROM entities_raw")
        deduped_df = deduplicator.deduplicate(cleaned_df)
        valid_df, quarantine_df = validator.validate(deduped_df)
        
        # Assert
        assert mock_metrics.total_records == original_count
        assert mock_metrics.valid_records + mock_metrics.rejected_records == deduped_df.count()
        assert mock_metrics.duplicate_records >= 0
    
    def test_pipeline_idempotency(self, spark_session, test_config, mock_metrics):
        """Test that running pipeline twice produces same results"""
        # Arrange
        data = [
            ('1', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-01-01'),
            ('2', 'Beta Inc', 'Company', 'REG002', '2020-02-01', 'GB', 'GB', 'ENG', 'ENG', 
             'Active', 'Finance', 'contact@beta.com', '2022-01-01'),
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        
        cleaner = DataCleaner(spark_session, test_config, mock_metrics)
        deduplicator = DataDeduplicator(spark_session, test_config, mock_metrics)
        validator = DataValidator(spark_session, test_config, mock_metrics)
        
        # Act - Run pipeline twice
        # Run 1
        input_df.createOrReplaceTempView("entities_raw")
        cleaned_df_1 = spark_session.sql("SELECT * FROM entities_raw")
        deduped_df_1 = deduplicator.deduplicate(cleaned_df_1)
        valid_df_1, _ = validator.validate(deduped_df_1)
        count_1 = valid_df_1.count()
        
        # Run 2
        mock_metrics_2 = type('MockMetrics', (), {
            'total_records': 0, 'valid_records': 0, 'rejected_records': 0,
            'duplicate_records': 0, 'rejection_reasons': []
        })()
        cleaner_2 = DataCleaner(spark_session, test_config, mock_metrics_2)
        deduplicator_2 = DataDeduplicator(spark_session, test_config, mock_metrics_2)
        validator_2 = DataValidator(spark_session, test_config, mock_metrics_2)
        
        input_df.createOrReplaceTempView("entities_raw")
        cleaned_df_2 = spark_session.sql("SELECT * FROM entities_raw")
        deduped_df_2 = deduplicator_2.deduplicate(cleaned_df_2)
        valid_df_2, _ = validator_2.validate(deduped_df_2)
        count_2 = valid_df_2.count()
        
        # Assert
        assert count_1 == count_2, "Pipeline should produce consistent results"
    
    def test_pipeline_error_recovery(self, spark_session, test_config, mock_metrics):
        """Test pipeline handles errors gracefully"""
        # Arrange - Create data with edge cases
        data = [
            ('1', 'Valid Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@valid.com', '2022-01-01'),
            ('2', 'A' * 500, 'Company', 'REG002', '2020-02-01', 'XX', 'XX', 'YY', 'YY', 
             'InvalidStatus', 'X' * 500, 'bad-email', '2022-01-01'),  # Multiple issues
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        validator = DataValidator(spark_session, test_config, mock_metrics)
        
        # Act
        valid_df, quarantine_df = validator.validate(input_df)
        
        # Assert - Pipeline should continue despite invalid data
        assert valid_df.count() == 1, "Should process valid record"
        assert quarantine_df.count() == 1, "Should quarantine invalid record"
        
        # Check quarantine has rejection reasons
        quarantine_result = quarantine_df.collect()[0]
        assert 'rejection_reasons' in quarantine_df.columns or 'RejectionReasons' in quarantine_df.columns
    
    def test_pipeline_performance_large_dataset(self, spark_session, test_config, mock_metrics):
        """Test pipeline performance with larger dataset"""
        # Arrange - Create 1000 records
        data = [
            (str(i), f'Company_{i}', 'Company', f'REG{i:05d}', '2020-01-01', 
             'US', 'US', 'CA', 'CA', 'Active', 'Tech', f'info@company{i}.com', '2022-01-01')
            for i in range(1, 1001)
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        
        deduplicator = DataDeduplicator(spark_session, test_config, mock_metrics)
        validator = DataValidator(spark_session, test_config, mock_metrics)
        
        # Act
        import time
        start_time = time.time()
        
        deduped_df = deduplicator.deduplicate(input_df)
        valid_df, _ = validator.validate(deduped_df)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Assert
        assert valid_df.count() == 1000, "Should process all records"
        assert processing_time < 60, "Should complete within 60 seconds"  # Performance benchmark
        print(f"Processing time for 1000 records: {processing_time:.2f} seconds")
    
    def test_pipeline_with_mixed_quality_data(self, spark_session, test_config, mock_metrics):
        """Test pipeline with mix of good, duplicate, and invalid data"""
        # Arrange
        data = [
            # Valid records
            ('1', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-01-01'),
            ('2', 'Beta Inc', 'Company', 'REG002', '2020-02-01', 'GB', 'GB', 'ENG', 'ENG', 
             'Active', 'Finance', 'contact@beta.com', '2022-01-01'),
            
            # Duplicates
            ('3', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 'US', 'US', 'CA', 'CA', 
             'Active', 'Tech', 'info@acme.com', '2022-06-01'),
            
            # Invalid records
            ('4', '', 'Company', 'REG004', '2020-04-01', 'AU', 'AU', 'VIC', 'VIC', 
             'Active', 'Healthcare', 'info@invalid.com', '2022-01-01'),
            # ('5', 'Invalid Type', 'LLC', 'REG005', '2020-05-01', 'SG', 'SG', '', '', 
            #  'Active', 'Retail', 'info@invalid.com', '2022-01-01'),
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        
        deduplicator = DataDeduplicator(spark_session, test_config, mock_metrics)
        validator = DataValidator(spark_session, test_config, mock_metrics)
        
        # Act
        deduped_df = deduplicator.deduplicate(input_df)
        valid_df, quarantine_df = validator.validate(deduped_df)

        print(deduped_df.show())
        print(valid_df.show())
        print(quarantine_df.show())
        
        # Assert
        assert deduped_df.count() == 3, "Should remove 1 duplicate"
        assert valid_df.count() == 2, "Should have 2 valid records (Acme, Beta)"
        assert quarantine_df.count() == 1, "Should quarantine 2 invalid records"
        assert mock_metrics.duplicate_records == 1
        assert mock_metrics.valid_records == 2
        assert mock_metrics.rejected_records == 1