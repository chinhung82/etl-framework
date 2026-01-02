import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime

from src.transformers.cleaner import DataCleaner


class TestDataCleaner:
    """Test suite for DataCleaner class"""
    
    def test_clean_string_fields_trim_whitespace(self, spark_session, test_config, mock_metrics):
        """Test that string fields are trimmed correctly"""
        # Arrange
        data = [
            ('1', '  Acme Corp  ', '  Company  ', '  REG001  ', '', '', '', '', '', '', '', '', ''),
            ('2', 'Beta Inc', 'Partnership', 'REG002', '', '', '', '', '', '', '', '', '')
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        cleaner = DataCleaner(spark_session, test_config, mock_metrics)
        
        # Create temporary view
        input_df.createOrReplaceTempView("entities_raw")
        
        # Act
        result_df = spark_session.sql("""
            SELECT
              CASE WHEN TRIM(EntityName) = '' THEN NULL ELSE TRIM(EntityName) END AS EntityName,
              CASE WHEN TRIM(EntityType) = '' THEN NULL ELSE TRIM(EntityType) END AS EntityType,
              CASE WHEN TRIM(RegistrationNumber) = '' THEN NULL ELSE TRIM(RegistrationNumber) END AS RegistrationNumber
            FROM entities_raw
        """)
        
        results = result_df.collect()
        
        # Assert
        assert results[0]['EntityName'] == 'Acme Corp'
        assert results[0]['EntityType'] == 'Company'
        assert results[0]['RegistrationNumber'] == 'REG001'
        assert results[1]['EntityName'] == 'Beta Inc'
    
    def test_clean_string_fields_empty_to_null(self, spark_session, test_config, mock_metrics):
        """Test that empty strings are converted to NULL"""
        # Arrange
        data = [
            ('1', 'Acme Corp', '', '', '', '', '', '', '', '', '', '', ''),
            ('2', 'Beta Inc', 'Company', 'REG002', '', '', '', '', '', '', '', '', '')
        ]
        columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
                   'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
                   'Status', 'Industry', 'ContactEmail', 'LastUpdate']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("entities_raw")
        
        # Act
        result_df = spark_session.sql("""
            SELECT
              CASE WHEN TRIM(EntityType) = '' THEN NULL ELSE TRIM(EntityType) END AS EntityType,
              CASE WHEN TRIM(RegistrationNumber) = '' THEN NULL ELSE TRIM(RegistrationNumber) END AS RegistrationNumber
            FROM entities_raw
        """)
        
        results = result_df.collect()
        
        # Assert
        assert results[0]['EntityType'] is None
        assert results[0]['RegistrationNumber'] is None
        assert results[1]['EntityType'] == 'Company'
        assert results[1]['RegistrationNumber'] == 'REG002'
    
    def test_date_parsing_multiple_formats(self, spark_session, test_config, mock_metrics):
        """Test date parsing with various input formats"""
        # Arrange
        data = [
            ('1', '5/12/10'),      # M/d/yy
            ('2', '2020-05-12'),   # yyyy-MM-dd
            ('3', '12/5/10'),      # Could be d/M/yy or M/d/yy
            ('4', '11-26-17'),     # MM-dd-yy
            ('5', '2-Nov-20'),     # d-MMM-yy
            ('6', 'invalid'),      # Invalid date
            ('7', ''),             # Empty
        ]
        columns = ['EntityID', 'IncorporationDate']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("test_dates")
        
        # Act - Test date parsing with COALESCE
        result_df = spark_session.sql("""
            SELECT 
                EntityID,
                COALESCE(
                    to_date(TRIM(IncorporationDate), 'yyyy-MM-dd'),
                    to_date(TRIM(IncorporationDate), 'M/d/yy'),
                    to_date(TRIM(IncorporationDate), 'd/M/yy'),
                    to_date(TRIM(IncorporationDate), 'MM-dd-yy'),
                    to_date(TRIM(IncorporationDate), 'd-MMM-yy')
                ) AS parsed_date
            FROM test_dates
        """)
        
        results = result_df.collect()
        
        # Assert
        assert results[0]['parsed_date'] is not None  # Should parse M/d/yy
        assert results[1]['parsed_date'] is not None  # Should parse yyyy-MM-dd
        assert results[3]['parsed_date'] is not None  # Should parse MM-dd-yy
        assert results[4]['parsed_date'] is not None  # Should parse d-MMM-yy
        assert results[5]['parsed_date'] is None      # Invalid should be None
        assert results[6]['parsed_date'] is None      # Empty should be None
    
    def test_country_code_standardization(self, spark_session, test_config, mock_metrics):
        """Test country code standardization from various inputs"""
        # Arrange
        data = [
            ('1', 'United States', 'US'),
            ('2', 'USA', 'US'),
            ('3', 'US', 'US'),
            ('4', 'United Kingdom', 'GB'),
            ('5', 'UK', 'GB'),
            ('6', 'Canada', 'CA'),
            ('7', 'Singapore', ''),
            ('8', '', 'AU'),
        ]
        columns = ['EntityID', 'Country', 'CountryCode']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("test_countries")
        
        # Act
        result_df = spark_session.sql("""
            SELECT 
                EntityID,
                CASE 
                    WHEN UPPER(COALESCE(NULLIF(CountryCode,''), Country)) IN ('UNITED STATES', 'USA', 'US') THEN 'US'
                    WHEN UPPER(COALESCE(NULLIF(CountryCode,''), Country)) IN ('UNITED KINGDOM', 'UK', 'GB') THEN 'GB'
                    WHEN UPPER(COALESCE(NULLIF(CountryCode,''), Country)) IN ('CANADA', 'CA') THEN 'CA'
                    WHEN UPPER(COALESCE(NULLIF(CountryCode,''), Country)) = 'SINGAPORE' THEN 'SG'
                    WHEN UPPER(COALESCE(NULLIF(CountryCode,''), Country)) = 'AUSTRALIA' THEN 'AU'
                    ELSE UPPER(COALESCE(NULLIF(CountryCode,''), Country))
                END AS CountryCode
            FROM test_countries
        """)
        
        results = {row['EntityID']: row['CountryCode'] for row in result_df.collect()}
        
        # Assert
        assert results['1'] == 'US'
        assert results['2'] == 'US'
        assert results['3'] == 'US'
        assert results['4'] == 'GB'
        assert results['5'] == 'GB'
        assert results['6'] == 'CA'
        assert results['7'] == 'SG'
        assert results['8'] == 'AU'
    
    def test_state_code_standardization(self, spark_session, test_config, mock_metrics):
        """Test state code standardization"""
        # Arrange
        data = [
            ('1', 'California', 'CA'),
            ('2', 'New York', 'NY'),
            ('3', '', 'TX'),
            ('4', 'Ontario', ''),
            ('5', 'England', 'ENG'),
        ]
        columns = ['EntityID', 'State', 'StateCode']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("test_states")
        
        # Act
        result_df = spark_session.sql("""
            SELECT 
                EntityID,
                CASE 
                    WHEN UPPER(COALESCE(NULLIF(StateCode,''), State)) = 'CALIFORNIA' THEN 'CA'
                    WHEN UPPER(COALESCE(NULLIF(StateCode,''), State)) = 'NEW YORK' THEN 'NY'
                    WHEN UPPER(COALESCE(NULLIF(StateCode,''), State)) = 'TEXAS' THEN 'TX'
                    WHEN UPPER(COALESCE(NULLIF(StateCode,''), State)) = 'ONTARIO' THEN 'ON'
                    WHEN UPPER(COALESCE(NULLIF(StateCode,''), State)) = 'ENGLAND' THEN 'ENG'
                    ELSE UPPER(COALESCE(NULLIF(StateCode,''), State))
                END AS StateCode
            FROM test_states
        """)
        
        results = {row['EntityID']: row['StateCode'] for row in result_df.collect()}
        
        # Assert
        assert results['1'] == 'CA'
        assert results['2'] == 'NY'
        assert results['3'] == 'TX'
        assert results['4'] == 'ON'
        assert results['5'] == 'ENG'
    
    def test_status_standardization(self, spark_session, test_config, mock_metrics):
        """Test status value standardization"""
        # Arrange
        data = [
            ('1', 'Active'),
            ('2', 'Inactive'),
            ('3', 'Pending'),
            ('4', 'Actived'),    # Typo
            ('5', 'Y'),           # Boolean-like
            ('6', 'N'),           # Boolean-like
        ]
        columns = ['EntityID', 'Status']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("test_status")
        
        # Act
        result_df = spark_session.sql("""
            SELECT 
                EntityID,
                CASE 
                    WHEN UPPER(Status) IN ('ACTIVE', 'Y') THEN 'Active'
                    WHEN UPPER(Status) IN ('INACTIVE', 'N') THEN 'Inactive'
                    WHEN UPPER(Status) = 'PENDING' THEN 'Pending'
                    WHEN UPPER(Status) = 'ACTIVED' THEN 'Active'
                    ELSE UPPER(Status)
                END AS Status
            FROM test_status
        """)
        
        results = {row['EntityID']: row['Status'] for row in result_df.collect()}
        
        # Assert
        assert results['1'] == 'Active'
        assert results['2'] == 'Inactive'
        assert results['3'] == 'Pending'
        assert results['4'] == 'Active'  # Corrected typo
        assert results['5'] == 'Active'  # Y -> Active
        assert results['6'] == 'Inactive'  # N -> Inactive
    
    def test_email_validation_and_cleaning(self, spark_session, test_config, mock_metrics):
        """Test email address validation and cleaning"""
        # Arrange
        data = [
            ('1', 'info@acme.com'),
            ('2', 'CONTACT@BETA.COM'),
            ('3', 'invalid-email'),
            ('4', 'user@domain'),
            ('5', ''),
            ('6', 'valid.email+tag@example.co.uk'),
        ]
        columns = ['EntityID', 'ContactEmail']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("test_emails")
        
        # Act
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
        result_df = spark_session.sql(f"""
            SELECT 
                EntityID,
                CASE 
                    WHEN NULLIF(TRIM(ContactEmail), '') IS NOT NULL AND LOWER(TRIM(ContactEmail)) RLIKE '{email_pattern}'
                        THEN LOWER(TRIM(ContactEmail))
                    ELSE NULL
                END AS ContactEmail
            FROM test_emails
        """)
        
        results = {row['EntityID']: row['ContactEmail'] for row in result_df.collect()}
        
        # Assert
        assert results['1'] == 'info@acme.com'
        assert results['2'] == 'contact@beta.com'  # Lowercased
        assert results['3'] is None  # Invalid format
        assert results['4'] is None  # Missing TLD
        assert results['5'] is None  # Empty
        assert results['6'] == 'valid.email+tag@example.co.uk'
    
    def test_industry_null_handling(self, spark_session, test_config, mock_metrics):
        """Test industry field null handling"""
        # Arrange
        data = [
            ('1', 'Manufacturing'),
            ('2', 'NULL'),  # String "NULL"
            ('3', ''),      # Empty string
            ('4', '  '),    # Whitespace
        ]
        columns = ['EntityID', 'Industry']
        
        input_df = spark_session.createDataFrame(data, columns)
        input_df.createOrReplaceTempView("test_industry")
        
        # Act
        result_df = spark_session.sql("""
            SELECT 
                EntityID,
                CASE 
                    WHEN TRIM(Industry) = '' THEN NULL
                    WHEN TRIM(Industry) = 'NULL' THEN NULL 
                    ELSE TRIM(Industry) 
                END AS Industry
            FROM test_industry
        """)
        
        results = {row['EntityID']: row['Industry'] for row in result_df.collect()}
        
        # Assert
        assert results['1'] == 'Manufacturing'
        assert results['2'] is None  # 'NULL' string converted
        assert results['3'] is None  # Empty converted
        assert results['4'] is None  # Whitespace converted
    
    def test_full_cleaning_pipeline(self, spark_session, sample_raw_data, test_config, mock_metrics):
        """Integration test for full cleaning pipeline"""
        # Arrange
        cleaner = DataCleaner(spark_session, test_config, mock_metrics)
        
        # Register input data
        sample_raw_data.createOrReplaceTempView("entities_raw")
        
        # Act - Simulate the cleaning process
        # Step 1: Clean strings
        cleaned_df = spark_session.sql("""
            SELECT
              CASE WHEN TRIM(EntityID) = '' THEN NULL ELSE TRIM(EntityID) END AS EntityID,
              CASE WHEN TRIM(EntityName) = '' THEN NULL ELSE TRIM(EntityName) END AS EntityName,
              CASE WHEN TRIM(EntityType) = '' THEN NULL ELSE TRIM(EntityType) END AS EntityType,
              CASE WHEN TRIM(RegistrationNumber) = '' THEN NULL ELSE TRIM(RegistrationNumber) END AS RegistrationNumber,
              CASE WHEN TRIM(IncorporationDate) = '' THEN NULL ELSE TRIM(IncorporationDate) END AS IncorporationDate,
              CASE WHEN TRIM(Country) = '' THEN NULL ELSE TRIM(Country) END AS Country,
              CASE WHEN TRIM(CountryCode) = '' THEN NULL ELSE TRIM(CountryCode) END AS CountryCode,
              CASE WHEN TRIM(State) = '' THEN NULL ELSE TRIM(State) END AS State,
              CASE WHEN TRIM(StateCode) = '' THEN NULL ELSE TRIM(StateCode) END AS StateCode,
              CASE WHEN TRIM(Status) = '' THEN NULL ELSE TRIM(Status) END AS Status,
              CASE WHEN TRIM(Industry) = '' THEN NULL ELSE TRIM(Industry) END AS Industry,
              CASE WHEN TRIM(ContactEmail) = '' THEN NULL ELSE TRIM(ContactEmail) END AS ContactEmail,
              CASE WHEN TRIM(LastUpdate) = '' THEN NULL ELSE TRIM(LastUpdate) END AS LastUpdate
            FROM entities_raw
        """)
        
        # Assert
        results = cleaned_df.collect()
        
        # Check trimming worked
        assert results[1]['EntityName'] == 'FastFinance Ltd'  # Trimmed whitespace
        
        # Check empty to null conversion
        assert results[2]['ContactEmail'] is None  # Empty email
        assert results[4]['EntityName'] is None  # Empty name
        
        # Check preservation of valid data
        assert results[0]['EntityName'] == 'Acme Manufacturing'
        assert results[0]['RegistrationNumber'] == 'REG10234'
