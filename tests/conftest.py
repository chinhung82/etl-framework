import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import tempfile
import shutil
from datetime import datetime
import yaml

@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing"""
    spark = SparkSession.builder \
        .appName("ETL_Test") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    spark.stop()

#.config("spark.jars", "libs/mysql-connector-j-9.5.0.jar") \

@pytest.fixture(scope="session")
def test_config():
    """Create test configuration"""
    config = {
        'source': {
            'file_path': 'test_data.csv',
            'file_format': 'csv',
            'header': True,
            'delimiter': ',',
            'encoding': 'UTF-8'
        },
        'database': {
            'host': 'localhost',
            'port': 3306,
            'database': 'test_ems',
            'table': 'test_entities',
            'user': 'testuser',
            'password': 'testpass'
        },
        'output': {
            'quarantine_path': 'test_output/quarantine',
            'quality_report_path': 'test_output/quality_reports',
            'processed_path': 'test_output/processed',
            'log_path': 'test_logs'
        },
        'data_quality': {
            'required_fields': ['entity_name', 'entity_type'],
            'valid_entity_types': ['Company', 'Partnership', 'Trust', 'Nonprofit'],
            'valid_statuses': ['Active', 'Inactive', 'Pending'],
            'max_lengths': {
                'entity_name': 150,
                'entity_type': 30,
                'registration_number': 50,
                'country_code': 3,
                'state_code': 50,
                'status': 30,
                'industry': 100,
                'contact_email': 100
            }
        },
        'date_formats': {
            'input_formats_IncorporationDate': [
                'yyyy-MM-dd', 'M/d/yy', 'M/d/yyyy', 'd/M/yy',
                'dd/MM/yyyy', 'MM-dd-yy', 'MM-dd-yyyy', 'd-MMM-yy'
            ],
            'input_formats_LastUpdate': ['M/d/yy'],
            'output_format': 'yyyy-MM-dd',
            'ambiguity_resolution': {
                'priority': 'month_first',
                'year_threshold': 50
            }
        },
        'country_mapping': {
            'United States': 'US',
            'US': 'US',
            'USA': 'US',
            'United Kingdom': 'GB',
            'UK': 'GB',
            'Canada': 'CA',
            'CA': 'CA',
            'Australia': 'AU',
            'Singapore': 'SG',
            'Germany': 'DE',
            'India': 'IN',
            'Malaysia': 'MY'
        },
        'state_mapping': {
            'California': 'CA',
            'New York': 'NY',
            'Texas': 'TX',
            'Florida': 'FL',
            'Illinois': 'IL',
            'Ontario': 'ON',
            'Quebec': 'QBC',
            'British Columbia': 'BC',
            'England': 'ENG',
            'Scotland': 'SCT',
            'Wales': 'WLS',
            'Northern Ireland': 'NIR',
            'New South Wales': 'NSW',
            'Victoria': 'VIC'
        },
        'status_mapping': {
            'Active': 'Active',
            'Inactive': 'Inactive',
            'Pending': 'Pending',
            'Actived': 'Active',
            'N': 'Inactive',
            'Y': 'Active'
        },
        'email_regex_pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        'deduplication': {
            'strategy': 'composite',
            'match_criteria': {
                'entity_name_weight': 0.4,
                'registration_number_weight': 0.6
            },
            'fuzzy_threshold': 0.85,
            'keep_preference': 'most_complete'
        }
    }
    return config

@pytest.fixture
def temp_dir():
    """Create temporary directory for test outputs"""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)

@pytest.fixture
def sample_raw_data(spark_session):
    """Create sample raw CSV data for testing"""
    data = [
        ('1001', 'Acme Manufacturing', 'Company', 'REG10234', '5/12/10', 
         'United States', 'US', 'California', 'CA', 'Active', 'Manufacturing', 
         'info@acmemfg.com', '6/15/22'),
        ('1002', '  FastFinance Ltd  ', 'Company', 'REG56432', '12/6/14', 
         'Singapore', 'SG', '', 'SG', 'Inactive', 'Finance', 
         'contact@fastfin.sg', '2/1/22'),
        ('1003', 'Pineview Foundation', 'Nonprofit', 'REG90088', '7/30/11', 
         'United Kingdom', 'GB', 'England', 'ENG', 'Pending', 'Charity', 
         '', '1/10/22'),
        ('1004', 'Acme Manufacturing', 'Company', 'REG10234', '12/5/10', 
         'United States', 'US', 'California', 'CA', 'Active', 'Manufacturing', 
         'info@acmemfg.com', ''),
        ('1005', '', 'Partnership', '', '12/1/14', 
         'United States', 'US', 'New York', 'NY', 'Active', 'Retail', 
         'eastside@example.com', '1/25/22'),
        ('1006', 'GreenRidge Trust', 'Trust', 'REG88722', '', 
         'Canada', 'CA', 'Ontario', 'ON', 'Active', 'Real Estate', 
         'trust@greenridge.com', '3/11/22'),
        ('1007', 'Invalid Status Co', 'Company', 'REG99999', '1/1/20', 
         'US', 'US', '', '', 'Actived', 'Technology', 
         'info@invalid.com', '1/1/22'),
        ('1008', 'Bad Email Corp', 'Company', 'REG88888', '2/2/20', 
         'USA', 'US', 'Texas', 'TX', 'Y', 'Trading', 
         'bad-email', '2/2/22'),
    ]
    
    columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
               'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
               'Status', 'Industry', 'ContactEmail', 'LastUpdate']
    
    return spark_session.createDataFrame(data, columns)

@pytest.fixture
def duplicate_test_data(spark_session):
    """Create test data with known duplicates"""
    data = [
        # Exact duplicates (same RegNum + Name)
        ('1001', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 
         'US', 'US', 'CA', 'CA', 'Active', 'Tech', 'info@acme.com', '2022-01-01'),
        ('1002', 'Acme Corp', 'Company', 'REG001', '2020-01-01', 
         'US', 'US', 'CA', 'CA', 'Active', 'Tech', 'info@acme.com', '2022-06-01'),  # Later date
        
        # Same RegNum, different names (keep most complete)
        ('1003', 'Beta Inc', 'Company', 'REG002', '2020-02-01', 
         'US', 'US', 'NY', 'NY', 'Active', 'Finance', '', '2022-01-01'),
        ('1004', 'Beta Incorporated', 'Company', 'REG002', '2020-02-01', 
         'US', 'US', 'NY', 'NY', 'Active', 'Finance', 'contact@beta.com', '2022-01-01'),
        
        # Same name, no RegNum (keep most complete)
        ('1005', 'Gamma LLC', 'Partnership', '', '2020-03-01', 
         'CA', 'CA', 'ON', 'ON', 'Active', 'Retail', 'info@gamma.com', '2022-01-01'),
        ('1006', 'Gamma LLC', 'Partnership', '', '2020-03-01', 
         'CA', 'CA', '', '', 'Active', '', '', '2022-01-01'),
        
        # Unique records (no duplicates)
        ('1007', 'Delta Co', 'Company', 'REG003', '2020-04-01', 
         'GB', 'GB', 'ENG', 'ENG', 'Pending', 'Healthcare', 'info@delta.co.uk', '2022-01-01'),
    ]
    
    columns = ['EntityID', 'EntityName', 'EntityType', 'RegistrationNumber', 
               'IncorporationDate', 'Country', 'CountryCode', 'State', 'StateCode', 
               'Status', 'Industry', 'ContactEmail', 'LastUpdate']
    
    return spark_session.createDataFrame(data, columns)

@pytest.fixture
def mock_metrics():
    """Create mock ETL metrics object"""
    class MockMetrics:
        def __init__(self):
            self.total_records = 0
            self.valid_records = 0
            self.rejected_records = 0
            self.duplicate_records = 0
            self.date_format_fixes = 0
            self.country_standardizations = 0
            self.state_standardizations = 0
            self.rejection_reasons = []
        
        def add_rejection(self, reason):
            self.rejection_reasons.append(reason)
        
        def get_summary(self):
            return {
                'total_records': self.total_records,
                'valid_records': self.valid_records,
                'rejected_records': self.rejected_records,
                'duplicate_records': self.duplicate_records,
                'success_rate': f"{(self.valid_records/self.total_records*100):.2f}%" if self.total_records > 0 else "0%"
            }
    
    return MockMetrics()