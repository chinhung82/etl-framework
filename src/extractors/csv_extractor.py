"""
Data Extraction Module
Handles reading data from CSV files and initial schema enforcement
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
import logging
from typing import Dict, Any


class DataExtractor:
    """
    Handles data extraction from source files
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize DataExtractor
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger('ETL_Pipeline.Extractor')
        
    def define_source_schema(self) -> StructType:
        """
        Define the schema for CSV source data
        
        Returns:
            StructType defining the expected CSV schema
        """
        return StructType([
            StructField("EntityID", StringType(), True),
            StructField("EntityName", StringType(), True),
            StructField("EntityType", StringType(), True),
            StructField("RegistrationNumber", StringType(), True),
            StructField("IncorporationDate", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("CountryCode", StringType(), True),
            StructField("State", StringType(), True),
            StructField("StateCode", StringType(), True),
            StructField("Status", StringType(), True),
            StructField("Industry", StringType(), True),
            StructField("ContactEmail", StringType(), True),
            StructField("LastUpdate", StringType(), True)
        ])
    
    def extract_data(self) -> DataFrame:
        """
        Extract data from CSV file
        
        Returns:
            DataFrame containing the raw extracted data
            
        Raises:
            Exception: If file cannot be read or doesn't exist
        """
        source_config = self.config['source']
        file_path = source_config['file_path']
        
        self.logger.info(f"Starting data extraction from: {file_path}")
        
        try:
            # Define schema for consistent data types
            schema = self.define_source_schema()
            
            # Read CSV file with explicit schema
            df = self.spark.read \
                .format(source_config.get('file_format', 'csv')) \
                .option("header", source_config.get('header', True)) \
                .option("delimiter", source_config.get('delimiter', ',')) \
                .option("encoding", source_config.get('encoding', 'UTF-8')) \
                .option("mode", "PERMISSIVE") \
                .schema(schema) \
                .load(file_path)
            
            record_count = df.count()
            self.logger.info(f"Successfully extracted {record_count} records from source")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting data from {file_path}: {str(e)}")
            raise
    
    def validate_extraction(self, df: DataFrame) -> bool:
        """
        Validate that extracted data meets basic requirements
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if validation passes, False otherwise
        """
        self.logger.info("Validating extracted data...")
        
        # Check if DataFrame is empty
        if df.count() == 0:
            self.logger.error("Extracted DataFrame is empty")
            return False
        
        # Check if expected columns are present
        expected_columns = [
            "EntityID", "EntityName", "EntityType", "RegistrationNumber",
            "IncorporationDate", "Country", "CountryCode", "State",
            "StateCode", "Status", "Industry", "ContactEmail", "LastUpdate"
        ]
        
        actual_columns = df.columns
        missing_columns = set(expected_columns) - set(actual_columns)
        
        if missing_columns:
            self.logger.error(f"Missing expected columns: {missing_columns}")
            return False
        
        self.logger.info("Extraction validation passed")
        return True
    
    def get_extraction_summary(self, df: DataFrame) -> Dict[str, Any]:
        """
        Generate summary statistics for extracted data
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary containing extraction summary
        """
        summary = {
            'total_records': df.count(),
            'total_columns': len(df.columns),
            'columns': df.columns
        }
        
        # Calculate null counts for each column
        null_counts = {}
        for col in df.columns:
            null_count = df.filter(df[col].isNull() | (df[col] == "")).count()
            null_counts[col] = null_count
        
        summary['null_counts'] = null_counts
        
        self.logger.info("Extraction summary generated")
        return summary