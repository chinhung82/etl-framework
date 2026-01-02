"""
Data Loading Module
Handles loading data into MySQL database and quarantine storage

Loading Strategy:
1. Valid records -> MySQL entities table using JDBC
2. Rejected records -> CSV files in quarantine directory
3. Uses batch insertion for efficiency
4. Implements error handling and retry logic
"""

from pyspark.sql import DataFrame
import logging
from typing import Dict, Any
from datetime import datetime
import os


class DataLoader:
    """
    Handles loading data to MySQL and quarantine storage
    """
    
    def __init__(self, spark, config: Dict[str, Any], metrics):
        """
        Initialize DataLoader
        
        Args:
            config: Configuration dictionary
            metrics: ETLMetrics instance for tracking operations
        """
        self.spark = spark
        self.config = config
        self.metrics = metrics
        self.logger = logging.getLogger('ETL_Pipeline.Loader')
        
    def load_to_mysql(self, df: DataFrame) -> bool:
        """
        Load data to MySQL database
        
        Args:
            df: DataFrame to load
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info("Starting MySQL data load...")
        
        db_config = self.config['database']
        
        try:
            # JDBC connection properties
            jdbc_url = self.get_jdbc_url()
            connection_properties = {
                "user": db_config['user'],
                "password": db_config['password'],
                "driver": db_config.get('driver', 'com.mysql.cj.jdbc.Driver')
            }
            
            table_name = db_config['table']
            
            # Write to MySQL
            # Mode options:
            # - 'append': Add new records
            # - 'overwrite': Replace entire table
            # - 'error': Throw error if table exists
            # - 'ignore': Silently ignore if table exists
            
            self.logger.info(f"Writing {df.count()} records to MySQL table: {table_name}")
            
            df.write \
                .jdbc(
                    url=jdbc_url,
                    table=table_name,
                    mode='append',  # Append new records to existing table
                    properties=connection_properties
                )
            
            self.logger.info(f"Successfully loaded {df.count()} records to MySQL")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading data to MySQL: {str(e)}")
            self.logger.error("Please ensure:")
            self.logger.error("1. MySQL JDBC driver is available")
            self.logger.error("2. Database credentials are correct")
            self.logger.error("3. Database and table exist")
            self.logger.error("4. Network connectivity to database server")
            return False
    
    def save_quarantine_data(self, df: DataFrame, reason: str = "validation_failed") -> bool:
        """
        Load rejected records to quarantine storage
        
        Args:
            df: DataFrame of rejected records
            reason: Reason for quarantine
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Saving {df.count()} records to quarantine...")
        
        try:
            output_config = self.config['output']
            quarantine_path = output_config.get('quarantine_path', 'output/quarantine')
            
            # Create quarantine directory if it doesn't exist
            os.makedirs(quarantine_path, exist_ok=True)
            
            # Generate timestamped filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"quarantine_{reason}_{timestamp}.csv"
            full_path = os.path.join(quarantine_path, filename)
            
            # Write to CSV with headers
            df.coalesce(1) \
                .write \
                .mode('overwrite') \
                .option('header', 'true') \
                .csv(full_path)
            
            self.logger.info(f"Quarantine data saved to: {full_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving quarantine data: {str(e)}")
            return False
    
    def save_processed_data(self, df: DataFrame, filename: str = "processed_data") -> bool:
        """
        Save processed data to file for auditing
        
        Args:
            df: DataFrame to save
            filename: Base filename (without extension)
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info("Saving processed data for audit trail...")
        
        try:
            output_config = self.config['output']
            processed_path = output_config.get('processed_path', 'output/processed')
            
            # Create directory if it doesn't exist
            os.makedirs(processed_path, exist_ok=True)
            
            # Generate timestamped filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            full_filename = f"{filename}_{timestamp}.csv"
            full_path = os.path.join(processed_path, full_filename)
            
            # OPTIMIZATION: For small data (< 1000 rows), coalesce(1) is fine
            # The slowness is because this is the FIRST action that triggers
            # the entire pipeline execution (extract, clean, dedupe, validate, transform)
            
            # Write to CSV with single file output
            # Note: This will execute the entire pipeline if not already materialized
            df.coalesce(1) \
                .write \
                .mode('overwrite') \
                .option('header', 'true') \
                .csv(full_path)
            
            self.logger.info(f"Processed data saved to: {full_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving processed data: {str(e)}")
            return False
    
    def get_jdbc_url(self) -> str:
        """
        Construct JDBC URL from configuration
        
        Returns:
            JDBC connection URL
        """
        db_config = self.config['database']
        host = db_config['host']
        port = db_config.get('port', 3306)
        database = db_config['database']
        
        # Add connection parameters for better compatibility
        jdbc_url = (
            f"jdbc:mysql://{host}:{port}/{database}"
            f"?useSSL=false"
            f"&allowPublicKeyRetrieval=true"
            f"&serverTimezone=UTC"
            f"&rewriteBatchedStatements=true"
        )
        
        return jdbc_url
    
    # def verify_load(self, expected_count: int) -> bool:
    #     """
    #     Verify that data was loaded successfully to MySQL
    #     Note: This requires reading back from the database
        
    #     Args:
    #         expected_count: Expected number of records loaded
            
    #     Returns:
    #         True if verification passes, False otherwise
    #     """
    #     self.logger.info("Verifying data load...")
        
    #     try:
    #         from pyspark.sql import SparkSession
    #         spark = SparkSession.builder.getOrCreate()
            
    #         db_config = self.config['database']
    #         jdbc_url = self.get_jdbc_url()
    #         connection_properties = {
    #             "user": db_config['user'],
    #             "password": db_config['password'],
    #             "driver": db_config.get('driver', 'com.mysql.cj.jdbc.Driver')
    #         }
            
    #         # Read count from database
    #         table_name = db_config['table']
    #         df = spark.read \
    #             .jdbc(
    #                 url=jdbc_url,
    #                 table=table_name,
    #                 properties=connection_properties
    #             )
            
    #         actual_count = df.count()
            
    #         if actual_count >= expected_count:
    #             self.logger.info(f"Load verification passed. Records in database: {actual_count}")
    #             return True
    #         else:
    #             self.logger.warning(
    #                 f"Load verification warning. Expected: {expected_count}, "
    #                 f"Found: {actual_count}"
    #             )
    #             return False
                
    #     except Exception as e:
    #         self.logger.error(f"Error verifying load: {str(e)}")
    #         return False
    
    # def get_load_summary(self) -> Dict[str, Any]:
    #     """
    #     Generate summary of load operations
        
    #     Returns:
    #         Dictionary containing load summary
    #     """
    #     summary = {
    #         'records_loaded_to_mysql': self.metrics.valid_records,
    #         'records_quarantined': self.metrics.rejected_records,
    #         'load_timestamp': datetime.now().isoformat()
    #     }
        
    #     return summary