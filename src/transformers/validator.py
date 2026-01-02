"""
===============================================================================
FILE: src/transformers/validator.py
===============================================================================
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, when
import logging
from typing import Dict, Any, Tuple

from src.utils.sql_loader import SparkSQLExecutor


class DataValidator:
    """SQL-based validation"""
    
    def __init__(self, spark, config: Dict[str, Any], metrics):
        self.spark = spark
        self.config = config
        self.metrics = metrics
        self.logger = logging.getLogger('ETL_Pipeline.Validator')
        self.sql_executor = SparkSQLExecutor(spark, self.logger)
        
    def validate(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """Validate using SQL"""
        self.logger.info("Starting validation using SQL...")
        
        # Add validation flags
        self.sql_executor.register_temp_view(df, "entities_validation_flag")

        self.logger.info("Adding validation flags...")
        df = self.sql_executor.execute_sql_file(
            "03_validation/add_validation_flags.sql"
        )
        self.sql_executor.register_temp_view(df, "entities_validated")
        
        # Calculate validation statistics
        self.logger.info("Calculating validation statistics...")
        stats = self.sql_executor.execute_sql_file(
            "03_validation/calculate_rejection_stats.sql"
        ).collect()[0]
        
        self.metrics.valid_records = stats['valid_count']
        self.metrics.rejected_records = stats['invalid_count']
        
        # Update rejection reasons
        if stats['missing_required_count'] > 0:
            self.metrics.add_rejection('Missing required fields')
        if stats['invalid_entity_type_count'] > 0:
            self.metrics.add_rejection('Invalid entity type')
        if stats['invalid_status_count'] > 0:
            self.metrics.add_rejection('Invalid status')
        if stats['field_length_exceeded_count'] > 0:
            self.metrics.add_rejection('Field length exceeded')
        # if stats['date_logic_error_count'] > 0:
        #     self.metrics.add_rejection('Date logic error')
        # if stats['country_state_error_count'] > 0:
        #     self.metrics.add_rejection('Country-State inconsistency')
        
        self.logger.info(f"Valid: {self.metrics.valid_records}, "
                        f"Rejected: {self.metrics.rejected_records}")
        
        # Split into valid and quarantine
        valid_df = df.filter(col("is_valid") == True)
        quarantine_df = self.sql_executor.execute_sql_file(
            "03_validation/prepare_quarantine.sql"
        )
        
        return valid_df, quarantine_df