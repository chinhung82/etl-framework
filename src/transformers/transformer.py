from pyspark.sql import DataFrame
import logging
from typing import Dict, Any

from src.utils.sql_loader import SparkSQLExecutor


class DataTransformer:
    
    def __init__(self, spark, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger('ETL_Pipeline.Transformer')
        self.sql_executor = SparkSQLExecutor(spark, self.logger)
        
    def transform(self, df: DataFrame) -> DataFrame:
        """Data transformation"""
        self.logger.info("Starting transformation")
        
        # Map to target schema
        self.logger.info("Mapping to target schema...")
        df = self.sql_executor.execute_sql_file(
            "04_transformation/map_to_target_schema.sql"
        )
        self.sql_executor.register_temp_view(df, "entities_transformed")
        
        # Select final columns
        self.logger.info("Selecting final columns...")
        df = self.sql_executor.execute_sql_file(
            "04_transformation/select_final_columns.sql"
        )
        
        self.logger.info("Transformation completed")
        return df