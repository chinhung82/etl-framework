from pyspark.sql import DataFrame
from pyspark import StorageLevel
import logging
from typing import Dict, Any

from src.utils.sql_loader import SparkSQLExecutor


class DataDeduplicator:
    """
    SQL-based deduplication handler
    All logic implemented in SQL files for reusability and clarity
    """
    
    def __init__(self, spark, config: Dict[str, Any], metrics):
        """
        Initialize DataDeduplicator
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            metrics: ETLMetrics instance
        """
        self.spark = spark
        self.config = config
        self.metrics = metrics
        self.logger = logging.getLogger('ETL_Pipeline.Deduplicator')
        self.sql_executor = SparkSQLExecutor(spark, self.logger)
        

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """
        Main deduplication method using SQL-based approach
        
        Process:
        1. Calculate Completeness Score
        2. Group Records by Composite Key (RegistrationNumber + EntityName (if RegistrationNumber), or EntityName only (if no RegistrationNumber))
        3. Rank Within Each Group ORDER BY Score DESC, LastUpdate DESC, Name Length DESC, EntityID ASC
        4. Remove entity name duplicates
        
        Args:
            df: Input DataFrame (cleaned data)
            
        Returns:
            Deduplicated DataFrame
        """
        self.logger.info("=" * 80)
        self.logger.info("STARTING DEDUPLICATION PROCESS")
        self.logger.info("=" * 80)
        
        # Register input DataFrame
        self.sql_executor.register_temp_view(df, "entities_clean")
        original_count = df.count()
        self.logger.info(f"Input records: {original_count:,}")

        self.logger.info("\nStart remove duplicates...")
        df = self.sql_executor.execute_sql_file(
            "02_deduplication/remove_duplicates.sql"
        )
        
        final_count = df.count()

        # Calculate total duplicates removed
        total_removed = original_count - final_count
        self.metrics.duplicate_records = total_removed
        
        self.logger.info("\n" + "=" * 80)
        self.logger.info("DEDUPLICATION SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"Original records:              {original_count:,}")
        # self.logger.info(f"  - Exact duplicates removed:  {exact_removed:,}")
        # self.logger.info(f"  - RegNum duplicates removed: {reg_removed:,}") #to remove since not exists in the flie
        # self.logger.info(f"  - Name duplicates removed:   {name_removed:,}")
        self.logger.info(f"Final unique records:          {final_count:,}")
        self.logger.info(f"Total duplicates removed:      {total_removed:,}")
        self.logger.info(f"Deduplication rate:            {(total_removed/original_count*100):.2f}%")
        self.logger.info("=" * 80)
        
        return df
    
