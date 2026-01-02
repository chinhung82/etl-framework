"""
===============================================================================
FILE: src/pipeline.py - Main Pipeline Orchestrator
===============================================================================
"""
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark import StorageLevel
import logging
import json
from datetime import datetime
from typing import Dict, Any
import argparse

from src.utils.config_loader import load_config
from src.utils.logger import setup_logging
from src.utils.metrics import ETLMetrics
from src.extractors.csv_extractor import DataExtractor
from src.transformers.cleaner import DataCleaner
from src.transformers.deduplicator import DataDeduplicator
from src.transformers.validator import DataValidator
from src.transformers.transformer import DataTransformer
from src.loaders.mysql_loader import DataLoader
from src.utils.quality_reporter import QualityReporter

class ETLPipeline:
    """Main ETL Pipeline using SQL transformations"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        # Load configuration
        self.config = load_config(config_path)
        
        # Setup logging
        self.logger = setup_logging(self.config)
        
        # Initialize metrics
        self.metrics = ETLMetrics()
        
        # Initialize Spark
        self.spark = self.initialize_spark()
        
        # Initialize components
        self.extractor = DataExtractor(self.spark, self.config)
        self.cleaner = DataCleaner(self.spark, self.config, self.metrics)
        self.deduplicator = DataDeduplicator(self.spark, self.config, self.metrics)
        self.validator = DataValidator(self.spark, self.config, self.metrics)
        self.transformer = DataTransformer(self.spark, self.config)
        self.loader = DataLoader(self.spark, self.config, self.metrics)
        self.reporter = QualityReporter(self.config)
        
        self.logger.info("ETL Pipeline")
    
    def initialize_spark(self) -> SparkSession:
        """Initialize Spark session"""
        spark_config = self.config.get('spark', {})
        
        builder = SparkSession.builder \
            .appName(spark_config.get('app_name', 'ETL_Pipeline'))
        
        # Apply Spark configurations
        for key, value in spark_config.get('config', {}).items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        self.logger.info("Spark session initialized")
        return spark
    
    def run(self) -> bool:
        """Execute the ETL pipeline"""
        try:
            # Step 1: Extract
            self.logger.info("STEP 1: Extracting data from CSV...")
            raw_df = self.extractor.extract_data()
            self.metrics.total_records = raw_df.count()
            before_stats = self._collect_stats(raw_df)
            
            # Step 2: Clean
            self.logger.info("STEP 2: Cleaning data...")
            cleaned_df = self.cleaner.clean_data(raw_df)
            
            # Step 3: Deduplicate
            self.logger.info("STEP 3: Deduplicating records...")
            deduped_df = self.deduplicator.deduplicate(cleaned_df)

            # Step 4: Validate
            self.logger.info("STEP 4: Validating data...")
            valid_df, quarantine_df = self.validator.validate(deduped_df)

            after_stats = self._collect_stats(valid_df) 
            
            # Step 5: Transform - NO USE
            self.logger.info("STEP 5: Transforming to target schema...")
            transformed_df = self.transformer.transform(valid_df)
           
            # Step 6: Load to MySQL
            self.logger.info("STEP 6: Loading to MySQL...")
            self.loader.load_to_mysql(transformed_df)
            
            # Step 7: Save quarantine data
            self.logger.info("STEP 7: Saving quarantine records...")
            self.loader.save_quarantine_data(quarantine_df)
            
            # Step 8: Generate quality report
            self.logger.info("STEP 8: Generating quality report...")
            report_path = self.reporter.generate_report(
                self.metrics, before_stats, after_stats
            )
            
            self.logger.info("=" * 80)
            self.logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            self.logger.info(f"Quality Report: {report_path}")
            self.logger.info("=" * 80)
            
            self._print_summary()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
            return False


    def _collect_stats(self, df: DataFrame) -> Dict[str, int]:
        """Collect statistics about DataFrame"""
        try:
            from pyspark.sql.functions import count, col

            stats = df.agg(
                count("*").alias("total_count"),
                count(col("EntityName")).alias("EntityName"),
                count(col("IncorporationDate")).alias("IncorporationDate"),
                count(col("CountryCode")).alias("CountryCode"),
                count(col("StateCode")).alias("StateCode"),
                count(col("Status")).alias("Status"),
                count(col("Industry")).alias("Industry"),                
                count(col("ContactEmail")).alias("ContactEmail")
            ).collect()[0].asDict()
            
            return stats
        except Exception as e:
            self.logger.error(f"Error collecting stats: {e}")
            return {}
    
    def _print_summary(self):
        """Print pipeline summary"""
        summary = self.metrics.get_summary()
        
        print("\n" + "=" * 80)
        print("ETL PIPELINE SUMMARY")
        print("=" * 80)
        print(f"Total Records:      {summary['total_records']:,}")
        print(f"Valid Records:      {summary['valid_records']:,}")
        print(f"Rejected Records:   {summary['rejected_records']:,}")
        print(f"Duplicate Records:  {summary['duplicate_records']:,}")
        print(f"Success Rate:       {summary['success_rate']}")
        print("=" * 80)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Run ETL Pipeline')
    parser.add_argument('--config', default='config/config.yaml',
                       help='Path to config file')
    args = parser.parse_args()
    
    pipeline = ETLPipeline(args.config)
    success = pipeline.run()
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())