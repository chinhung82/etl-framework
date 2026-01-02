
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DateType
from pyspark import StorageLevel
import logging
from datetime import datetime
from typing import Dict, Any, List
from collections import defaultdict
from pathlib import Path

from src.utils.sql_loader import SparkSQLExecutor
from src.loaders.mysql_loader import DataLoader


class DataCleaner:
    """
    data cleansing
    """
    
    def __init__(self, spark, config: Dict[str, Any], metrics):
        """
        Initialize DataCleaner
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            metrics: ETLMetrics instance
        """
        self.spark = spark
        self.config = config
        self.metrics = metrics
        self.logger = logging.getLogger('ETL_Pipeline.Cleaner')
        self.sql_executor = SparkSQLExecutor(spark, self.logger)
        self.loader = DataLoader(spark, config, metrics)
        
        # # Register UDFs for date parsing
        # self._register_date_udf()
    
    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Main cleansing method using SQL transformations
        
        Args:
            df: Input DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        self.logger.info("Starting data cleaning process using SQL...")
        
        # Register input DataFrame as temp view
        self.sql_executor.register_temp_view(df, "entities_raw")
        
        # Step 1: Clean string fields
        self.logger.info("Step 1: Cleaning string fields...")
        df = self.sql_executor.execute_sql_file(
            "01_cleansing/clean_string_fields.sql"
        )

        self.sql_executor.register_temp_view(df, "entities_clean_strings")
        
     # Step 2: Parse and standardize dates using SQL
        self.logger.info("Step 2: Parsing and standardizing dates...")
        #df_before_count = df.count()

        # Generate and execute SQL
        IncorporationDate_str, LastUpdate_str = self.generate_date_parsing_sql("01_cleansing/parse_dates.sql")
        df = self.sql_executor.execute_sql_file(
            "01_cleansing/parse_dates.sql",{
                "IncorporationDate_parsing_placeholder":IncorporationDate_str, 
                "LastUpdate_parsing_placeholder":LastUpdate_str}
        )

        # Count successful date parses (on driver, after transformation)
        date_success_count = df.filter(
            (df.IncorporationDate.isNotNull()) | 
            (df.LastUpdate.isNotNull())
        ).count()
        
        self.metrics.date_format_fixes += date_success_count
        self.logger.info(f"Successfully parsed {date_success_count} date values")
        
        self.sql_executor.register_temp_view(df, "entities_dates")
        
        # Step 3: Standardize country codes
        self.logger.info("Step 3: Standardizing country codes...")
        # Generate and execute SQL
        country_code_str = self.generate_country_mapping_sql("01_cleansing/standardize_country_codes.sql")
        df = self.sql_executor.execute_sql_file(
            "01_cleansing/standardize_country_codes.sql",{
                "country_mapping_placeholder": country_code_str
            }
        )
        self.metrics.countrycode_standardizations += df.count()
        self.sql_executor.register_temp_view(df, "entities_countries")
        
        # Step 4: Standardize state codes
        self.logger.info("Step 4: Standardizing state codes...")
        state_code_str = self.generate_state_mapping_sql("01_cleansing/standardize_state_codes.sql")
        df = self.sql_executor.execute_sql_file(
            "01_cleansing/standardize_state_codes.sql",{
                "state_mapping_placeholder": state_code_str
            }
        )
        self.metrics.statecode_standardizations += df.count()
        self.sql_executor.register_temp_view(df, "entities_states")

        # Step 5: Standardize status
        self.logger.info("Step 5: Standardizing status values...")
        status_str = self.generate_status_mapping_sql("01_cleansing/standardize_status.sql")
        df = self.sql_executor.execute_sql_file(
            "01_cleansing/standardize_status.sql",{
                "status_mapping_placeholder": status_str
            }
        )
        self.metrics.status_standardizations += df.count()
        self.sql_executor.register_temp_view(df, "entities_status")
        
        # Step 6: Standardize industry
        self.logger.info("Step 6: Standardizing industry values...")
        industry_str = self.generate_industry_mapping_sql("01_cleansing/standardize_industry.sql")
        df = self.sql_executor.execute_sql_file(
            "01_cleansing/standardize_industry.sql",{
                "industry_mapping_placeholder": industry_str
            }
        )
        self.metrics.industry_standardizations += df.count()
        self.sql_executor.register_temp_view(df, "entities_industry")
        
        # Step 7: Clean email addresses
        self.logger.info("Step 7: Cleaning email addresses...")
        email_regex_str = self.generate_email_regex_sql("01_cleansing/clean_email_addresses.sql")
        df = self.sql_executor.execute_sql_file(
            "01_cleansing/clean_email_addresses.sql",{
                "email_regex_placeholder": email_regex_str
            }
        )
        
        # Count successful date parses AFTER the transformation
        # This avoids serialization issues
        date_parse_count = df.filter(
            (df.IncorporationDate.isNotNull()) | (df.LastUpdate.isNotNull())
        ).count()
        self.metrics.date_format_fixes += date_parse_count
        
        self.logger.info("Data cleaning process completed")
        return df


    def generate_date_parsing_sql(self, sql_file):
        """
        Generate SQL COALESCE statement with all format attempts
        """
        input_formats_IncorporationDate = self.config['date_formats']['input_formats_IncorporationDate']
        input_formats_LastUpdate = self.config['date_formats']['input_formats_LastUpdate']
        output_format = self.config['date_formats']['output_format']
        priority = self.config['date_formats'].get('ambiguity_resolution', {}).get('priority', 'month_first')
        
        # Build COALESCE with all formats for IncorporationDate
        attempts_IncorporationDate = []
        
        # Add each format from config
        for fmt in input_formats_IncorporationDate:
            attempts_IncorporationDate.append(f"to_date(TRIM(IncorporationDate), '{fmt}')")

        # Build COALESCE with all formats for LastUpdate
        attempts_LastUpdate = []
        for fmt in input_formats_LastUpdate:
            attempts_LastUpdate.append(f"to_date(TRIM(LastUpdate), '{fmt}')")

        # For ambiguous formats, add alternative interpretation
        if priority == "month_first":
            # Try M/d/yy first, then d/M/yy
            attempts_IncorporationDate.extend([
                "to_date(TRIM(IncorporationDate), 'M/d/yy')",
                "to_date(TRIM(IncorporationDate), 'd/M/yy')",
            ])
        else:  # day_first
            # Try d/M/yy first, then M/d/yy
            attempts_IncorporationDate.extend([
                "to_date(TRIM(IncorporationDate), 'd/M/yy')",
                "to_date(TRIM(IncorporationDate), 'M/d/yy')",
            ])
        
        # Remove duplicates while preserving order
        attempts_IncorporationDate = list(dict.fromkeys(attempts_IncorporationDate))
        
        # Build final COALESCE for IncorporationDate
        coalesce_expr_IncorporationDate = "COALESCE(\n    " + ",\n    ".join(attempts_IncorporationDate) + "\n)"

        
        #for LastUpdate
        attempts_LastUpdate = []
        for fmt in input_formats_LastUpdate:
            attempts_LastUpdate.append(f"to_date(TRIM(LastUpdate), '{fmt}')")

        # Remove duplicates while preserving order
        attempts_LastUpdate = list(dict.fromkeys(attempts_LastUpdate))

        # Build final COALESCE for LastUpdate
        coalesce_expr_LastUpdate = "COALESCE(\n    " + ",\n    ".join(attempts_LastUpdate) + "\n)"

        # Build final date formatting strings
        IncorporationDate_str = f"""
            date_format(
                    {coalesce_expr_IncorporationDate},
                    '{output_format}'
                )
            """
        
        LastUpdate_str = f"""
            date_format(
                    {coalesce_expr_LastUpdate},
                    '{output_format}'
                )
        """

        return IncorporationDate_str, LastUpdate_str
    
    def generate_country_mapping_sql(self, sql_file):
        """
        Process SQL with placeholders
        Placeholders:
            {country_mapping_placeholder}
        """
        mapping_country = self.config.get('country_mapping', {})
        
        # Replace country mapping
        country_case = self.generate_mapping_case_statement(
            mapping_country,
            source_column='Country',
            target_column='CountryCode',
        )
        return country_case
    
    def generate_state_mapping_sql(self, sql_file):
        """
        Process SQL with placeholders
        Placeholders:
            {state_mapping_placeholder}
        """
        mapping_state = self.config.get('state_mapping', {})
        
        # Replace state mapping
        state_case = self.generate_mapping_case_statement(
            mapping_state,
            source_column='State',
            target_column='StateCode',
        )

        return state_case
    
    def generate_status_mapping_sql(self, sql_file):
        """
        Process SQL with placeholders
        Placeholders:
            {status_mapping_placeholder}
        """
        mapping_status = self.config.get('status_mapping', {})
        
        # Replace status mapping
        status_case = self.generate_mapping_case_statement(
            mapping_status,
            source_column='Status',
            target_column='Status',
        )
        return status_case
    
    def generate_industry_mapping_sql(self, sql_file):
        """
        Process SQL with placeholders
        Placeholders:
            {industry_mapping_placeholder}
        """
        mapping_industry = self.config.get('industry_mapping', {})
        
        # Replace industry mapping
        industry_case = self.generate_mapping_case_statement(
            mapping_industry,
            source_column='Industry',
            target_column='Industry',
        )

        return industry_case

    def generate_email_regex_sql(self, sql_file):
        """
        Process SQL with placeholders
        Placeholders:
            {email_regex_placeholder}
        """
        email_regex_pattern = self.config['email_regex_pattern']
     
        # generate email regex 
        email_regex_str = f"""
            CASE 
                WHEN ContactEmail IS NOT NULL 
                    AND LOWER(TRIM(ContactEmail)) RLIKE '{email_regex_pattern}'
                    THEN LOWER(TRIM(ContactEmail))
                ELSE NULL
            END AS ContactEmail
        """
        return email_regex_str



    def generate_mapping_case_statement(self,mapping_dict, 
                                    source_column, 
                                    target_column, 
                                    coalesce_columns=None):
        """
        Generic function to generate CASE statement for any mapping
        
        Args:
            mapping_dict: Dictionary of source -> target mappings
            source_column: Name of source column
            target_column: Name of target column
            coalesce_columns: List of columns to COALESCE (e.g., ["CountryCode", "Country"])
            indent: Indentation spaces
        
        Returns:
            str: SQL CASE statement
        """
        # Group mappings by target value
        target_to_sources = defaultdict(list)
        for source, target in mapping_dict.items():
            target_to_sources[target].append(source)
        
        # Build column reference
        if coalesce_columns:
            col_ref = f"UPPER(COALESCE({', '.join(coalesce_columns)}))"
        else:
            col_ref = f"UPPER({source_column})"
        
        # Build CASE statement
        lines = ["CASE"]
        
        for target_value in sorted(target_to_sources.keys()):
            sources = target_to_sources[target_value]
            upper_sources = [s.upper() for s in sources]
            
            if len(upper_sources) == 1:
                lines.append(f" WHEN {col_ref} = '{upper_sources[0]}'")
                lines.append(f" THEN '{target_value}'")
            else:
                sources_list = ", ".join([f"'{s}'" for s in upper_sources])
                lines.append(f" WHEN {col_ref} IN ({sources_list})")
                lines.append(f" THEN '{target_value}'")
        
        lines.append(f" ELSE {target_column}")
        lines.append(f" END AS {target_column}")
        
        return "\n".join(lines)