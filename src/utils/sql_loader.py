"""
SQL Loader Utility
Loads SQL files and executes them using PySpark SQL
"""

import os
import re
from typing import Dict, Any, List
from pathlib import Path


class SQLLoader:
    """
    Load and execute SQL files with PySpark
    """
    
    def __init__(self, sql_base_path: str = "sql"):
        """
        Initialize SQL Loader
        
        Args:
            sql_base_path: Base directory containing SQL files
        """
        self.sql_base_path = Path(sql_base_path)
        
        if not self.sql_base_path.exists():
            raise FileNotFoundError(f"SQL base path not found: {sql_base_path}")
    
    def load_sql(self, sql_file: str, params: Dict[str, Any] = None) -> str:
        """
        Load SQL from file and substitute parameters
        
        Args:
            sql_file: Path to SQL file relative to sql_base_path
            params: Dictionary of parameters for substitution
            
        Returns:
            SQL string with parameters substituted
            
        Example:
            sql = loader.load_sql(
                "01_cleansing/clean_string_fields.sql",
                {"table_name": "entities_raw"}
            )
        """
        sql_path = self.sql_base_path / sql_file
        
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_path}")
        
        # Read SQL file
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Substitute parameters if provided
        if params:
            sql_content = self._substitute_params(sql_content, params)
        
        return sql_content
    
    def load_sql_statements(self, sql_file: str, params: Dict[str, Any] = None) -> List[str]:
        """
        Load SQL file and split into individual statements
        
        Args:
            sql_file: Path to SQL file
            params: Parameters for substitution
            
        Returns:
            List of SQL statements
        """
        sql_content = self.load_sql(sql_file, params)
        
        # Split by semicolon (end of statement)
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        return statements
    
    def _substitute_params(self, sql: str, params: Dict[str, Any]) -> str:
        """
        Substitute parameters in SQL string
        
        Supports:
        - {{param_name}} - replaced with param value
        - {{param_name|default}} - replaced with param or default
        
        Args:
            sql: SQL string with parameter placeholders
            params: Dictionary of parameter values
            
        Returns:
            SQL with parameters substituted
        """
        # Pattern: {{param_name}} or {{param_name|default}}
        pattern = r'\{\{(\w+)(?:\|([^}]+))?\}\}'
        
        def replace_param(match):
            param_name = match.group(1)
            default_value = match.group(2)
            
            # Get parameter value
            if param_name in params:
                value = params[param_name]
            elif default_value:
                value = default_value
            else:
                raise ValueError(f"Parameter '{param_name}' not found and no default provided")
            
            # Convert value
            if isinstance(value, str):
                return f"{value}"
            else:
                return str(value)
        
        return re.sub(pattern, replace_param, sql)
    
    def get_sql_files_in_directory(self, directory: str) -> List[str]:
        """
        Get all SQL files in a directory (sorted alphabetically)
        
        Args:
            directory: Directory path relative to sql_base_path
            
        Returns:
            List of SQL file paths
        """
        dir_path = self.sql_base_path / directory
        
        if not dir_path.exists():
            raise FileNotFoundError(f"Directory not found: {dir_path}")
        
        sql_files = sorted(dir_path.glob("*.sql"))
        
        # Return relative paths
        return [str(f.relative_to(self.sql_base_path)) for f in sql_files]


class SparkSQLExecutor:
    """
    Execute SQL using PySpark
    """
    
    def __init__(self, spark, logger=None):
        """
        Initialize SQL Executor
        
        Args:
            spark: SparkSession instance
            logger: Logger instance (optional)
        """
        self.spark = spark
        self.logger = logger
        self.sql_loader = SQLLoader()
    
    def execute_sql_file(self, sql_file: str, params: Dict[str, Any] = None):
        """
        Load and execute SQL file
        
        Args:
            sql_file: Path to SQL file
            params: Parameters for substitution
            
        Returns:
            DataFrame result (if SELECT) or None
        """
        if self.logger:
            self.logger.info(f"Executing SQL file: {sql_file}")
        
        sql = self.sql_loader.load_sql(sql_file, params)
        
        try:
            result = self.spark.sql(sql)
            
            if self.logger:
                self.logger.info(f"SQL execution completed: {sql_file}")
            
            return result
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"SQL execution failed: {sql_file}")
                self.logger.error(f"Error: {str(e)}")
                self.logger.error(f"SQL: {sql[:500]}...")  # Log first 500 chars
            raise
    
    def execute_sql(self, sql: str, description: str = "SQL Query"):
        """
        Execute SQL string directly
        
        Args:
            sql: SQL string to execute
            description: Description for logging
            
        Returns:
            DataFrame result
        """
        if self.logger:
            self.logger.info(f"Executing: {description}")
        
        try:
            result = self.spark.sql(sql)
            
            if self.logger:
                self.logger.info(f"Completed: {description}")
            
            return result
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed: {description}")
                self.logger.error(f"Error: {str(e)}")
            raise
    
    def register_temp_view(self, df, view_name: str):
        """
        Register DataFrame as temporary view for SQL queries
        
        Args:
            df: DataFrame to register
            view_name: Name for the temporary view
        """
        df.createOrReplaceTempView(view_name)
        
        if self.logger:
            self.logger.info(f"Registered temp view: {view_name}")
    
    def execute_sql_directory(self, directory: str, params: Dict[str, Any] = None):
        """
        Execute all SQL files in a directory (alphabetically)
        
        Args:
            directory: Directory containing SQL files
            params: Parameters for substitution
            
        Returns:
            Result from last SQL file
        """
        sql_files = self.sql_loader.get_sql_files_in_directory(directory)
        
        if self.logger:
            self.logger.info(f"Executing {len(sql_files)} SQL files from: {directory}")
        
        result = None
        for sql_file in sql_files:
            result = self.execute_sql_file(sql_file, params)
        
        return result