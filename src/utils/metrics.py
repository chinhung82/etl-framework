import yaml
import logging
import os
from datetime import datetime
from typing import Dict, Any, List
from logging.handlers import RotatingFileHandler

class ETLMetrics:
    """
    Class to track ETL pipeline metrics
    """
    def __init__(self):
        self.total_records = 0
        self.valid_records = 0
        self.rejected_records = 0
        self.duplicate_records = 0
        self.null_handling_count = 0
        self.date_format_fixes = 0
        self.countrycode_standardizations = 0
        self.statecode_standardizations = 0
        self.status_standardizations = 0
        self.industry_standardizations = 0
        self.rejection_reasons = {}
        
    def add_rejection(self, reason: str) -> None:
        """Add a rejection reason to the counter"""
        self.rejection_reasons[reason] = self.rejection_reasons.get(reason, 0) + 1
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics"""
        return {
            'total_records': self.total_records,
            'valid_records': self.valid_records,
            'rejected_records': self.rejected_records,
            'duplicate_records': self.duplicate_records,
            'null_handling_count': self.null_handling_count,
            'date_format_fixes': self.date_format_fixes,
            'countrycode_standardizations': self.countrycode_standardizations,
            'statecode_standardizations': self.statecode_standardizations,
            'status_standardizations': self.status_standardizations,
            'industry_standardizations': self.industry_standardizations,
            'rejection_reasons': self.rejection_reasons,
            'success_rate': f"{(self.valid_records / self.total_records * 100):.2f}%" if self.total_records > 0 else "0%"
        }