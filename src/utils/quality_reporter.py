from typing import Dict, Any
import json
from datetime import datetime
from pathlib import Path
import os


class QualityReporter:
    """
    Generate comprehensive data quality reports
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.report_dir = Path(config['output']['quality_report_path'])
        self.report_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_report(self, metrics: Any, before_stats: Dict = None, 
                       after_stats: Dict = None) -> str:
        """
        Generate comprehensive quality report
        
        Args:
            metrics: ETLMetrics instance
            before_stats: Statistics before cleaning
            after_stats: Statistics after cleaning
            
        Returns:
            Path to generated report
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.report_dir / f"quality_report_{timestamp}.txt"
        
        report_content = self._build_report_content(metrics, before_stats, after_stats)
        
        # Save as text
        with open(report_file, 'w') as f:
            f.write(report_content)
        
        # # Also save as JSON
        # json_file = self.report_dir / f"quality_report_{timestamp}.json"
        # with open(json_file, 'w') as f:
        #     json.dump(metrics.get_summary(), f, indent=2, default=str)
        
        return str(report_file)
    
    def _build_report_content(self, metrics: Any, before_stats: Dict = None, 
                             after_stats: Dict = None) -> str:
        """Build formatted report content"""
        
        lines = []
        lines.append("=" * 80)
        lines.append("ETL PIPELINE - DATA QUALITY REPORT")
        lines.append("=" * 80)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        
        # Summary Statistics
        lines.append("SUMMARY STATISTICS")
        lines.append("-" * 80)
        lines.append(f"Total Records Processed:    {metrics.total_records:,}")
        lines.append(f"Records Successfully Loaded: {metrics.valid_records:,}")
        lines.append(f"Records Rejected:           {metrics.rejected_records:,}")
        lines.append(f"Duplicate Records Found:    {metrics.duplicate_records:,}")
        lines.append(f"Success Rate:               {metrics.get_summary()['success_rate']}")
        lines.append("")
        
        # Data Quality Fixes
        lines.append("DATA QUALITY FIXES APPLIED")
        lines.append("-" * 80)
        lines.append(f"Null Values Handled:        {metrics.null_handling_count:,}")
        lines.append(f"Date Formats Fixed:         {metrics.date_format_fixes:,}")
        lines.append(f"Country Codes Standardized: {metrics.countrycode_standardizations:,}")
        lines.append(f"State Codes Standardized:   {metrics.statecode_standardizations:,}")
        lines.append(f"Status Standardized:        {metrics.status_standardizations:,}")
        lines.append(f"Industry Standardized:      {metrics.industry_standardizations:,}")
        lines.append("")
        
        # Rejection Reasons
        if metrics.rejection_reasons:
            lines.append("REJECTION REASONS BREAKDOWN")
            lines.append("-" * 80)
            for reason, count in sorted(metrics.rejection_reasons.items(), 
                                       key=lambda x: x[1], reverse=True):
                lines.append(f"  {reason:40s}: {count:,}")
            lines.append("")
        
        # Before/After Comparison
        if before_stats and after_stats:
            lines.append("BEFORE/AFTER DATA QUALITY METRICS")
            lines.append("-" * 80)
            self._add_comparison_metrics(lines, before_stats, after_stats)
            lines.append("")
        
        lines.append("=" * 80)
        lines.append("END OF REPORT")
        lines.append("=" * 80)
        
        return "\n".join(lines)
    
    def _add_comparison_metrics(self, lines: list, before: Dict, after: Dict):
        """Add before/after comparison metrics"""
        
        metrics_to_compare = [
            ('Total Records', 'total_count'),
            ('EntityName', 'EntityName'),
            ('IncorporationDate', 'IncorporationDate'),
            ('CountryCode', 'CountryCode'),
            ('StateCode', 'StateCode'),
            ('Status', 'Status'),
            ('Industry', 'Industry'),
            ('ContactEmail', 'ContactEmail')
        ]

        lines.append(f"{'Metric':<30} {'Before':>15} {'After':>15} {'Change':>15}")
        lines.append("-" * 80)
        
        for label, key in metrics_to_compare:
            before_val = before.get(key, 0)
            after_val = after.get(key, 0)
            change = after_val - before_val
            change_pct = (change / before_val * 100) if before_val > 0 else 0
            
            lines.append(
                f"{label:<30} {before_val:>15,} {after_val:>15,} "
                f"{change:>+10,} ({change_pct:+.1f}%)"
            )