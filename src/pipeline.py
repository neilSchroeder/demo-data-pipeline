"""Main pipeline orchestrator that coordinates the ETL process."""

from pathlib import Path
from typing import Optional, List, Dict, Any
import pandas as pd

from .ingestion import ingest_csv, get_data_info
from .cleaning import (
    remove_duplicates,
    handle_missing_values,
    standardize_column_names,
    clean_text_columns,
    parse_dates,
    remove_outliers
)
from .validation import (
    validate_schema,
    validate_data_types,
    validate_ranges,
    validate_completeness,
    generate_data_quality_report
)
from .export import export_to_csv, export_to_json, export_summary_report
from .logger import setup_logger
from .exceptions import PipelineError

logger = setup_logger(__name__)


class DataCleaningPipeline:
    """
    Main ETL pipeline for data cleaning and validation.
    
    This pipeline provides a complete workflow for:
    1. Ingesting raw CSV data
    2. Cleaning and standardizing the data
    3. Validating data quality
    4. Exporting cleaned data and quality reports
    """
    
    def __init__(
        self,
        input_path: Path,
        output_dir: Path,
        output_filename: str = "cleaned_data.csv"
    ):
        """
        Initialize the pipeline.
        
        Args:
            input_path: Path to the raw data CSV file
            output_dir: Directory for output files
            output_filename: Name for the cleaned data file
        """
        self.input_path = input_path
        self.output_dir = output_dir
        self.output_filename = output_filename
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.raw_data: Optional[pd.DataFrame] = None
        self.cleaned_data: Optional[pd.DataFrame] = None
        self.quality_report: Optional[Dict[str, Any]] = None
        
        logger.info(f"Initialized pipeline: {input_path} -> {output_dir / output_filename}")
    
    def run(
        self,
        remove_duplicates_flag: bool = True,
        missing_value_strategy: str = "auto",
        standardize_columns: bool = True,
        date_columns: Optional[List[str]] = None,
        remove_outliers_flag: bool = False,
        outlier_columns: Optional[List[str]] = None,
        export_format: str = "csv"
    ) -> pd.DataFrame:
        """
        Run the complete data cleaning pipeline.
        
        Args:
            remove_duplicates_flag: Whether to remove duplicate rows
            missing_value_strategy: Strategy for handling missing values
            standardize_columns: Whether to standardize column names
            date_columns: List of columns to parse as dates
            remove_outliers_flag: Whether to remove outliers
            outlier_columns: Specific columns to check for outliers
            export_format: Export format ('csv' or 'json')
            
        Returns:
            Cleaned dataframe
            
        Raises:
            PipelineError: If any step in the pipeline fails
        """
        logger.info("=" * 60)
        logger.info("Starting Data Cleaning Pipeline")
        logger.info("=" * 60)
        
        try:
            # Step 1: Ingest data
            logger.info("Step 1: Data Ingestion")
            self.raw_data = ingest_csv(self.input_path)
            raw_info = get_data_info(self.raw_data)
            
            # Initialize cleaned data
            self.cleaned_data = self.raw_data.copy()
            
            # Step 2: Clean data
            logger.info("Step 2: Data Cleaning")
            
            if standardize_columns:
                logger.info("- Standardizing column names")
                self.cleaned_data = standardize_column_names(self.cleaned_data)
            
            if remove_duplicates_flag:
                logger.info("- Removing duplicates")
                self.cleaned_data = remove_duplicates(self.cleaned_data)
            
            logger.info("- Handling missing values")
            self.cleaned_data = handle_missing_values(
                self.cleaned_data,
                strategy=missing_value_strategy
            )
            
            logger.info("- Cleaning text columns")
            self.cleaned_data = clean_text_columns(self.cleaned_data)
            
            if date_columns:
                logger.info(f"- Parsing date columns: {date_columns}")
                self.cleaned_data = parse_dates(self.cleaned_data, date_columns)
            
            if remove_outliers_flag:
                logger.info("- Removing outliers")
                self.cleaned_data = remove_outliers(
                    self.cleaned_data,
                    columns=outlier_columns
                )
            
            # Step 3: Generate quality report
            logger.info("Step 3: Generating Quality Report")
            self.quality_report = generate_data_quality_report(self.cleaned_data)
            
            # Step 4: Export results
            logger.info("Step 4: Exporting Results")
            output_path = self.output_dir / self.output_filename
            
            if export_format == "csv":
                export_to_csv(self.cleaned_data, output_path)
            elif export_format == "json":
                export_to_json(self.cleaned_data, output_path.with_suffix('.json'))
            
            # Export quality report
            report_path = self.output_dir / "quality_report.json"
            export_summary_report(self.quality_report, report_path)
            
            logger.info("=" * 60)
            logger.info("Pipeline completed successfully!")
            logger.info(f"Input rows: {raw_info['rows']}, Output rows: {len(self.cleaned_data)}")
            logger.info(f"Rows removed: {raw_info['rows'] - len(self.cleaned_data)}")
            logger.info("=" * 60)
            
            return self.cleaned_data
            
        except Exception as e:
            error_msg = f"Pipeline failed: {str(e)}"
            logger.error(error_msg)
            raise PipelineError(error_msg) from e
    
    def get_cleaned_data(self) -> Optional[pd.DataFrame]:
        """Get the cleaned dataframe."""
        return self.cleaned_data
    
    def get_quality_report(self) -> Optional[Dict[str, Any]]:
        """Get the data quality report."""
        return self.quality_report
