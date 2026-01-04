"""Data export module for saving cleaned data."""

from pathlib import Path
from typing import Optional
import pandas as pd
import json

from .exceptions import DataExportError
from .logger import setup_logger

logger = setup_logger(__name__)


def export_to_csv(
    df: pd.DataFrame,
    output_path: Path,
    index: bool = False,
    **kwargs
) -> Path:
    """
    Export dataframe to CSV file.
    
    Args:
        df: Dataframe to export
        output_path: Path for the output CSV file
        index: Whether to write row indices (default: False)
        **kwargs: Additional arguments to pass to df.to_csv
        
    Returns:
        Path to the exported file
        
    Raises:
        DataExportError: If export fails
    """
    logger.info(f"Exporting data to CSV: {output_path}")
    
    try:
        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_csv(output_path, index=index, **kwargs)
        
        file_size_mb = output_path.stat().st_size / 1024 / 1024
        logger.info(f"Successfully exported {len(df)} rows to {output_path} ({file_size_mb:.2f} MB)")
        
        return output_path
    except Exception as e:
        error_msg = f"Failed to export to CSV: {str(e)}"
        logger.error(error_msg)
        raise DataExportError(error_msg) from e


def export_to_json(
    df: pd.DataFrame,
    output_path: Path,
    orient: str = "records",
    **kwargs
) -> Path:
    """
    Export dataframe to JSON file.
    
    Args:
        df: Dataframe to export
        output_path: Path for the output JSON file
        orient: Format of JSON string (default: 'records')
        **kwargs: Additional arguments to pass to df.to_json
        
    Returns:
        Path to the exported file
        
    Raises:
        DataExportError: If export fails
    """
    logger.info(f"Exporting data to JSON: {output_path}")
    
    try:
        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_json(output_path, orient=orient, **kwargs)
        
        file_size_mb = output_path.stat().st_size / 1024 / 1024
        logger.info(f"Successfully exported {len(df)} rows to {output_path} ({file_size_mb:.2f} MB)")
        
        return output_path
    except Exception as e:
        error_msg = f"Failed to export to JSON: {str(e)}"
        logger.error(error_msg)
        raise DataExportError(error_msg) from e


def export_summary_report(
    quality_report: dict,
    output_path: Path
) -> Path:
    """
    Export data quality report to JSON file.
    
    Args:
        quality_report: Data quality report dictionary
        output_path: Path for the output JSON file
        
    Returns:
        Path to the exported file
        
    Raises:
        DataExportError: If export fails
    """
    logger.info(f"Exporting quality report to: {output_path}")
    
    try:
        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(quality_report, f, indent=2, default=str)
        
        logger.info(f"Successfully exported quality report to {output_path}")
        
        return output_path
    except Exception as e:
        error_msg = f"Failed to export quality report: {str(e)}"
        logger.error(error_msg)
        raise DataExportError(error_msg) from e
