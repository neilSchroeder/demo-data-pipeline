"""Data ingestion module for loading raw data."""

from pathlib import Path
from typing import Optional
import pandas as pd

from .exceptions import DataIngestionError
from .logger import setup_logger

logger = setup_logger(__name__)


def ingest_csv(
    file_path: Path,
    encoding: str = "utf-8",
    **kwargs
) -> pd.DataFrame:
    """
    Ingest CSV data from a file.
    
    Args:
        file_path: Path to the CSV file
        encoding: Character encoding (default: utf-8)
        **kwargs: Additional arguments to pass to pd.read_csv
        
    Returns:
        DataFrame containing the raw data
        
    Raises:
        DataIngestionError: If file cannot be read or parsed
    """
    logger.info(f"Starting data ingestion from {file_path}")
    
    if not file_path.exists():
        error_msg = f"File not found: {file_path}"
        logger.error(error_msg)
        raise DataIngestionError(error_msg)
    
    try:
        df = pd.read_csv(file_path, encoding=encoding, **kwargs)
        logger.info(f"Successfully ingested {len(df)} rows and {len(df.columns)} columns")
        logger.debug(f"Columns: {list(df.columns)}")
        return df
    except Exception as e:
        error_msg = f"Failed to read CSV file: {str(e)}"
        logger.error(error_msg)
        raise DataIngestionError(error_msg) from e


def get_data_info(df: pd.DataFrame) -> dict:
    """
    Get summary information about the dataframe.
    
    Args:
        df: Input dataframe
        
    Returns:
        Dictionary with data statistics
    """
    info = {
        "rows": len(df),
        "columns": len(df.columns),
        "column_names": list(df.columns),
        "dtypes": df.dtypes.to_dict(),
        "missing_values": df.isnull().sum().to_dict(),
        "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024
    }
    
    logger.info(f"Data info: {info['rows']} rows, {info['columns']} columns")
    return info
