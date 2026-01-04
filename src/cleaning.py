"""Data cleaning module with functions to handle messy data."""

from typing import List, Optional, Union
import pandas as pd
import numpy as np
from datetime import datetime

from .config import MISSING_VALUE_THRESHOLD, DATE_FORMATS
from .exceptions import DataCleaningError
from .logger import setup_logger

logger = setup_logger(__name__)


def remove_duplicates(
    df: pd.DataFrame,
    subset: Optional[List[str]] = None,
    keep: str = "first"
) -> pd.DataFrame:
    """
    Remove duplicate rows from the dataframe.
    
    Args:
        df: Input dataframe
        subset: Column labels to consider for identifying duplicates
        keep: Which duplicates to keep ('first', 'last', or False to drop all)
        
    Returns:
        DataFrame with duplicates removed
    """
    initial_rows = len(df)
    df_cleaned = df.drop_duplicates(subset=subset, keep=keep)
    duplicates_removed = initial_rows - len(df_cleaned)
    
    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed} duplicate rows ({duplicates_removed/initial_rows*100:.2f}%)")
    else:
        logger.info("No duplicate rows found")
    
    return df_cleaned


def handle_missing_values(
    df: pd.DataFrame,
    strategy: str = "auto",
    threshold: float = MISSING_VALUE_THRESHOLD,
    fill_value: Optional[Union[str, int, float]] = None
) -> pd.DataFrame:
    """
    Handle missing values in the dataframe.
    
    Args:
        df: Input dataframe
        strategy: Strategy for handling missing values:
            - 'auto': Drop columns with >threshold missing, impute the rest
            - 'drop_rows': Drop rows with any missing values
            - 'drop_columns': Drop columns with any missing values
            - 'fill': Fill with specified value or strategy
        threshold: Threshold for dropping columns (fraction of missing values)
        fill_value: Value to use for filling (if strategy='fill')
        
    Returns:
        DataFrame with missing values handled
    """
    df_cleaned = df.copy()
    initial_missing = df_cleaned.isnull().sum().sum()
    
    logger.info(f"Initial missing values: {initial_missing}")
    
    if strategy == "auto":
        # Drop columns with too many missing values
        missing_pct = df_cleaned.isnull().sum() / len(df_cleaned)
        cols_to_drop = missing_pct[missing_pct > threshold].index.tolist()
        
        if cols_to_drop:
            logger.info(f"Dropping columns with >{threshold*100}% missing: {cols_to_drop}")
            df_cleaned = df_cleaned.drop(columns=cols_to_drop)
        
        # Impute remaining missing values
        for col in df_cleaned.columns:
            if df_cleaned[col].isnull().any():
                if df_cleaned[col].dtype in ['int64', 'float64']:
                    # Fill numeric with median
                    fill_val = df_cleaned[col].median()
                    df_cleaned[col] = df_cleaned[col].fillna(fill_val)
                    logger.debug(f"Filled {col} with median: {fill_val}")
                else:
                    # Fill categorical with mode or 'Unknown'
                    if df_cleaned[col].mode().empty:
                        fill_val = 'Unknown'
                    else:
                        fill_val = df_cleaned[col].mode()[0]
                    df_cleaned[col] = df_cleaned[col].fillna(fill_val)
                    logger.debug(f"Filled {col} with: {fill_val}")
    
    elif strategy == "drop_rows":
        df_cleaned = df_cleaned.dropna()
        logger.info(f"Dropped {len(df) - len(df_cleaned)} rows with missing values")
    
    elif strategy == "drop_columns":
        df_cleaned = df_cleaned.dropna(axis=1)
        logger.info(f"Dropped {len(df.columns) - len(df_cleaned.columns)} columns with missing values")
    
    elif strategy == "fill":
        if fill_value is None:
            raise DataCleaningError("fill_value must be provided when strategy='fill'")
        df_cleaned = df_cleaned.fillna(fill_value)
        logger.info(f"Filled all missing values with: {fill_value}")
    
    final_missing = df_cleaned.isnull().sum().sum()
    logger.info(f"Final missing values: {final_missing}")
    
    return df_cleaned


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names to lowercase with underscores.
    
    Args:
        df: Input dataframe
        
    Returns:
        DataFrame with standardized column names
    """
    original_cols = df.columns.tolist()
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(r'[^\w\s]', '', regex=True)
        .str.replace(r'\s+', '_', regex=True)
    )
    
    logger.info(f"Standardized {len(df.columns)} column names")
    if original_cols != df.columns.tolist():
        logger.debug(f"Column name changes: {dict(zip(original_cols, df.columns))}")
    
    return df


def clean_text_columns(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Clean text columns by trimming whitespace and handling case.
    
    Args:
        df: Input dataframe
        columns: Specific columns to clean (default: all object columns)
        
    Returns:
        DataFrame with cleaned text columns
    """
    df_cleaned = df.copy()
    
    if columns is None:
        columns = df_cleaned.select_dtypes(include=['object']).columns.tolist()
    
    for col in columns:
        if col in df_cleaned.columns and df_cleaned[col].dtype == 'object':
            # Strip whitespace
            df_cleaned[col] = df_cleaned[col].str.strip()
            logger.debug(f"Cleaned text column: {col}")
    
    logger.info(f"Cleaned {len(columns)} text columns")
    return df_cleaned


def parse_dates(
    df: pd.DataFrame,
    date_columns: List[str],
    formats: List[str] = DATE_FORMATS
) -> pd.DataFrame:
    """
    Parse date columns with multiple format attempts.
    
    Args:
        df: Input dataframe
        date_columns: List of column names containing dates
        formats: List of date formats to try
        
    Returns:
        DataFrame with parsed date columns
    """
    df_cleaned = df.copy()
    
    for col in date_columns:
        if col not in df_cleaned.columns:
            logger.warning(f"Date column '{col}' not found in dataframe")
            continue
        
        parsed = False
        for fmt in formats:
            try:
                df_cleaned[col] = pd.to_datetime(df_cleaned[col], format=fmt, errors='coerce')
                logger.info(f"Parsed date column '{col}' with format: {fmt}")
                parsed = True
                break
            except:
                continue
        
        if not parsed:
            # Try pandas automatic parsing as fallback
            try:
                df_cleaned[col] = pd.to_datetime(df_cleaned[col], errors='coerce')
                logger.info(f"Parsed date column '{col}' with automatic format detection")
            except Exception as e:
                logger.warning(f"Failed to parse date column '{col}': {str(e)}")
    
    return df_cleaned


def remove_outliers(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    method: str = "iqr",
    threshold: float = 1.5
) -> pd.DataFrame:
    """
    Remove outliers from numeric columns.
    
    Args:
        df: Input dataframe
        columns: Specific columns to check (default: all numeric columns)
        method: Method for outlier detection ('iqr' or 'zscore')
        threshold: Threshold for outlier detection (IQR multiplier or Z-score)
        
    Returns:
        DataFrame with outliers removed
    """
    df_cleaned = df.copy()
    initial_rows = len(df_cleaned)
    
    if columns is None:
        columns = df_cleaned.select_dtypes(include=[np.number]).columns.tolist()
    
    for col in columns:
        if col not in df_cleaned.columns:
            continue
        
        if method == "iqr":
            Q1 = df_cleaned[col].quantile(0.25)
            Q3 = df_cleaned[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            
            mask = (df_cleaned[col] >= lower_bound) & (df_cleaned[col] <= upper_bound)
            outliers = (~mask).sum()
            df_cleaned = df_cleaned[mask]
            
            if outliers > 0:
                logger.info(f"Removed {outliers} outliers from '{col}' using IQR method")
        
        elif method == "zscore":
            z_scores = np.abs((df_cleaned[col] - df_cleaned[col].mean()) / df_cleaned[col].std())
            mask = z_scores <= threshold
            outliers = (~mask).sum()
            df_cleaned = df_cleaned[mask]
            
            if outliers > 0:
                logger.info(f"Removed {outliers} outliers from '{col}' using Z-score method")
    
    total_outliers = initial_rows - len(df_cleaned)
    if total_outliers > 0:
        logger.info(f"Total outliers removed: {total_outliers} ({total_outliers/initial_rows*100:.2f}%)")
    
    return df_cleaned
