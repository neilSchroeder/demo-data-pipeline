"""Data validation module to ensure data quality."""

from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np

from .exceptions import DataValidationError
from .logger import setup_logger

logger = setup_logger(__name__)


def validate_schema(
    df: pd.DataFrame,
    expected_columns: List[str],
    strict: bool = False
) -> bool:
    """
    Validate that the dataframe has expected columns.
    
    Args:
        df: Input dataframe
        expected_columns: List of expected column names
        strict: If True, dataframe must have exactly these columns
        
    Returns:
        True if validation passes
        
    Raises:
        DataValidationError: If validation fails
    """
    actual_columns = set(df.columns)
    expected_set = set(expected_columns)
    
    if strict:
        if actual_columns != expected_set:
            missing = expected_set - actual_columns
            extra = actual_columns - expected_set
            error_msg = f"Schema mismatch. Missing: {missing}, Extra: {extra}"
            logger.error(error_msg)
            raise DataValidationError(error_msg)
    else:
        missing = expected_set - actual_columns
        if missing:
            error_msg = f"Missing required columns: {missing}"
            logger.error(error_msg)
            raise DataValidationError(error_msg)
    
    logger.info("Schema validation passed")
    return True


def validate_data_types(
    df: pd.DataFrame,
    type_requirements: Dict[str, str]
) -> bool:
    """
    Validate that columns have expected data types.
    
    Args:
        df: Input dataframe
        type_requirements: Dictionary mapping column names to expected types
        
    Returns:
        True if validation passes
        
    Raises:
        DataValidationError: If validation fails
    """
    errors = []
    
    for col, expected_type in type_requirements.items():
        if col not in df.columns:
            errors.append(f"Column '{col}' not found")
            continue
        
        actual_type = str(df[col].dtype)
        
        # Flexible type checking
        if expected_type == "numeric":
            if not pd.api.types.is_numeric_dtype(df[col]):
                errors.append(f"Column '{col}' should be numeric, got {actual_type}")
        elif expected_type == "datetime":
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                errors.append(f"Column '{col}' should be datetime, got {actual_type}")
        elif expected_type == "string":
            if not pd.api.types.is_string_dtype(df[col]) and not pd.api.types.is_object_dtype(df[col]):
                errors.append(f"Column '{col}' should be string, got {actual_type}")
        else:
            # Exact match for specific types
            if expected_type not in actual_type:
                errors.append(f"Column '{col}' should be {expected_type}, got {actual_type}")
    
    if errors:
        error_msg = "Data type validation failed: " + "; ".join(errors)
        logger.error(error_msg)
        raise DataValidationError(error_msg)
    
    logger.info("Data type validation passed")
    return True


def validate_ranges(
    df: pd.DataFrame,
    range_requirements: Dict[str, Dict[str, Any]]
) -> bool:
    """
    Validate that numeric columns fall within expected ranges.
    
    Args:
        df: Input dataframe
        range_requirements: Dictionary mapping column names to range specs
            e.g., {'age': {'min': 0, 'max': 120}}
        
    Returns:
        True if validation passes
        
    Raises:
        DataValidationError: If validation fails
    """
    errors = []
    
    for col, ranges in range_requirements.items():
        if col not in df.columns:
            errors.append(f"Column '{col}' not found")
            continue
        
        if not pd.api.types.is_numeric_dtype(df[col]):
            errors.append(f"Column '{col}' is not numeric, cannot validate range")
            continue
        
        if 'min' in ranges:
            min_val = ranges['min']
            violations = (df[col] < min_val).sum()
            if violations > 0:
                errors.append(f"Column '{col}' has {violations} values below minimum {min_val}")
        
        if 'max' in ranges:
            max_val = ranges['max']
            violations = (df[col] > max_val).sum()
            if violations > 0:
                errors.append(f"Column '{col}' has {violations} values above maximum {max_val}")
    
    if errors:
        error_msg = "Range validation failed: " + "; ".join(errors)
        logger.error(error_msg)
        raise DataValidationError(error_msg)
    
    logger.info("Range validation passed")
    return True


def validate_completeness(
    df: pd.DataFrame,
    required_columns: List[str],
    completeness_threshold: float = 0.95
) -> bool:
    """
    Validate that required columns meet completeness threshold.
    
    Args:
        df: Input dataframe
        required_columns: Columns that must meet completeness threshold
        completeness_threshold: Minimum fraction of non-null values (default: 0.95)
        
    Returns:
        True if validation passes
        
    Raises:
        DataValidationError: If validation fails
    """
    errors = []
    
    for col in required_columns:
        if col not in df.columns:
            errors.append(f"Required column '{col}' not found")
            continue
        
        completeness = 1 - (df[col].isnull().sum() / len(df))
        if completeness < completeness_threshold:
            errors.append(
                f"Column '{col}' completeness {completeness:.2%} "
                f"below threshold {completeness_threshold:.2%}"
            )
    
    if errors:
        error_msg = "Completeness validation failed: " + "; ".join(errors)
        logger.error(error_msg)
        raise DataValidationError(error_msg)
    
    logger.info("Completeness validation passed")
    return True


def generate_data_quality_report(df: pd.DataFrame, subset: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Generate a comprehensive data quality report.
    
    Args:
        df: Input dataframe
        subset: Column subset to use for duplicate detection (default: all columns)
        
    Returns:
        Dictionary containing quality metrics
    """
    report = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "duplicate_rows": df.duplicated(subset=subset).sum(),
        "columns_with_missing": (df.isnull().any()).sum(),
        "total_missing_values": df.isnull().sum().sum(),
        "missing_percentage": (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100,
        "column_stats": {}
    }
    
    for col in df.columns:
        stats = {
            "dtype": str(df[col].dtype),
            "missing_count": int(df[col].isnull().sum()),
            "missing_percentage": float((df[col].isnull().sum() / len(df)) * 100),
            "unique_values": int(df[col].nunique())
        }
        
        if pd.api.types.is_numeric_dtype(df[col]):
            stats.update({
                "mean": float(df[col].mean()) if not df[col].isnull().all() else None,
                "median": float(df[col].median()) if not df[col].isnull().all() else None,
                "std": float(df[col].std()) if not df[col].isnull().all() else None,
                "min": float(df[col].min()) if not df[col].isnull().all() else None,
                "max": float(df[col].max()) if not df[col].isnull().all() else None,
            })
        
        report["column_stats"][col] = stats
    
    logger.info(f"Generated data quality report: {report['total_rows']} rows, "
                f"{report['total_missing_values']} missing values "
                f"({report['missing_percentage']:.2f}%)")
    
    return report
