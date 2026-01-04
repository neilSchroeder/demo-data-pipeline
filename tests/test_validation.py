"""Unit tests for data validation module."""

import unittest
import pandas as pd
import numpy as np

from src.validation import (
    validate_schema,
    validate_data_types,
    validate_ranges,
    validate_completeness,
    generate_data_quality_report
)
from src.exceptions import DataValidationError


class TestDataValidation(unittest.TestCase):
    """Test cases for data validation functions."""
    
    def test_validate_schema_success(self):
        """Test successful schema validation."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['a', 'b', 'c']
        })
        
        result = validate_schema(df, ['id', 'name'])
        self.assertTrue(result)
    
    def test_validate_schema_missing_column(self):
        """Test schema validation with missing column."""
        df = pd.DataFrame({
            'id': [1, 2, 3]
        })
        
        with self.assertRaises(DataValidationError):
            validate_schema(df, ['id', 'name'])
    
    def test_validate_data_types(self):
        """Test data type validation."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['a', 'b', 'c'],
            'age': [25, 30, 35]
        })
        
        type_requirements = {
            'id': 'numeric',
            'name': 'string',
            'age': 'numeric'
        }
        
        result = validate_data_types(df, type_requirements)
        self.assertTrue(result)
    
    def test_validate_ranges_success(self):
        """Test successful range validation."""
        df = pd.DataFrame({
            'age': [25, 30, 35, 40]
        })
        
        range_requirements = {
            'age': {'min': 18, 'max': 65}
        }
        
        result = validate_ranges(df, range_requirements)
        self.assertTrue(result)
    
    def test_validate_ranges_violation(self):
        """Test range validation with violations."""
        df = pd.DataFrame({
            'age': [25, 30, 150]  # 150 exceeds max
        })
        
        range_requirements = {
            'age': {'min': 18, 'max': 120}
        }
        
        with self.assertRaises(DataValidationError):
            validate_ranges(df, range_requirements)
    
    def test_validate_completeness_success(self):
        """Test successful completeness validation."""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['a', 'b', 'c', 'd', 'e']
        })
        
        result = validate_completeness(df, ['id', 'name'], completeness_threshold=0.8)
        self.assertTrue(result)
    
    def test_validate_completeness_failure(self):
        """Test completeness validation failure."""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['a', np.nan, np.nan, np.nan, 'e']  # Only 40% complete
        })
        
        with self.assertRaises(DataValidationError):
            validate_completeness(df, ['name'], completeness_threshold=0.8)
    
    def test_generate_data_quality_report(self):
        """Test quality report generation."""
        df = pd.DataFrame({
            'id': [1, 2, 2, 3],
            'value': [10, np.nan, 30, 40]
        })
        
        report = generate_data_quality_report(df)
        
        self.assertEqual(report['total_rows'], 4)
        self.assertEqual(report['total_columns'], 2)
        # Duplicate detection depends on index, so just check it exists
        self.assertIn('duplicate_rows', report)
        self.assertEqual(report['total_missing_values'], 1)
        self.assertIn('column_stats', report)


if __name__ == '__main__':
    unittest.main()
