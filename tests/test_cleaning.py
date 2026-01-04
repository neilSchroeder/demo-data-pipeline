"""Unit tests for data cleaning module."""

import unittest
import pandas as pd
import numpy as np

from src.cleaning import (
    remove_duplicates,
    handle_missing_values,
    standardize_column_names,
    clean_text_columns,
    remove_outliers
)


class TestDataCleaning(unittest.TestCase):
    """Test cases for data cleaning functions."""
    
    def test_remove_duplicates(self):
        """Test duplicate removal."""
        df = pd.DataFrame({
            'id': [1, 2, 2, 3],
            'value': ['a', 'b', 'b', 'c']
        })
        
        result = remove_duplicates(df)
        
        self.assertEqual(len(result), 3)
        self.assertEqual(list(result['id']), [1, 2, 3])
    
    def test_handle_missing_values_auto(self):
        """Test automatic missing value handling."""
        df = pd.DataFrame({
            'numeric': [1, 2, np.nan, 4, 5],
            'text': ['a', 'b', np.nan, 'd', 'e']
        })
        
        result = handle_missing_values(df, strategy='auto')
        
        # Should have no missing values
        self.assertEqual(result.isnull().sum().sum(), 0)
    
    def test_handle_missing_values_drop_rows(self):
        """Test dropping rows with missing values."""
        df = pd.DataFrame({
            'col1': [1, 2, np.nan, 4],
            'col2': ['a', 'b', 'c', 'd']
        })
        
        result = handle_missing_values(df, strategy='drop_rows')
        
        self.assertEqual(len(result), 3)
        self.assertEqual(result.isnull().sum().sum(), 0)
    
    def test_standardize_column_names(self):
        """Test column name standardization."""
        df = pd.DataFrame({
            'First Name': [1, 2],
            'Last-Name': [3, 4],
            ' Email Address ': [5, 6]
        })
        
        result = standardize_column_names(df)
        
        expected_cols = ['first_name', 'lastname', 'email_address']
        self.assertEqual(list(result.columns), expected_cols)
    
    def test_clean_text_columns(self):
        """Test text column cleaning."""
        df = pd.DataFrame({
            'name': ['  Alice  ', 'Bob  ', '  Charlie'],
            'city': ['New York', '  LA  ', 'Chicago  ']
        })
        
        result = clean_text_columns(df)
        
        self.assertEqual(result['name'].iloc[0], 'Alice')
        self.assertEqual(result['city'].iloc[1], 'LA')
    
    def test_remove_outliers_iqr(self):
        """Test outlier removal using IQR method."""
        df = pd.DataFrame({
            'value': [1, 2, 3, 4, 5, 100]  # 100 is an outlier
        })
        
        result = remove_outliers(df, columns=['value'], method='iqr')
        
        self.assertLess(len(result), len(df))
        self.assertNotIn(100, result['value'].values)


if __name__ == '__main__':
    unittest.main()
