"""Unit tests for data ingestion module."""

import unittest
import pandas as pd
import tempfile
from pathlib import Path

from src.ingestion import ingest_csv, get_data_info
from src.exceptions import DataIngestionError


class TestDataIngestion(unittest.TestCase):
    """Test cases for data ingestion functions."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
    
    def test_ingest_csv_success(self):
        """Test successful CSV ingestion."""
        # Create a temporary CSV file
        csv_path = Path(self.temp_dir) / "test.csv"
        self.test_data.to_csv(csv_path, index=False)
        
        # Ingest the file
        df = ingest_csv(csv_path)
        
        # Verify
        self.assertEqual(len(df), 3)
        self.assertEqual(list(df.columns), ['id', 'name', 'age'])
        pd.testing.assert_frame_equal(df, self.test_data)
    
    def test_ingest_csv_file_not_found(self):
        """Test ingestion with non-existent file."""
        non_existent_path = Path(self.temp_dir) / "nonexistent.csv"
        
        with self.assertRaises(DataIngestionError):
            ingest_csv(non_existent_path)
    
    def test_get_data_info(self):
        """Test data info extraction."""
        info = get_data_info(self.test_data)
        
        self.assertEqual(info['rows'], 3)
        self.assertEqual(info['columns'], 3)
        self.assertEqual(info['column_names'], ['id', 'name', 'age'])
        self.assertIn('id', info['dtypes'])
        self.assertIn('missing_values', info)


if __name__ == '__main__':
    unittest.main()
