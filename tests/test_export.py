"""Unit tests for data export module."""

import unittest
import pandas as pd
import tempfile
import json
from pathlib import Path

from src.export import export_to_csv, export_to_json, export_summary_report
from src.exceptions import DataExportError


class TestDataExport(unittest.TestCase):
    """Test cases for data export functions."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
    
    def test_export_to_csv_success(self):
        """Test successful CSV export."""
        output_path = Path(self.temp_dir) / "output.csv"
        
        result_path = export_to_csv(self.test_data, output_path)
        
        self.assertTrue(output_path.exists())
        self.assertEqual(result_path, output_path)
        
        # Verify content
        df = pd.read_csv(output_path)
        pd.testing.assert_frame_equal(df, self.test_data)
    
    def test_export_to_json_success(self):
        """Test successful JSON export."""
        output_path = Path(self.temp_dir) / "output.json"
        
        result_path = export_to_json(self.test_data, output_path)
        
        self.assertTrue(output_path.exists())
        self.assertEqual(result_path, output_path)
        
        # Verify content
        df = pd.read_json(output_path)
        pd.testing.assert_frame_equal(df, self.test_data)
    
    def test_export_summary_report(self):
        """Test summary report export."""
        output_path = Path(self.temp_dir) / "report.json"
        test_report = {
            'total_rows': 100,
            'total_columns': 5,
            'missing_values': 10
        }
        
        result_path = export_summary_report(test_report, output_path)
        
        self.assertTrue(output_path.exists())
        self.assertEqual(result_path, output_path)
        
        # Verify content
        with open(output_path, 'r') as f:
            loaded_report = json.load(f)
        
        self.assertEqual(loaded_report, test_report)


if __name__ == '__main__':
    unittest.main()
