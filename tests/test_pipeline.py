"""Unit tests for the main pipeline."""

import unittest
import pandas as pd
import tempfile
from pathlib import Path

from src.pipeline import DataCleaningPipeline
from src.exceptions import PipelineError


class TestDataCleaningPipeline(unittest.TestCase):
    """Test cases for the main data cleaning pipeline."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create test data with issues
        self.test_data = pd.DataFrame({
            'ID': [1, 2, 2, 3, 4],  # Has duplicate
            'Name': ['  Alice  ', 'Bob', 'Bob', 'Charlie', None],  # Has whitespace and missing
            'Age': [25, 30, 30, 35, 40],
            'Email': ['alice@test.com', 'bob@test.com', 'bob@test.com', 'charlie@test.com', 'dave@test.com']
        })
        
        # Save test data to CSV
        self.input_path = Path(self.temp_dir) / "input.csv"
        self.test_data.to_csv(self.input_path, index=False)
        
        self.output_dir = Path(self.temp_dir) / "output"
    
    def test_pipeline_initialization(self):
        """Test pipeline initialization."""
        pipeline = DataCleaningPipeline(
            input_path=self.input_path,
            output_dir=self.output_dir,
            output_filename="cleaned.csv"
        )
        
        self.assertEqual(pipeline.input_path, self.input_path)
        self.assertEqual(pipeline.output_dir, self.output_dir)
        self.assertEqual(pipeline.output_filename, "cleaned.csv")
    
    def test_pipeline_run_success(self):
        """Test successful pipeline execution."""
        pipeline = DataCleaningPipeline(
            input_path=self.input_path,
            output_dir=self.output_dir,
            output_filename="cleaned.csv"
        )
        
        result = pipeline.run(
            remove_duplicates_flag=True,
            missing_value_strategy='auto',
            standardize_columns=True
        )
        
        # Verify result
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertLess(len(result), len(self.test_data))  # Should remove duplicates
        
        # Verify output file exists
        output_file = self.output_dir / "cleaned.csv"
        self.assertTrue(output_file.exists())
        
        # Verify quality report exists
        report_file = self.output_dir / "quality_report.json"
        self.assertTrue(report_file.exists())
    
    def test_pipeline_get_cleaned_data(self):
        """Test getting cleaned data from pipeline."""
        pipeline = DataCleaningPipeline(
            input_path=self.input_path,
            output_dir=self.output_dir,
            output_filename="cleaned.csv"
        )
        
        pipeline.run()
        cleaned_data = pipeline.get_cleaned_data()
        
        self.assertIsNotNone(cleaned_data)
        self.assertIsInstance(cleaned_data, pd.DataFrame)
    
    def test_pipeline_get_quality_report(self):
        """Test getting quality report from pipeline."""
        pipeline = DataCleaningPipeline(
            input_path=self.input_path,
            output_dir=self.output_dir,
            output_filename="cleaned.csv"
        )
        
        pipeline.run()
        report = pipeline.get_quality_report()
        
        self.assertIsNotNone(report)
        self.assertIn('total_rows', report)
        self.assertIn('total_columns', report)
        self.assertIn('column_stats', report)


if __name__ == '__main__':
    unittest.main()
