# Data Cleaning Pipeline

A production-ready automated data cleaning and ETL pipeline built with Python. This project demonstrates professional data engineering practices including data ingestion, cleaning, validation, and export with comprehensive logging and error handling.

## 🎯 Features

- **Automated Data Ingestion**: Load CSV data with robust error handling
- **Intelligent Data Cleaning**:
  - Remove duplicate records
  - Handle missing values with multiple strategies
  - Standardize column names and text formatting
  - Parse dates in multiple formats
  - Remove outliers using IQR or Z-score methods
- **Data Validation**: Schema, data type, range, and completeness checks
- **Quality Reporting**: Generate detailed data quality metrics
- **Multiple Export Formats**: CSV and JSON support
- **Production Features**:
  - Comprehensive logging to file and console
  - Custom exception hierarchy
  - Type hints throughout
  - Configurable settings
  - Clean, modular architecture

## 📁 Project Structure

```
demo-data-pipeline/
├── data/
│   ├── raw/              # Raw input data
│   ├── processed/        # Cleaned output data
│   └── sample/           # Sample datasets
├── src/
│   ├── __init__.py       # Package initialization
│   ├── config.py         # Configuration settings
│   ├── exceptions.py     # Custom exceptions
│   ├── logger.py         # Logging setup
│   ├── ingestion.py      # Data loading
│   ├── cleaning.py       # Data cleaning functions
│   ├── validation.py     # Data quality validation
│   ├── export.py         # Data export functions
│   ├── pipeline.py       # Main pipeline orchestrator
│   └── data_generator.py # Sample data generator
├── tests/
│   ├── __init__.py       # Test runner
│   ├── test_ingestion.py
│   ├── test_cleaning.py
│   ├── test_validation.py
│   ├── test_export.py
│   └── test_pipeline.py
├── logs/                 # Application logs
├── main.py               # CLI entry point
├── requirements.txt      # Python dependencies
├── .gitignore           
└── README.md

```

## 🚀 Quick Start

### Installation

1. Clone the repository:
```bash
git clone https://github.com/neilSchroeder/demo-data-pipeline.git
cd demo-data-pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

### Usage

#### Generate Sample Data

Generate messy sample data for testing:
```bash
python main.py --generate-sample --rows 1000
```

This creates a CSV file with intentional data quality issues:
- Duplicate rows (~5%)
- Missing values (~10%)
- Whitespace and formatting issues
- Inconsistent date formats
- Column name formatting problems
- Outliers in numeric fields

#### Run the Pipeline

Clean the sample data:
```bash
python main.py
```

Clean a specific file:
```bash
python main.py --input path/to/your/data.csv --output-dir path/to/output
```

Advanced options:
```bash
python main.py \
  --input data/raw/messy_data.csv \
  --output-dir data/processed \
  --remove-outliers \
  --export-format json
```

#### Command Line Options

```
--input PATH              Input CSV file path
--output-dir PATH         Output directory (default: data/processed)
--generate-sample         Generate sample messy data
--rows N                  Number of rows for sample data (default: 1000)
--no-duplicates          Skip duplicate removal
--no-standardize         Skip column name standardization
--remove-outliers        Enable outlier removal
--export-format {csv,json}  Export format (default: csv)
```

### Using as a Library

```python
from src.pipeline import DataCleaningPipeline
from pathlib import Path

# Initialize pipeline
pipeline = DataCleaningPipeline(
    input_path=Path("data/raw/messy_data.csv"),
    output_dir=Path("data/processed"),
    output_filename="cleaned_data.csv"
)

# Run pipeline with options
cleaned_data = pipeline.run(
    remove_duplicates_flag=True,
    missing_value_strategy="auto",
    standardize_columns=True,
    date_columns=["signup_date", "last_login"],
    remove_outliers_flag=True,
    export_format="csv"
)

# Get results
quality_report = pipeline.get_quality_report()
print(f"Cleaned {quality_report['total_rows']} rows")
```

### Individual Functions

```python
from src.cleaning import (
    remove_duplicates,
    handle_missing_values,
    standardize_column_names,
    clean_text_columns
)
from src.validation import validate_schema, generate_data_quality_report
import pandas as pd

# Load data
df = pd.read_csv("data/raw/messy_data.csv")

# Clean step by step
df = remove_duplicates(df)
df = standardize_column_names(df)
df = handle_missing_values(df, strategy="auto")
df = clean_text_columns(df)

# Validate
validate_schema(df, expected_columns=["id", "name", "email"])
report = generate_data_quality_report(df)

# Export
df.to_csv("data/processed/cleaned.csv", index=False)
```

## 🧪 Testing

Run all tests:
```bash
python -m pytest tests/ -v
```

Or using unittest:
```bash
python -m unittest discover tests -v
```

Run specific test module:
```bash
python -m pytest tests/test_cleaning.py -v
```

## 📊 Pipeline Workflow

1. **Ingestion**: Load raw CSV data with encoding detection
2. **Cleaning**:
   - Standardize column names (lowercase, underscores)
   - Remove duplicate rows
   - Handle missing values (drop high-missing columns, impute remaining)
   - Clean text fields (strip whitespace)
   - Parse date columns with multiple format attempts
   - Remove outliers (optional)
3. **Validation**: Generate data quality metrics
4. **Export**: Save cleaned data and quality report

## 🛠️ Configuration

Edit `src/config.py` to customize:

```python
# Missing value threshold for column removal
MISSING_VALUE_THRESHOLD = 0.5  # 50%

# Date formats to try when parsing
DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", ...]

# Logging level
LOG_LEVEL = "INFO"
```

## 📝 Data Quality Report

The pipeline generates a comprehensive quality report including:

- Total rows and columns
- Duplicate count
- Missing value statistics (overall and per-column)
- Column-level statistics:
  - Data types
  - Unique value counts
  - Numeric statistics (mean, median, std, min, max)
  - Missing value percentages

Example output:
```json
{
  "total_rows": 950,
  "total_columns": 9,
  "duplicate_rows": 0,
  "missing_percentage": 0.0,
  "column_stats": {
    "customer_id": {
      "dtype": "int64",
      "missing_count": 0,
      "unique_values": 950,
      "mean": 500.5,
      "median": 500.5
    }
  }
}
```

## 🔍 Error Handling

Custom exception hierarchy for clear error reporting:

- `PipelineError`: Base exception
- `DataIngestionError`: File reading issues
- `DataCleaningError`: Cleaning operation failures
- `DataValidationError`: Validation failures
- `DataExportError`: Export operation failures

## 📈 Logging

Comprehensive logging to both console and file (`logs/pipeline.log`):

```
2024-01-04 10:30:15 - src.pipeline - INFO - Starting Data Cleaning Pipeline
2024-01-04 10:30:15 - src.ingestion - INFO - Successfully ingested 1050 rows and 9 columns
2024-01-04 10:30:15 - src.cleaning - INFO - Removed 50 duplicate rows (4.76%)
2024-01-04 10:30:16 - src.cleaning - INFO - Final missing values: 0
2024-01-04 10:30:16 - src.export - INFO - Successfully exported 950 rows
```

## 🎓 Key Concepts Demonstrated

- **ETL Pipeline Architecture**: Modular, testable design
- **Data Quality Management**: Comprehensive validation and reporting
- **Error Handling**: Custom exceptions with detailed context
- **Logging**: Structured logging for debugging and monitoring
- **Configuration Management**: Centralized settings
- **Type Safety**: Type hints throughout
- **Testing**: Unit tests for all modules
- **Documentation**: Docstrings and usage examples
- **Professional Practices**: PEP 8 style, clean code principles

## 📦 Dependencies

- `pandas >= 2.0.0`: Data manipulation
- `numpy >= 1.24.0`: Numerical operations
- `python-dateutil >= 2.8.2`: Date parsing

## 🤝 Contributing

This is a portfolio project demonstrating data engineering skills. Feel free to fork and adapt for your own needs.

## 📄 License

MIT License - feel free to use this project as a template or learning resource.

## 👤 Author

Built as a portfolio project to demonstrate production-ready Python ETL development skills.

## 🌟 Skills Showcased

- Python programming (OOP, type hints, best practices)
- Data engineering (ETL pipelines, data quality)
- Software engineering (testing, logging, error handling)
- Documentation and code clarity
- Production-ready code design