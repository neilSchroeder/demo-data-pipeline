# Developer Documentation

## Architecture Overview

The data cleaning pipeline follows a modular architecture with clear separation of concerns:

```
┌─────────────┐
│   main.py   │  CLI Entry Point
└──────┬──────┘
       │
       v
┌─────────────────────────────────────────────┐
│            pipeline.py                       │
│   DataCleaningPipeline (Orchestrator)       │
└──┬──────────┬──────────┬──────────┬────────┘
   │          │          │          │
   v          v          v          v
┌──────┐ ┌─────────┐ ┌──────────┐ ┌────────┐
│ingest│ │cleaning │ │validation│ │ export │
└──────┘ └─────────┘ └──────────┘ └────────┘
   │          │          │          │
   └──────────┴──────────┴──────────┘
              │
         ┌────┴─────┐
         │  config  │
         │  logger  │
         │exceptions│
         └──────────┘
```

## Module Responsibilities

### `config.py`
- Centralized configuration
- Path management
- Default settings for data cleaning

### `exceptions.py`
- Custom exception hierarchy
- Clear error types for different failure modes

### `logger.py`
- Dual logging (console + file)
- Configurable log levels
- Structured log formatting

### `ingestion.py`
- CSV file loading with error handling
- Data info extraction
- Encoding detection support

### `cleaning.py`
Core cleaning operations:
- `remove_duplicates()`: Eliminate duplicate rows
- `handle_missing_values()`: Multiple strategies (auto, drop, fill)
- `standardize_column_names()`: Lowercase, underscores
- `clean_text_columns()`: Trim whitespace
- `parse_dates()`: Multi-format date parsing
- `remove_outliers()`: IQR and Z-score methods

### `validation.py`
Data quality checks:
- `validate_schema()`: Column presence validation
- `validate_data_types()`: Type checking
- `validate_ranges()`: Numeric bounds validation
- `validate_completeness()`: Missing value thresholds
- `generate_data_quality_report()`: Comprehensive metrics

### `export.py`
- `export_to_csv()`: CSV output with options
- `export_to_json()`: JSON output
- `export_summary_report()`: Quality report JSON

### `pipeline.py`
Main orchestrator that:
1. Coordinates all cleaning steps
2. Manages data flow
3. Handles errors gracefully
4. Generates quality reports
5. Exports results

### `data_generator.py`
- Generates synthetic test data
- Introduces controlled data quality issues
- Supports reproducible testing

## Code Style Guidelines

### Type Hints
All functions use type hints:
```python
def remove_duplicates(
    df: pd.DataFrame,
    subset: Optional[List[str]] = None,
    keep: str = "first"
) -> pd.DataFrame:
```

### Docstrings
Google-style docstrings for all public functions:
```python
def validate_schema(df: pd.DataFrame, expected_columns: List[str]) -> bool:
    """
    Validate that the dataframe has expected columns.
    
    Args:
        df: Input dataframe
        expected_columns: List of expected column names
        
    Returns:
        True if validation passes
        
    Raises:
        DataValidationError: If validation fails
    """
```

### Logging
Comprehensive logging at appropriate levels:
```python
logger.info("Starting data ingestion from {file_path}")
logger.debug(f"Columns: {list(df.columns)}")
logger.warning("Date column '{col}' not found")
logger.error(error_msg)
```

### Error Handling
Use custom exceptions with context:
```python
try:
    df = pd.read_csv(file_path)
except Exception as e:
    error_msg = f"Failed to read CSV: {str(e)}"
    logger.error(error_msg)
    raise DataIngestionError(error_msg) from e
```

## Testing Strategy

### Unit Tests
Each module has dedicated tests:
- `test_ingestion.py`: File loading, error cases
- `test_cleaning.py`: Each cleaning function
- `test_validation.py`: Validation rules
- `test_export.py`: Export formats
- `test_pipeline.py`: End-to-end pipeline

### Test Structure
```python
class TestDataCleaning(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.test_data = pd.DataFrame({...})
    
    def test_remove_duplicates(self):
        """Test duplicate removal."""
        result = remove_duplicates(self.test_data)
        self.assertEqual(len(result), expected_length)
```

### Running Tests
```bash
# All tests
python -m unittest discover tests -v

# Specific module
python -m unittest tests.test_cleaning -v

# Single test
python -m unittest tests.test_cleaning.TestDataCleaning.test_remove_duplicates
```

## Adding New Features

### Adding a New Cleaning Function

1. **Add to `cleaning.py`**:
```python
def new_cleaning_function(
    df: pd.DataFrame,
    param: str
) -> pd.DataFrame:
    """
    Description of function.
    
    Args:
        df: Input dataframe
        param: Description
        
    Returns:
        Cleaned dataframe
    """
    logger.info("Applying new cleaning function")
    df_cleaned = df.copy()
    # Your logic here
    return df_cleaned
```

2. **Add tests in `test_cleaning.py`**:
```python
def test_new_cleaning_function(self):
    """Test new cleaning function."""
    result = new_cleaning_function(self.test_data, param="value")
    self.assertEqual(expected, actual)
```

3. **Integrate into pipeline** in `pipeline.py`:
```python
if use_new_function:
    logger.info("- Applying new function")
    self.cleaned_data = new_cleaning_function(self.cleaned_data, param)
```

4. **Update CLI** in `main.py`:
```python
parser.add_argument("--use-new-function", action="store_true")
```

5. **Document** in README.md

### Adding a New Validation

Follow similar pattern in `validation.py`:
1. Create validation function
2. Add tests
3. Optionally integrate into pipeline
4. Document

## Performance Considerations

### Memory Management
- Use `df.copy()` sparingly
- Consider chunked processing for large files
- Monitor memory with `df.memory_usage(deep=True)`

### Optimization Tips
- Use vectorized pandas operations
- Avoid iterating over rows when possible
- Use `.loc` and `.iloc` for indexing
- Consider `category` dtype for text columns with few unique values

### Profiling
```python
import cProfile
import pstats

profiler = cProfile.Profile()
profiler.enable()
# Your code
profiler.disable()
stats = pstats.Stats(profiler)
stats.sort_stats('cumulative')
stats.print_stats(10)
```

## Configuration

### Environment-Specific Settings
Modify `src/config.py` for:
- Path locations
- Cleaning thresholds
- Date formats
- Log levels

### Runtime Configuration
Pass parameters to pipeline:
```python
pipeline = DataCleaningPipeline(input_path, output_dir)
pipeline.run(
    missing_value_strategy="drop_rows",  # Override default
    remove_outliers_flag=True
)
```

## Debugging

### Enable Debug Logging
Edit `src/config.py`:
```python
LOG_LEVEL = "DEBUG"
```

### Check Log Files
```bash
tail -f logs/pipeline.log
```

### Interactive Debugging
```python
import pdb

# In your code
pdb.set_trace()

# Or post-mortem debugging
try:
    pipeline.run()
except Exception:
    import pdb
    pdb.post_mortem()
```

## Deployment Considerations

### Production Checklist
- [ ] Set `LOG_LEVEL = "INFO"` or `"WARNING"`
- [ ] Configure proper output directories
- [ ] Set up log rotation
- [ ] Monitor disk space for data/logs
- [ ] Add health checks
- [ ] Set up alerting for failures
- [ ] Document operational procedures

### Scheduled Runs
Use cron or similar:
```bash
# Run daily at 2 AM
0 2 * * * cd /path/to/pipeline && python main.py --input /data/daily.csv
```

### Docker Container
Example Dockerfile:
```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "main.py"]
```

## Contributing Guidelines

1. **Fork and Branch**: Create feature branches
2. **Write Tests**: All new code needs tests
3. **Follow Style**: PEP 8, type hints, docstrings
4. **Document**: Update README and this file
5. **Test**: Run full test suite before PR
6. **Log**: Add appropriate logging
7. **Review**: Get code review before merge

## Troubleshooting

### Common Issues

**Issue**: Import errors
```bash
# Solution: Install dependencies
pip install -r requirements.txt
```

**Issue**: "File not found" errors
```bash
# Solution: Check paths in config.py
# Ensure data directories exist
```

**Issue**: Out of memory errors
```bash
# Solution: Process in chunks or reduce data size
# Consider using dask for very large files
```

**Issue**: Test failures
```bash
# Solution: Check test isolation
# Ensure setUp/tearDown properly clean up
```

## Resources

- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Python Logging HOWTO](https://docs.python.org/3/howto/logging.html)
- [PEP 8 Style Guide](https://pep8.org/)
- [Type Hints Cheat Sheet](https://mypy.readthedocs.io/en/stable/cheat_sheet_py3.html)
