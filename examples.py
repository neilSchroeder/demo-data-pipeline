"""
Example usage of the data cleaning pipeline.

This script demonstrates various ways to use the pipeline with different options.
"""

from pathlib import Path
from src.pipeline import DataCleaningPipeline
from src.data_generator import generate_sample_data
from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR

# Example 1: Generate and clean sample data
print("=" * 70)
print("Example 1: Generate and clean sample data")
print("=" * 70)

# Generate messy data
sample_file = RAW_DATA_DIR / "example_messy.csv"
df_messy = generate_sample_data(num_rows=500, output_path=sample_file, messy=True)
print(f"\nGenerated messy data: {len(df_messy)} rows")
print(f"Missing values: {df_messy.isnull().sum().sum()}")
print(f"Duplicates: {df_messy.duplicated().sum()}")

# Run basic cleaning pipeline
pipeline1 = DataCleaningPipeline(
    input_path=sample_file,
    output_dir=PROCESSED_DATA_DIR / "example1",
    output_filename="cleaned_basic.csv"
)

cleaned_df1 = pipeline1.run(
    remove_duplicates_flag=True,
    missing_value_strategy="auto",
    standardize_columns=True
)

print(f"\nCleaned data: {len(cleaned_df1)} rows")
print(f"Missing values: {cleaned_df1.isnull().sum().sum()}")

# Example 2: Advanced cleaning with outlier removal
print("\n" + "=" * 70)
print("Example 2: Advanced cleaning with outlier removal")
print("=" * 70)

pipeline2 = DataCleaningPipeline(
    input_path=sample_file,
    output_dir=PROCESSED_DATA_DIR / "example2",
    output_filename="cleaned_advanced.csv"
)

cleaned_df2 = pipeline2.run(
    remove_duplicates_flag=True,
    missing_value_strategy="auto",
    standardize_columns=True,
    date_columns=["signup_date"],
    remove_outliers_flag=True,
    outlier_columns=["age", "purchaseamount"],
    export_format="csv"
)

report = pipeline2.get_quality_report()
print(f"\nCleaned data: {len(cleaned_df2)} rows")
print(f"Quality score: {100 - report['missing_percentage']:.2f}% complete")

# Example 3: Minimal cleaning (keep most data)
print("\n" + "=" * 70)
print("Example 3: Minimal cleaning (preserve more data)")
print("=" * 70)

pipeline3 = DataCleaningPipeline(
    input_path=sample_file,
    output_dir=PROCESSED_DATA_DIR / "example3",
    output_filename="cleaned_minimal.csv"
)

cleaned_df3 = pipeline3.run(
    remove_duplicates_flag=True,
    missing_value_strategy="fill",  # Just fill with default values
    standardize_columns=False,  # Keep original column names
    remove_outliers_flag=False  # Keep all data points
)

print(f"\nCleaned data: {len(cleaned_df3)} rows")
print(f"Columns preserved: {list(cleaned_df3.columns)}")

# Example 4: Export to JSON
print("\n" + "=" * 70)
print("Example 4: Export to JSON format")
print("=" * 70)

pipeline4 = DataCleaningPipeline(
    input_path=sample_file,
    output_dir=PROCESSED_DATA_DIR / "example4",
    output_filename="cleaned_data.json"
)

cleaned_df4 = pipeline4.run(
    remove_duplicates_flag=True,
    missing_value_strategy="auto",
    standardize_columns=True,
    export_format="json"
)

print(f"\nData exported to JSON: {PROCESSED_DATA_DIR / 'example4' / 'cleaned_data.json'}")

print("\n" + "=" * 70)
print("All examples completed successfully!")
print("=" * 70)
