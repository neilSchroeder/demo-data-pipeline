#!/usr/bin/env python3
"""
Main entry point for the data cleaning pipeline.

This script demonstrates how to use the pipeline with sample data.
"""

import argparse
from pathlib import Path

from src.pipeline import DataCleaningPipeline
from src.data_generator import generate_sample_data
from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR, SAMPLE_DATA_DIR
from src.logger import setup_logger

logger = setup_logger(__name__)


def main():
    """Run the data cleaning pipeline."""
    parser = argparse.ArgumentParser(
        description="Data Cleaning Pipeline - Clean and validate messy CSV data"
    )
    parser.add_argument(
        "--input",
        type=str,
        help="Path to input CSV file (default: use sample data)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(PROCESSED_DATA_DIR),
        help=f"Output directory for cleaned data (default: {PROCESSED_DATA_DIR})"
    )
    parser.add_argument(
        "--generate-sample",
        action="store_true",
        help="Generate sample messy data for demonstration"
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=1000,
        help="Number of rows to generate in sample data (default: 1000)"
    )
    parser.add_argument(
        "--no-duplicates",
        action="store_true",
        help="Skip duplicate removal"
    )
    parser.add_argument(
        "--no-standardize",
        action="store_true",
        help="Skip column name standardization"
    )
    parser.add_argument(
        "--remove-outliers",
        action="store_true",
        help="Enable outlier removal"
    )
    parser.add_argument(
        "--export-format",
        choices=["csv", "json"],
        default="csv",
        help="Export format (default: csv)"
    )
    
    args = parser.parse_args()
    
    # Generate sample data if requested
    if args.generate_sample:
        logger.info("Generating sample messy data...")
        sample_file = SAMPLE_DATA_DIR / "sample_messy_data.csv"
        generate_sample_data(
            num_rows=args.rows,
            output_path=sample_file,
            messy=True
        )
        print(f"\n✓ Sample data generated: {sample_file}")
        print(f"  Rows: {args.rows}")
        print(f"  Data issues: duplicates, missing values, formatting problems, outliers")
        return
    
    # Determine input file
    if args.input:
        input_path = Path(args.input)
    else:
        # Use sample data
        sample_file = SAMPLE_DATA_DIR / "sample_messy_data.csv"
        if not sample_file.exists():
            logger.info("Sample data not found, generating it...")
            generate_sample_data(
                num_rows=1000,
                output_path=sample_file,
                messy=True
            )
        input_path = sample_file
    
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        return 1
    
    # Set up pipeline
    output_dir = Path(args.output_dir)
    output_filename = f"cleaned_data.{args.export_format}"
    
    print(f"\n{'='*60}")
    print(f"Data Cleaning Pipeline")
    print(f"{'='*60}")
    print(f"Input:  {input_path}")
    print(f"Output: {output_dir / output_filename}")
    print(f"{'='*60}\n")
    
    # Run pipeline
    pipeline = DataCleaningPipeline(
        input_path=input_path,
        output_dir=output_dir,
        output_filename=output_filename
    )
    
    try:
        cleaned_data = pipeline.run(
            remove_duplicates_flag=not args.no_duplicates,
            missing_value_strategy="auto",
            standardize_columns=not args.no_standardize,
            date_columns=["signup_date"] if not args.no_standardize else ["Signup Date"],
            remove_outliers_flag=args.remove_outliers,
            export_format=args.export_format
        )
        
        # Print summary
        quality_report = pipeline.get_quality_report()
        if quality_report:
            print(f"\n{'='*60}")
            print(f"Data Quality Summary")
            print(f"{'='*60}")
            print(f"Total Rows:          {quality_report['total_rows']}")
            print(f"Total Columns:       {quality_report['total_columns']}")
            print(f"Duplicate Rows:      {quality_report['duplicate_rows']}")
            print(f"Missing Values:      {quality_report['total_missing_values']} "
                  f"({quality_report['missing_percentage']:.2f}%)")
            print(f"{'='*60}\n")
        
        print(f"✓ Pipeline completed successfully!")
        print(f"✓ Cleaned data saved to: {output_dir / output_filename}")
        print(f"✓ Quality report saved to: {output_dir / 'quality_report.json'}")
        
        return 0
        
    except Exception as e:
        print(f"\n✗ Pipeline failed: {str(e)}")
        logger.exception("Pipeline failed with exception")
        return 1


if __name__ == "__main__":
    exit(main())
