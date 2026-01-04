"""Generate sample messy data for testing the pipeline."""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
import random

from .logger import setup_logger

logger = setup_logger(__name__)


def generate_sample_data(
    num_rows: int = 1000,
    output_path: Optional[Path] = None,
    messy: bool = True
) -> pd.DataFrame:
    """
    Generate sample customer data with various data quality issues.
    
    Args:
        num_rows: Number of rows to generate
        output_path: Path to save the CSV (optional)
        messy: Whether to introduce data quality issues
        
    Returns:
        DataFrame with sample data
    """
    logger.info(f"Generating sample data with {num_rows} rows")
    
    # Set random seed for reproducibility
    np.random.seed(42)
    random.seed(42)
    
    # Generate base data
    data = {
        "Customer ID": range(1, num_rows + 1),
        "First Name": [random.choice([
            "John", "Jane", "Bob", "Alice", "Charlie", "Diana",
            "Edward", "Fiona", "George", "Hannah", "Ian", "Julia"
        ]) for _ in range(num_rows)],
        "Last Name": [random.choice([
            "Smith", "Johnson", "Williams", "Brown", "Jones",
            "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"
        ]) for _ in range(num_rows)],
        "Email": [],
        "Age": np.random.randint(18, 80, num_rows),
        "Signup Date": [],
        "Purchase Amount": np.random.uniform(10, 1000, num_rows).round(2),
        "City": [random.choice([
            "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
            "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"
        ]) for _ in range(num_rows)],
        "Status": [random.choice([
            "active", "inactive", "pending", "suspended"
        ]) for _ in range(num_rows)],
    }
    
    # Generate emails
    for i in range(num_rows):
        first = data["First Name"][i].lower()
        last = data["Last Name"][i].lower()
        domain = random.choice(["gmail.com", "yahoo.com", "hotmail.com", "company.com"])
        data["Email"].append(f"{first}.{last}{i}@{domain}")
    
    # Generate dates
    start_date = datetime(2020, 1, 1)
    for i in range(num_rows):
        days_offset = random.randint(0, 1460)  # Up to 4 years
        date = start_date + timedelta(days=days_offset)
        # Mix different date formats if messy
        if messy and random.random() < 0.3:
            data["Signup Date"].append(date.strftime("%d/%m/%Y"))
        elif messy and random.random() < 0.3:
            data["Signup Date"].append(date.strftime("%m-%d-%Y"))
        else:
            data["Signup Date"].append(date.strftime("%Y-%m-%d"))
    
    df = pd.DataFrame(data)
    
    if messy:
        logger.info("Introducing data quality issues...")
        
        # Add duplicates (5%)
        num_duplicates = int(num_rows * 0.05)
        duplicate_indices = np.random.choice(df.index, num_duplicates, replace=True)
        duplicates = df.loc[duplicate_indices].copy()
        df = pd.concat([df, duplicates], ignore_index=True)
        logger.info(f"Added {num_duplicates} duplicate rows")
        
        # Add missing values (10% across various columns)
        missing_indices = np.random.choice(len(df), int(len(df) * 0.1), replace=False)
        for idx in missing_indices:
            col = random.choice(["Email", "Age", "City", "Purchase Amount"])
            df.loc[idx, col] = np.nan
        logger.info("Added missing values")
        
        # Add whitespace issues
        text_columns = ["First Name", "Last Name", "Email", "City", "Status"]
        for col in text_columns:
            indices = np.random.choice(len(df), int(len(df) * 0.15), replace=False)
            for idx in indices:
                if pd.notna(df.loc[idx, col]):
                    df.loc[idx, col] = "  " + str(df.loc[idx, col]) + "  "
        logger.info("Added whitespace issues")
        
        # Add case inconsistencies
        indices = np.random.choice(len(df), int(len(df) * 0.1), replace=False)
        for idx in indices:
            if pd.notna(df.loc[idx, "Status"]):
                df.loc[idx, "Status"] = df.loc[idx, "Status"].upper()
        logger.info("Added case inconsistencies")
        
        # Add some outliers
        outlier_indices = np.random.choice(len(df), 10, replace=False)
        for idx in outlier_indices:
            df.loc[idx, "Age"] = random.choice([5, 150, -10, 200])
            df.loc[idx, "Purchase Amount"] = random.choice([0.01, 50000, -100])
        logger.info("Added outliers")
        
        # Mix column name formatting
        df.columns = [
            "Customer ID",
            " First Name ",
            "Last_Name",
            "Email Address",
            "AGE",
            "Signup Date",
            "Purchase-Amount",
            "City",
            "Status"
        ]
        logger.info("Added column name formatting issues")
    
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        logger.info(f"Saved sample data to {output_path}")
    
    logger.info(f"Generated dataframe with {len(df)} rows and {len(df.columns)} columns")
    return df


if __name__ == "__main__":
    from .config import SAMPLE_DATA_DIR
    
    # Generate sample messy data
    sample_file = SAMPLE_DATA_DIR / "sample_messy_data.csv"
    df = generate_sample_data(num_rows=1000, output_path=sample_file, messy=True)
    
    print(f"\nSample data generated: {sample_file}")
    print(f"Shape: {df.shape}")
    print(f"\nFirst few rows:")
    print(df.head())
    print(f"\nData info:")
    print(df.info())
