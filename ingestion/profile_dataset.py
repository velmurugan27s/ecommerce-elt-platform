
import pandas as pd
from pathlib import Path

DATA_DIR = Path("data/raw")

csv_files = sorted(DATA_DIR.glob("*.csv"))

if not csv_files:
    raise FileNotFoundError(
        f"No CSV files found in {DATA_DIR.resolve()}"
    )

print("=" * 70)
print("E-COMMERCE DATASET PROFILING")
print("=" * 70)

for file in csv_files:

    print(f"\nDATASET: {file.name}")
    print("-" * 70)

    df = pd.read_csv(file)

    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")

    print("\nColumn names:")
    print(df.columns.tolist())

    print("\nData types:")
    print(df.dtypes)

    print("\nMissing values:")
    print(df.isnull().sum())

    print("\nDuplicate rows:")
    print(df.duplicated().sum())

    print("\nSample records:")
    print(df.head(3).to_string(index=False))

print("\nDataset profiling completed.")
