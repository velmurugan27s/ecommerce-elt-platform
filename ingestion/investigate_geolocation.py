import pandas as pd
from pathlib import Path

DATA_DIR = Path("data/raw")

df = pd.read_csv(
    DATA_DIR / "olist_geolocation_dataset.csv",
    dtype={
        "geolocation_zip_code_prefix": "string",
        "geolocation_city": "string",
        "geolocation_state": "string"
    }
)

print("=" * 70)
print("GEOLOCATION DATA QUALITY INVESTIGATION")
print("=" * 70)

# 1. Basic statistics

print("\n--- BASIC STATISTICS ---")

print("Total records:", len(df))
print(
    "Distinct postal prefixes:",
    df["geolocation_zip_code_prefix"].nunique()
)

print("Exact duplicate rows:", df.duplicated().sum())

# 2. Check null values

print("\n--- NULL VALUE CHECK ---")

print(df.isna().sum().to_string())

# 3. Analyze records per postal prefix

prefix_summary = (
    df.groupby("geolocation_zip_code_prefix")
    .agg(
        record_count=("geolocation_zip_code_prefix", "size"),
        distinct_coordinates=(
            "geolocation_lat",
            "nunique"
        ),
        distinct_longitudes=(
            "geolocation_lng",
            "nunique"
        ),
        distinct_cities=(
            "geolocation_city",
            "nunique"
        ),
        distinct_states=(
            "geolocation_state",
            "nunique"
        )
    )
    .reset_index()
)

print("\n--- POSTAL PREFIX ANALYSIS ---")

print("Unique postal prefixes:", len(prefix_summary))

print(
    "Prefixes with multiple records:",
    (prefix_summary["record_count"] > 1).sum()
)

print(
    "Prefixes with multiple coordinates:",
    (prefix_summary["distinct_coordinates"] > 1).sum()
)

print(
    "Prefixes associated with multiple cities:",
    (prefix_summary["distinct_cities"] > 1).sum()
)

print(
    "Prefixes associated with multiple states:",
    (prefix_summary["distinct_states"] > 1).sum()
)

# 4. Distribution of records per prefix

print("\n--- RECORD COUNT DISTRIBUTION ---")

print(
    prefix_summary["record_count"]
    .describe()
    .round(2)
    .to_string()
)

# 5. Examine postal prefixes with many records

print("\n--- POSTAL PREFIXES WITH MOST RECORDS ---")

print(
    prefix_summary
    .sort_values("record_count", ascending=False)
    .head(15)
    .to_string(index=False)
)

# 6. Examine prefixes associated with multiple cities

print("\n--- PREFIXES WITH MULTIPLE CITIES OR STATES ---")

inconsistent = prefix_summary[
    (prefix_summary["distinct_cities"] > 1)
    | (prefix_summary["distinct_states"] > 1)
]

print(inconsistent.head(20).to_string(index=False))

# 7. Coordinate range checks

print("\n--- COORDINATE VALIDATION ---")

invalid_latitude = (
    (df["geolocation_lat"] < -90)
    | (df["geolocation_lat"] > 90)
).sum()

invalid_longitude = (
    (df["geolocation_lng"] < -180)
    | (df["geolocation_lng"] > 180)
).sum()

print("Invalid latitude records:", invalid_latitude)
print("Invalid longitude records:", invalid_longitude)

print("\nGeolocation investigation completed.")

# Count unique latitude-longitude pairs per postal prefix

coordinate_pairs = (
    df.drop_duplicates(
        subset=[
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng"
        ]
    )
    .groupby("geolocation_zip_code_prefix")
    .size()
    .rename("distinct_coordinate_pairs")
)

prefix_summary = prefix_summary.merge(
    coordinate_pairs,
    on="geolocation_zip_code_prefix",
    how="left"
)

print(
    "Prefixes with multiple coordinate pairs:",
    (prefix_summary["distinct_coordinate_pairs"] > 1).sum()
)
