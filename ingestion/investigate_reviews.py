import pandas as pd
from pathlib import Path

DATA_DIR = Path("data/raw")

file_path = DATA_DIR / "olist_order_reviews_dataset.csv"

df = pd.read_csv(file_path)

print("=" * 65)
print("REVIEW DATA INVESTIGATION")
print("=" * 65)

# 1. Basic statistics
print("\n--- BASIC STATISTICS ---")

print("Total review rows:", len(df))
print("Distinct review IDs:", df["review_id"].nunique())
print("Duplicate review ID rows:", df.duplicated(
    subset=["review_id"]
).sum())

# 2. Find every row belonging to a repeated review ID
duplicate_reviews = df[
    df.duplicated(subset=["review_id"], keep=False)
].sort_values("review_id")

print("\n--- DUPLICATE REVIEW ID GROUPS ---")

print(
    "Rows belonging to repeated review IDs:",
    len(duplicate_reviews)
)

print(
    "Number of affected review IDs:",
    duplicate_reviews["review_id"].nunique()
)

# 3. Check whether the entire rows are identical
print("\n--- EXACT DUPLICATE ROWS ---")

print("Exact duplicate rows:", df.duplicated().sum())

# 4. Examine repeated IDs and their associated values
print("\n--- DUPLICATE ID DETAILS ---")

summary = (
    duplicate_reviews
    .groupby("review_id")
    .agg(
        row_count=("review_id", "size"),
        distinct_orders=("order_id", "nunique"),
        distinct_scores=("review_score", "nunique"),
        distinct_titles=("review_comment_title", "nunique"),
        distinct_messages=("review_comment_message", "nunique")
    )
    .reset_index()
)

print(summary.head(20).to_string(index=False))

# 5. Display actual records for the first repeated IDs
print("\n--- SAMPLE DUPLICATE RECORDS ---")

sample_ids = summary["review_id"].head(3)

sample = duplicate_reviews[
    duplicate_reviews["review_id"].isin(sample_ids)
]

print(sample.to_string(index=False))

print("\nReview investigation completed.")
