import pandas as pd
from pathlib import Path

DATA_DIR = Path("data/raw")

df = pd.read_csv(
    DATA_DIR / "olist_order_reviews_dataset.csv"
)

print("=" * 65)
print("REVIEW GRAIN ANALYSIS")
print("=" * 65)

# 1. Test review_id + order_id
duplicate_composite = df.duplicated(
    subset=["review_id", "order_id"]
).sum()

print("\n--- COMPOSITE KEY CHECK ---")
print("Total rows:", len(df))
print("Distinct review IDs:", df["review_id"].nunique())
print("Distinct order IDs:", df["order_id"].nunique())
print("Duplicate (review_id, order_id) combinations:",
      duplicate_composite)

# 2. Test order_id alone
duplicate_orders = df.duplicated(
    subset=["order_id"]
).sum()

print("\n--- ORDER ID CHECK ---")
print("Duplicate order_id rows:", duplicate_orders)

# 3. Check multiple reviews per order
reviews_per_order = (
    df.groupby("order_id")
    .size()
)

print("\n--- REVIEWS PER ORDER ---")
print("Orders with review records:", len(reviews_per_order))
print("Maximum reviews per order:", reviews_per_order.max())
print("Orders with multiple review records:",
      (reviews_per_order > 1).sum())

# 4. Check missing keys
print("\n--- NULL KEY CHECK ---")
print("Null review_id:", df["review_id"].isna().sum())
print("Null order_id:", df["order_id"].isna().sum())

print("\nReview grain analysis completed.")
