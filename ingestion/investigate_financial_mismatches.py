import pandas as pd
from pathlib import Path

DATA_DIR = Path("data/raw")

orders = pd.read_csv(
    DATA_DIR / "olist_orders_dataset.csv",
    usecols=["order_id", "order_status"]
)

items = pd.read_csv(
    DATA_DIR / "olist_order_items_dataset.csv",
    usecols=[
        "order_id",
        "order_item_id",
        "price",
        "freight_value"
    ]
)

payments = pd.read_csv(
    DATA_DIR / "olist_order_payments_dataset.csv",
    usecols=[
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_value"
    ]
)

print("=" * 70)
print("FINANCIAL MISMATCH INVESTIGATION")
print("=" * 70)

# 1. Aggregate item amounts independently

item_summary = (
    items.groupby("order_id")
    .agg(
        item_total=("price", "sum"),
        freight_total=("freight_value", "sum")
    )
    .reset_index()
)

item_summary["expected_amount"] = (
    item_summary["item_total"]
    + item_summary["freight_total"]
)

# 2. Aggregate payment amounts independently

payment_summary = (
    payments.groupby("order_id")
    .agg(
        payment_total=("payment_value", "sum"),
        payment_count=("payment_sequential", "count")
    )
    .reset_index()
)

# 3. Reconcile at order grain

reconciliation = (
    orders
    .merge(item_summary, on="order_id", how="inner")
    .merge(payment_summary, on="order_id", how="inner")
)

reconciliation["difference"] = (
    reconciliation["payment_total"]
    - reconciliation["expected_amount"]
).round(2)

mismatches = reconciliation[
    reconciliation["difference"].abs() > 0.01
].copy()

print("\n--- MISMATCH SUMMARY ---")

print("Total mismatched orders:", len(mismatches))

print("\nMismatch by status:")

print(
    mismatches["order_status"]
    .value_counts()
    .to_string()
)

# 4. Classify the size and direction of differences

mismatches["difference_direction"] = (
    mismatches["difference"] > 0
).map({
    True: "Payment higher",
    False: "Payment lower"
})

print("\n--- DIFFERENCE DIRECTION ---")

print(
    mismatches["difference_direction"]
    .value_counts()
    .to_string()
)

print("\n--- DIFFERENCE DISTRIBUTION ---")

print(
    mismatches["difference"].abs()
    .describe()
    .round(2)
    .to_string()
)

# 5. Payment methods used in mismatched orders

mismatch_ids = mismatches["order_id"]

mismatch_payments = payments[
    payments["order_id"].isin(mismatch_ids)
]

print("\n--- PAYMENT METHODS IN MISMATCHED ORDERS ---")

print(
    mismatch_payments.groupby("payment_type")
    .agg(
        payment_records=("payment_type", "count"),
        orders=("order_id", "nunique"),
        total_value=("payment_value", "sum")
    )
    .round(2)
    .to_string()
)

# 6. Inspect first 10 mismatched orders

print("\n--- SAMPLE MISMATCHED ORDERS ---")

print(
    mismatches[
        [
            "order_id",
            "order_status",
            "item_total",
            "freight_total",
            "expected_amount",
            "payment_total",
            "payment_count",
            "difference"
        ]
    ]
    .head(10)
    .round(2)
    .to_string(index=False)
)

# 7. Inspect source payment records for those orders

print("\n--- SOURCE PAYMENT RECORDS FOR SAMPLE ORDERS ---")

sample_ids = mismatches["order_id"].head(5)

print(
    payments[
        payments["order_id"].isin(sample_ids)
    ]
    .sort_values(["order_id", "payment_sequential"])
    .to_string(index=False)
)

# 8. Inspect source item records for those orders

print("\n--- SOURCE ITEM RECORDS FOR SAMPLE ORDERS ---")

print(
    items[
        items["order_id"].isin(sample_ids)
    ]
    .sort_values(["order_id", "order_item_id"])
    .to_string(index=False)
)

print("\nFinancial mismatch investigation completed.")
