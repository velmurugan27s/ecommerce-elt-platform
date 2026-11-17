import pandas as pd
from pathlib import Path

DATA_DIR = Path("data/raw")

orders = pd.read_csv(
    DATA_DIR / "olist_orders_dataset.csv",
    usecols=["order_id", "order_status"]
)

items = pd.read_csv(
    DATA_DIR / "olist_order_items_dataset.csv",
    usecols=["order_id", "price", "freight_value"]
)

payments = pd.read_csv(
    DATA_DIR / "olist_order_payments_dataset.csv",
    usecols=["order_id", "payment_type", "payment_value"]
)

print("=" * 70)
print("FINANCIAL RECONCILIATION ANALYSIS")
print("=" * 70)

# --------------------------------------------------
# 1. Aggregate item values independently
# --------------------------------------------------

item_summary = (
    items.groupby("order_id")
    .agg(
        item_total=("price", "sum"),
        freight_total=("freight_value", "sum")
    )
    .reset_index()
)

item_summary["expected_order_amount"] = (
    item_summary["item_total"]
    + item_summary["freight_total"]
)

# --------------------------------------------------
# 2. Aggregate payments independently
# --------------------------------------------------

payment_summary = (
    payments.groupby("order_id")
    .agg(
        payment_total=("payment_value", "sum"),
        payment_count=("payment_value", "count")
    )
    .reset_index()
)

# --------------------------------------------------
# 3. Combine at order grain
# --------------------------------------------------

reconciliation = (
    orders
    .merge(item_summary, on="order_id", how="left")
    .merge(payment_summary, on="order_id", how="left")
)

reconciliation["difference"] = (
    reconciliation["payment_total"]
    - reconciliation["expected_order_amount"]
)

print("\n--- MISSING CHILD RECORDS BY STATUS ---")

print(
    reconciliation.groupby("order_status").agg(
        orders=("order_id", "count"),
        missing_items=("item_total", lambda x: x.isna().sum()),
        missing_payments=("payment_total", lambda x: x.isna().sum())
    ).to_string()
)

# --------------------------------------------------
# 4. Reconciliation summary
# --------------------------------------------------

comparable = reconciliation.dropna(
    subset=["expected_order_amount", "payment_total"]
).copy()

comparable["difference"] = comparable["difference"].round(2)

comparable["reconciliation_status"] = (
    comparable["difference"].abs() <= 0.01
).map({
    True: "Matched",
    False: "Mismatch"
})

print("\n--- FINANCIAL RECONCILIATION SUMMARY ---")

print(
    comparable["reconciliation_status"]
    .value_counts()
    .to_string()
)

print("\n--- RECONCILIATION BY ORDER STATUS ---")

print(
    comparable.groupby("order_status").agg(
        orders=("order_id", "count"),
        matched=("reconciliation_status",
                 lambda x: (x == "Matched").sum()),
        mismatched=("reconciliation_status",
                    lambda x: (x == "Mismatch").sum())
    ).to_string()
)

# --------------------------------------------------
# 5. Inspect mismatched orders
# --------------------------------------------------

mismatches = comparable[
    comparable["reconciliation_status"] == "Mismatch"
].copy()

print("\n--- SAMPLE FINANCIAL MISMATCHES ---")

print(
    mismatches[
        [
            "order_id",
            "order_status",
            "item_total",
            "freight_total",
            "expected_order_amount",
            "payment_total",
            "difference"
        ]
    ]
    .head(20)
    .round(2)
    .to_string(index=False)
)

# --------------------------------------------------
# 6. Payment method distribution
# --------------------------------------------------

print("\n--- PAYMENT METHOD DISTRIBUTION ---")

print(
    payments.groupby("payment_type").agg(
        payment_records=("payment_type", "count"),
        total_value=("payment_value", "sum")
    )
    .sort_values("payment_records", ascending=False)
    .round(2)
    .to_string()
)

print("\nFinancial reconciliation completed.")
