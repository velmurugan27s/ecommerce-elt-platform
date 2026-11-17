import pandas as pd
from pathlib import Path

DATA_DIR = Path("data/raw")

df = pd.read_csv(
    DATA_DIR / "olist_orders_dataset.csv"
)

date_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for column in date_columns:
    df[column] = pd.to_datetime(
        df[column],
        errors="coerce"
    )

print("=" * 70)
print("ORDER ANOMALY INVESTIGATION")
print("=" * 70)

# 1. Reconcile delivered-order counts

print("\n--- DELIVERED ORDER RECONCILIATION ---")

delivered = df[df["order_status"] == "delivered"]

print("Delivered status rows:", len(delivered))

print(
    "Delivered rows missing delivery timestamp:",
    delivered["order_delivered_customer_date"].isna().sum()
)

print(
    "Delivered rows with delivery timestamp:",
    delivered["order_delivered_customer_date"].notna().sum()
)

# 2. Display delivered orders with missing timestamps

print("\n--- DELIVERED ORDERS WITH MISSING DELIVERY TIMESTAMP ---")

missing_delivery = delivered[
    delivered["order_delivered_customer_date"].isna()
]

print(
    missing_delivery[
        ["order_id"] + date_columns
    ].to_string(index=False)
)

# 3. Carrier handoff before approval

print("\n--- CARRIER BEFORE APPROVAL ---")

carrier_before_approval = df[
    df["order_delivered_carrier_date"].notna()
    & df["order_approved_at"].notna()
    & (
        df["order_delivered_carrier_date"]
        < df["order_approved_at"]
    )
].copy()

print("Violation count:", len(carrier_before_approval))

print(
    carrier_before_approval[
        ["order_id", "order_status",
         "order_approved_at",
         "order_delivered_carrier_date"]
    ].head(20).to_string(index=False)
)

# 4. Customer delivery before carrier handoff

print("\n--- CUSTOMER DELIVERY BEFORE CARRIER ---")

delivery_before_carrier = df[
    df["order_delivered_customer_date"].notna()
    & df["order_delivered_carrier_date"].notna()
    & (
        df["order_delivered_customer_date"]
        < df["order_delivered_carrier_date"]
    )
].copy()

print("Violation count:", len(delivery_before_carrier))

print(
    delivery_before_carrier[
        ["order_id", "order_status",
         "order_delivered_carrier_date",
         "order_delivered_customer_date"]
    ].to_string(index=False)
)

# 5. Investigate unusually long deliveries

print("\n--- 20 LONGEST DELIVERY DURATIONS ---")

completed = df[
    df["order_purchase_timestamp"].notna()
    & df["order_delivered_customer_date"].notna()
].copy()

completed["delivery_days"] = (
    completed["order_delivered_customer_date"]
    - completed["order_purchase_timestamp"]
).dt.total_seconds() / 86400

print(
    completed[
        ["order_id", "order_status", "delivery_days"]
    ]
    .sort_values("delivery_days", ascending=False)
    .head(20)
    .to_string(index=False)
)

print("\nOrder anomaly investigation completed.")
