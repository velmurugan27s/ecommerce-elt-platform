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

# Convert timestamp columns into datetime
for column in date_columns:
    df[column] = pd.to_datetime(
        df[column],
        errors="coerce"
    )

print("=" * 70)
print("OLIST ORDER LIFECYCLE ANALYSIS")
print("=" * 70)

# --------------------------------------------------
# 1. ORDER STATUS DISTRIBUTION
# --------------------------------------------------

print("\n--- ORDER STATUS DISTRIBUTION ---")

status_counts = (
    df["order_status"]
    .value_counts(dropna=False)
    .rename_axis("order_status")
    .reset_index(name="order_count")
)

status_counts["percentage"] = (
    status_counts["order_count"] / len(df) * 100
)

print(status_counts.to_string(index=False))

# --------------------------------------------------
# 2. MISSING TIMESTAMP ANALYSIS
# --------------------------------------------------

print("\n--- MISSING TIMESTAMPS BY ORDER STATUS ---")

missing_by_status = (
    df.groupby("order_status")[date_columns]
    .agg(lambda column: column.isna().sum())
)

print(missing_by_status.to_string())

# --------------------------------------------------
# 3. OVERALL TIMESTAMP COMPLETENESS
# --------------------------------------------------

print("\n--- TIMESTAMP COMPLETENESS ---")

for column in date_columns:
    missing = df[column].isna().sum()

    print(
        f"{column}: "
        f"{missing:,} missing "
        f"({missing / len(df) * 100:.2f}%)"
    )

# --------------------------------------------------
# 4. DELIVERY DURATION
# --------------------------------------------------

print("\n--- DELIVERY DURATION ---")

delivered = df[
    df["order_delivered_customer_date"].notna()
    & df["order_purchase_timestamp"].notna()
].copy()

delivered["delivery_days"] = (
    delivered["order_delivered_customer_date"]
    - delivered["order_purchase_timestamp"]
).dt.total_seconds() / 86400

print("Orders with delivery timestamps:", len(delivered))

print(
    delivered["delivery_days"]
    .describe()
    .round(2)
    .to_string()
)

# --------------------------------------------------
# 5. ON-TIME VS LATE DELIVERY
# --------------------------------------------------

print("\n--- DELIVERY PERFORMANCE ---")

delivery_comparable = delivered[
    delivered["order_estimated_delivery_date"].notna()
].copy()

delivery_comparable["delivery_result"] = (
    delivery_comparable["order_delivered_customer_date"]
    > delivery_comparable["order_estimated_delivery_date"]
).map({
    True: "Late",
    False: "On time or early"
})

print(
    delivery_comparable["delivery_result"]
    .value_counts()
    .to_string()
)

# --------------------------------------------------
# 6. TIMESTAMP CHRONOLOGY CHECK
# --------------------------------------------------

print("\n--- TIMESTAMP CHRONOLOGY CHECK ---")

chronology_checks = [
    (
        "Approval before purchase",
        "order_approved_at",
        "order_purchase_timestamp"
    ),
    (
        "Carrier handoff before approval",
        "order_delivered_carrier_date",
        "order_approved_at"
    ),
    (
        "Customer delivery before carrier handoff",
        "order_delivered_customer_date",
        "order_delivered_carrier_date"
    )
]

for label, earlier, later in chronology_checks:

    comparable = df[
        df[earlier].notna() & df[later].notna()
    ]

    violations = (
        comparable[earlier] < comparable[later]
    ).sum()

    print(f"{label}: {violations:,} violations")

print("\nOrder lifecycle analysis completed.")
