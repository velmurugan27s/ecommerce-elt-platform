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
        "product_id",
        "seller_id",
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
print("ORDER, ITEM AND PAYMENT CARDINALITY ANALYSIS")
print("=" * 70)

# --------------------------------------------------
# 1. BASIC COUNTS
# --------------------------------------------------

print("\n--- BASIC COUNTS ---")

print("Orders:", len(orders))
print("Order item records:", len(items))
print("Payment records:", len(payments))

# --------------------------------------------------
# 2. ITEMS PER ORDER
# --------------------------------------------------

items_per_order = (
    items.groupby("order_id")
    .size()
)

print("\n--- ITEMS PER ORDER ---")

print("Orders with item records:", len(items_per_order))
print("Maximum items in one order:", items_per_order.max())
print("Orders with multiple items:", (items_per_order > 1).sum())

print("\nItem count distribution:")

print(
    items_per_order.value_counts()
    .sort_index()
    .head(15)
    .to_string()
)

# --------------------------------------------------
# 3. PAYMENTS PER ORDER
# --------------------------------------------------

payments_per_order = (
    payments.groupby("order_id")
    .size()
)

print("\n--- PAYMENTS PER ORDER ---")

print("Orders with payment records:", len(payments_per_order))
print("Maximum payments in one order:", payments_per_order.max())
print("Orders with multiple payments:", (payments_per_order > 1).sum())

print("\nPayment count distribution:")

print(
    payments_per_order.value_counts()
    .sort_index()
    .to_string()
)

# --------------------------------------------------
# 4. ORDERS WITHOUT ITEMS OR PAYMENTS
# --------------------------------------------------

print("\n--- ORDERS WITHOUT CHILD RECORDS ---")

order_ids = set(orders["order_id"])

orders_with_items = set(items["order_id"])
orders_with_payments = set(payments["order_id"])

print(
    "Orders without item records:",
    len(order_ids - orders_with_items)
)

print(
    "Orders without payment records:",
    len(order_ids - orders_with_payments)
)

# --------------------------------------------------
# 5. ORDER-LEVEL AGGREGATION
# --------------------------------------------------

item_summary = (
    items.groupby("order_id")
    .agg(
        item_count=("order_item_id", "count"),
        item_total=("price", "sum"),
        freight_total=("freight_value", "sum")
    )
)

payment_summary = (
    payments.groupby("order_id")
    .agg(
        payment_count=("payment_sequential", "count"),
        payment_total=("payment_value", "sum")
    )
)

order_summary = (
    orders
    .merge(item_summary, on="order_id", how="left")
    .merge(payment_summary, on="order_id", how="left")
)

print("\n--- ORDER-LEVEL AGGREGATION ---")

print(
    order_summary[
        [
            "item_count",
            "item_total",
            "freight_total",
            "payment_count",
            "payment_total"
        ]
    ].describe().round(2).to_string()
)

# --------------------------------------------------
# 6. JOIN FANOUT DEMONSTRATION
# --------------------------------------------------

print("\n--- JOIN FANOUT DEMONSTRATION ---")

joined = items.merge(
    payments,
    on="order_id",
    how="inner"
)

print("Original item rows:", len(items))
print("Original payment rows:", len(payments))
print("Rows after joining items and payments:", len(joined))

# Orders having multiple item and payment records
item_counts = items.groupby("order_id").size()
payment_counts = payments.groupby("order_id").size()

common_orders = item_counts.index.intersection(
    payment_counts.index
)

multiple_both = [
    order_id
    for order_id in common_orders
    if item_counts[order_id] > 1
    and payment_counts[order_id] > 1
]

print(
    "Orders with multiple items AND multiple payments:",
    len(multiple_both)
)

if multiple_both:
    example_id = multiple_both[0]

    print("\nExample order:", example_id)
    print("Item records:", item_counts[example_id])
    print("Payment records:", payment_counts[example_id])
    print(
        "Rows produced by join:",
        item_counts[example_id] * payment_counts[example_id]
    )

print("\nCardinality analysis completed.")
