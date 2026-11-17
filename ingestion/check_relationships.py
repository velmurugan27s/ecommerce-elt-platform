
import pandas as pd
from pathlib import Path

DATA_DIR = Path("data/raw")


def load_csv(filename, columns):
    path = DATA_DIR / filename
    return pd.read_csv(path, usecols=columns)


def check_unique(df, columns, table_name):
    duplicates = df.duplicated(subset=columns).sum()

    print(f"\n{table_name}")
    print(f"Key columns: {columns}")
    print(f"Duplicate keys: {duplicates:,}")

    if duplicates == 0:
        print("Result: UNIQUE")
    else:
        print("Result: NOT UNIQUE")


def check_foreign_key(
    child_df,
    child_column,
    parent_df,
    parent_column,
    relationship
):
    child_keys = set(child_df[child_column].dropna())
    parent_keys = set(parent_df[parent_column].dropna())

    orphan_keys = child_keys - parent_keys

    print(f"\n{relationship}")
    print(f"Distinct child keys: {len(child_keys):,}")
    print(f"Orphan keys: {len(orphan_keys):,}")


print("=" * 65)
print("OLIST RELATIONAL INTEGRITY CHECK")
print("=" * 65)

# Load only the columns needed for relationship checks

customers = load_csv(
    "olist_customers_dataset.csv",
    ["customer_id", "customer_unique_id"]
)

orders = load_csv(
    "olist_orders_dataset.csv",
    ["order_id", "customer_id"]
)

items = load_csv(
    "olist_order_items_dataset.csv",
    ["order_id", "order_item_id", "product_id", "seller_id"]
)

payments = load_csv(
    "olist_order_payments_dataset.csv",
    ["order_id", "payment_sequential"]
)

reviews = load_csv(
    "olist_order_reviews_dataset.csv",
    ["review_id", "order_id"]
)

products = load_csv(
    "olist_products_dataset.csv",
    ["product_id"]
)

sellers = load_csv(
    "olist_sellers_dataset.csv",
    ["seller_id"]
)

# 1. Candidate primary-key checks

print("\n--- PRIMARY KEY CHECKS ---")

check_unique(customers, ["customer_id"], "CUSTOMERS")
check_unique(orders, ["order_id"], "ORDERS")
check_unique(items, ["order_id", "order_item_id"], "ORDER ITEMS")
check_unique(payments, ["order_id", "payment_sequential"], "PAYMENTS")
check_unique(reviews, ["review_id"], "REVIEWS")
check_unique(products, ["product_id"], "PRODUCTS")
check_unique(sellers, ["seller_id"], "SELLERS")

# 2. Foreign-key checks

print("\n--- FOREIGN KEY CHECKS ---")

check_foreign_key(
    orders, "customer_id",
    customers, "customer_id",
    "ORDERS -> CUSTOMERS"
)

check_foreign_key(
    items, "order_id",
    orders, "order_id",
    "ORDER ITEMS -> ORDERS"
)

check_foreign_key(
    items, "product_id",
    products, "product_id",
    "ORDER ITEMS -> PRODUCTS"
)

check_foreign_key(
    items, "seller_id",
    sellers, "seller_id",
    "ORDER ITEMS -> SELLERS"
)

check_foreign_key(
    payments, "order_id",
    orders, "order_id",
    "PAYMENTS -> ORDERS"
)

check_foreign_key(
    reviews, "order_id",
    orders, "order_id",
    "REVIEWS -> ORDERS"
)

# 3. Customer identity analysis

print("\n--- CUSTOMER IDENTITY ANALYSIS ---")

print(
    "Customer records:",
    len(customers)
)

print(
    "Unique customer identities:",
    customers["customer_unique_id"].nunique()
)

print(
    "Repeated customer identities:",
    customers["customer_unique_id"].duplicated().sum()
)

print("\nRelational integrity analysis completed.")
