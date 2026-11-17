# Olist Source Data Assessment

## Dataset Overview

- Source: Olist Brazilian E-Commerce Dataset
- Files: 9 CSV files
- Approximate records: 1.55 million
- Raw storage format: CSV

## Initial Data Quality Findings

1. Geolocation contains 261,831 exact duplicate rows.
2. Reviews contain missing comment fields.
3. Orders contain missing delivery timestamps.
4. Products contain missing category and dimension attributes.
5. All datasets require primary-key and foreign-key validation.

## Next Steps

- Validate primary keys.
- Validate foreign-key relationships.
- Investigate orphan records.
- Define Bronze, Silver and Gold transformation rules.

## Order Lifecycle Findings

- 96,478 orders have delivered status.
- 8 delivered orders have missing customer-delivery timestamps.
- 1,359 orders have carrier timestamps before approval timestamps.
- 23 orders have customer-delivery timestamps before carrier handoff.
- Maximum observed purchase-to-delivery duration is approximately 209.63 days.
- Delivery performance metrics require filtering by delivered status.
- Timestamp anomalies must be retained and flagged, not silently corrected.

## Financial Reconciliation Findings

- 98,362 orders matched between item-plus-freight
  amounts and payment totals.
- 303 orders had financial mismatches.
- 264 mismatches had payment totals higher than
  item-plus-freight totals.
- 39 mismatches had payment totals lower than
  item-plus-freight totals.
- Maximum absolute difference: 182.81.
- Most mismatches belonged to delivered orders.
- Credit card was the dominant payment method
  among mismatched orders.

### Engineering Decisions

- Aggregate item and payment datasets independently.
- Never join raw item and payment records before
  aggregation when calculating order-level totals.
- Preserve original financial amounts.
- Flag mismatches rather than silently correcting them.
- Use decimal data types for financial calculations
  in the warehouse.