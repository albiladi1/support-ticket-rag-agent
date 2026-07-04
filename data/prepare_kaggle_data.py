
import json
import os
import uuid

import pandas as pd

RAW_CSV = os.path.join(os.path.dirname(__file__), "kaggle_raw.csv")
OUT_JSON = os.path.join(os.path.dirname(__file__), "sample_tickets.json")

# Map Kaggle's column names -> our schema's field names.
# These match the 200K "customer_support_tickets" dataset columns:
# ['ticket_id', 'customer_name', 'customer_email', 'product', 'category',
#  'issue_description', 'resolution_notes', 'priority', 'status', 'channel',
#  'region', ... 'ticket_created_date', 'ticket_resolved_date', ...]
COLUMN_MAP = {
    "customer_name": "customer",
    "issue_description": "body",
    "category": "category",              # normalized below
    "ticket_created_date": "created_at",
}

# Kaggle's free-text category values -> the 4 allowed values in our schema
# NOTE: run df['category'].unique() and adjust this if new values appear.
CATEGORY_NORMALIZATION = {
    "billing": "billing",
    "billing inquiry": "billing",
    "refund": "billing",
    "payment": "billing",
    "technical": "technical",
    "technical issue": "technical",
    "bug": "technical",
    "account": "account",
    "login": "account",
    "password": "account",
}


def normalize_category(raw_value: str) -> str:
    key = str(raw_value).strip().lower()
    return CATEGORY_NORMALIZATION.get(key, "other")


def to_iso_timestamp(raw_value) -> str:
    """Converts whatever date format the CSV has into an ISO-8601 string
    with a timezone, since our schema requires format: date-time."""
    ts = pd.to_datetime(raw_value, errors="coerce")
    if pd.isna(ts):
        ts = pd.Timestamp("2026-01-01", tz="UTC")
    elif ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.isoformat()


def main(max_rows: int = 2000):
    df = pd.read_csv(RAW_CSV)
    df = df.rename(columns=COLUMN_MAP)
    df = df.dropna(subset=["body"]).head(max_rows)

    records = []
    for _, row in df.iterrows():
        # This dataset has no separate "subject" field, so we derive a
        # short one from the product + category for readability.
        product = str(row.get("product", "")).strip()
        category_raw = str(row.get("category", "other"))
        subject = f"{product} - {category_raw}".strip(" -") or "Support Ticket"

        # Prefer the dataset's own ticket_id if present and non-null;
        # otherwise generate one so schema validation never fails.
        raw_id = row.get("ticket_id")
        ticket_id = f"T-{raw_id}" if pd.notna(raw_id) else f"T-{uuid.uuid4().hex[:8].upper()}"

        records.append({
            "ticket_id": ticket_id,
            "customer": str(row.get("customer", "Unknown")).strip(),
            "subject": subject,
            "body": str(row["body"]).strip(),
            "category": normalize_category(category_raw),
            "created_at": to_iso_timestamp(row.get("created_at")),
        })

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"[prepare_kaggle_data] wrote {len(records)} tickets -> {OUT_JSON}")


if __name__ == "__main__":
    main()
