# etl/extract/generator/gen_categories.py
# ============================================================
# Generates dim_category DataFrame.
# No database interaction — pure in-memory generation.
# Returns DataFrame with event_type and ingested_at columns.
# ============================================================

import pandas as pd
from datetime import datetime, timezone

from .config import CATEGORIES


def generate_categories():
    """
    Builds dim_category DataFrame from hardcoded CATEGORIES list.
    Returns DataFrame for Bronze layer writing.
    """

    print("\n[dim_category] Generating categories...")

    rows = []
    for cat_id, cat_name, parent_id in CATEGORIES:
        rows.append({
            "category_id":        cat_id,
            "category_name":      cat_name,
            "parent_category_id": parent_id,   # string reference, SK resolved in Silver
            "event_type":         "created",
            "ingested_at":        datetime.now(timezone.utc).isoformat(),
        })

    df = pd.DataFrame(rows)

    print(f"  Done. {len(df)} category rows generated.")
    return df