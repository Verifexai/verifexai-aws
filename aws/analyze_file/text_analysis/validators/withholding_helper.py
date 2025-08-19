import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

def _norm_number(x: Any) -> Optional[str]:
    if x is None:
        return None
    digits = "".join(ch for ch in str(x) if ch.isdigit())
    # Israeli תיק ניכויים are 9 digits in these lists; ignore anything else
    return digits if len(digits) == 9 else None

@lru_cache(maxsize=1)
def _load_known_deduction_numbers() -> tuple[set, dict]:
    """
    Returns:
        (known_numbers_set, number_to_company)
    Loads once per runtime (warm starts reuse it in Lambda).
    """
    # choose path
    path = str(Path(__file__).resolve().parent/"tik_nikuyim_israel.csv")

    known: set[str] = set()
    number_to_company: dict[str, str] = {}

    if not path or not os.path.exists(path):
        print("path not exist:",path)
        # Nothing to load; return empty structures (check will mark as unknown)
        return known, number_to_company

    # Load CSV or JSON
    if path.lower().endswith(".json"):
        import json
        with open(path, encoding="utf-8") as f:
            rows = json.load(f)
        # infer columns
        for row in rows:
            num = _norm_number(row.get("withholding_file_number") or row.get("number"))
            if not num:
                continue
            known.add(num)
            # Try to capture a display name for evidence
            company = row.get("name") or row.get("company") or row.get("brand")
            if company:
                number_to_company[num] = str(company)
    else:
        import csv
        with open(path, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            # detect number column
            cols = [c.strip() for c in (rdr.fieldnames or [])]
            num_col = "withholding_file_number" if "withholding_file_number" in cols else (
                "number" if "number" in cols else None
            )
            name_col = None
            # prefer a human label for evidence
            for c in ("company", "name", "brand", "product", "sub_product"):
                if c in cols:
                    name_col = c
                    break
            for row in rdr:
                num = _norm_number(row.get(num_col)) if num_col else None
                if not num:
                    continue
                known.add(num)
                label = row.get(name_col) if name_col else None
                if label:
                    number_to_company[num] = str(label)

    return known, number_to_company