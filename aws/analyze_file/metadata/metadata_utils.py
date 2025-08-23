from datetime import datetime, timezone
from typing import Optional, Dict, List, Any


def _name(val) -> str:
    s = str(val) if val is not None else ""
    return s[1:] if s.startswith("/") else s

def _parse_pdf_date(date_str: str) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        if date_str.startswith("D:"):
            date_str = date_str[2:]
        return datetime.strptime(date_str[:14], "%Y%m%d%H%M%S")
    except Exception:
        return None

def _decode_annot_flags(f: Optional[int]) -> Dict[str, bool]:
    f = int(f or 0)
    return {
        "Invisible": bool(f & 1),
        "Hidden": bool(f & 2),
        "Print": bool(f & 4),
        "NoView": bool(f & 32),
        "ReadOnly": bool(f & 64),
        "Locked": bool(f & 128),
        "ToggleNoView": bool(f & 256),
        "LockedContents": bool(f & 512),
    }

def _summarize_annotations(annots: List[Dict[str, Any]]) -> Dict[str, int]:
    by_type = {}
    for a in annots:
        t = _name(a.get("subtype")) or "Unknown"
        by_type[t] = by_type.get(t, 0) + 1
    return by_type

def _parse_invoice_date(dates: List[str]) -> Optional[datetime]:
    for d in dates:
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%Y%m%d"):
            try:
                return datetime.strptime(d, fmt)
            except ValueError:
                continue
    return None

def _rect_area(rect) -> Optional[float]:
    try:
        llx, lly, urx, ury = map(float, rect)
        return max(0.0, (urx - llx)) * max(0.0, (ury - lly))
    except Exception:
        return None


def _safe_bool(x):
    return None if x is None else bool(x)

def _parse_sign_dt(s):
    sign_dt = None
    raw = s.get("signing_time") or s.get("signed_at")
    if raw:
        try:
            sign_dt = datetime.fromisoformat(raw)
        except Exception:
            try:
                sign_dt = _parse_pdf_date(raw or "")
            except Exception:
                sign_dt = None
    return sign_dt

def to_aware_utc(dt):
    """Return a timezone-aware datetime in UTC (or None).
    If dt is naive, assume it is UTC."""
    if dt is None:
        return None
    if isinstance(dt, str):
        # If you have strings here, parse them before calling this function
        raise TypeError("Pass datetime objects, not strings")
    if dt.tzinfo is None:
        # Assumption: naive means UTC in your pipeline
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _fmt_dt(dt):
    try:
        return dt.isoformat()
    except Exception:
        return str(dt)

def _describe_reason(reason, *, sign_dt, creation_dt, modification_dt, invoice_dt, max_diff_days):
    if reason == "integrity_fail":
        return "cryptographic integrity check failed"
    if reason == "sign_before_creation":
        return f"signing time {_fmt_dt(sign_dt)} is before file creation {_fmt_dt(creation_dt)}"
    if reason == "sign_after_modification":
        return f"signing time {_fmt_dt(sign_dt)} is after last modification {_fmt_dt(modification_dt)}"
    if reason == "invoice_date_far":
        if sign_dt and invoice_dt:
            diff = abs((sign_dt - invoice_dt).days)
            return f"signing time {_fmt_dt(sign_dt)} is {diff} days from invoice date {_fmt_dt(invoice_dt)} (>{max_diff_days} allowed)"
        return "signing time is too far from invoice date"
    if reason == "untrusted_chain":
        return "certificate chain not trusted by system CA store"
    if reason == "indeterminate_chain":
        return "certificate trust indeterminate (partial chain / missing revocation info)"
    if reason == "not_cover_entire":
        return "signature does not cover the entire file"
    if reason == "docmdp_fail":
        return "modification restrictions violated (DocMDP policy)"
    if reason == "missing_sign_time":
        return "signing time missing or unparseable"
    # fallback
    return reason.replace("_", " ")

# Fixed severity order for description (independent of weights)
_REASON_PRIORITY = [
    "integrity_fail",
    "sign_before_creation", "sign_after_modification", "invoice_date_far",
    "untrusted_chain", "indeterminate_chain",
    "not_cover_entire", "docmdp_fail",
    "missing_sign_time",
]

def _sort_reasons(reasons):
    order = {r: i for i, r in enumerate(_REASON_PRIORITY)}
    return sorted(dict.fromkeys(reasons), key=lambda r: order.get(r, 999))

def _join_msgs(msgs):
    if not msgs:
        return ""
    if len(msgs) == 1:
        return msgs[0]
    if len(msgs) == 2:
        return f"{msgs[0]} and {msgs[1]}"
    return ", ".join(msgs[:-1]) + f", and {msgs[-1]}"
