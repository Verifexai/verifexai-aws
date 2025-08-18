# aws/common/utilities/summary_builder.py
from __future__ import annotations
from typing import List

from aws.common.config.client_config import ClientConfig
from aws.common.models.check_result import CheckResult


def _label(v) -> str:
    """Return enum value or string label."""
    return getattr(v, "value", v)

def build_manual_summary_text(
    checks: List[CheckResult],
    cfg: ClientConfig,
    *,
    overall_score: int,
    threshold: int = 50,
    max_items: int = 4
) -> str:
    """
    Manual, low-latency summary:
      'Severity: <sev>. High-scoring issues: category=..., kind=..., score=...; ...'
    Only includes checks with score >= threshold.
    """
    severity = _label(cfg.severity_for(overall_score))  # enum -> "low|medium|high|critical"
    # keep only high-scoring issues
    high = [c for c in checks if isinstance(c.score, int) and c.score >= threshold]
    if not high:
        return f"Severity: {severity}({overall_score}). No high-scoring issues (≥{threshold})."

    # sort by score desc
    high.sort(key=lambda c: c.score, reverse=True)

    items = []
    for c in high[:max_items]:
        cat = _label(c.category)  # enum -> string
        kind = _label(c.kind)
        items.append(f"category={cat}, kind={kind}, score={c.score}")

    extra = len(high) - len(items)
    tail = f"; +{extra} more" if extra > 0 else ""

    return f"Severity: {severity}({overall_score}).\nIssues: " + "\n".join(items) + tail + "."
