from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Iterable, List, Tuple
import math
import uuid
from urllib.parse import unquote_plus
from pathlib import PurePosixPath
from typing import Optional
from aws.analyze_file.summary import build_manual_summary_text
from aws.common.config.config import client_config
from aws.common.config.version import PLATFORM_VERSION
from aws.common.models.check_result import CheckResult
from aws.common.models.document_info import DocumentInfo
from aws.common.models.fraud_report import FraudReport
from aws.common.models.overall import Overall
from aws.common.utilities.enums import Category
from aws.common.utilities.logger_manager import LoggerManager, ANALYZE_FILE


def _now_iso() -> datetime:
    return datetime.now(timezone.utc)

def _make_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"

def _create_fraud_report(checks: List[CheckResult], documentInfo: DocumentInfo) -> FraudReport:
    logger = LoggerManager.get_module_logger(ANALYZE_FILE)
    logger.info(f"Create Fraud report with {len(checks)} checks")
    K = choose_adaptive_k(checks, t_material=25, min_k=1, max_k=5)

    # pick which K to include if you want diversity
    idxs = pick_topk_indices_with_diversity(checks, K, diversity_first_k=3)
    scores_for_agg = [checks[i].score for i in idxs]

    # aggregate mean_top_k on the selected set
    overall_risk_score = aggregate_mean_top_k(scores_for_agg, K)
    logger.info(f"Overall Score: {overall_risk_score}, Calculated with {K} top k scores")
    summary_text = build_manual_summary_text(checks=checks, cfg=client_config, overall_score=overall_risk_score)
    # logger.info("summary_text",summary_text)

    overall = Overall(
        risk_score=overall_risk_score,
        severity=client_config.severity_for(overall_risk_score),
        summary_text= summary_text
    )

    return FraudReport(
        platform_version=PLATFORM_VERSION,
        run_id=_make_id("RUN"),
        document=documentInfo,
        overall=overall,
        checks=checks,
        meta={}
    )

def _get_score_from_checks(checks: List[CheckResult]) -> List[CheckResult]:
    out = []
    for c in checks:
        try:
            s = int(c.score)
            if 0 <= s <= 100 and math.isfinite(s):
                out.append(c)
        except Exception:
            continue
    return out

def aggregate_mean_top_k(scores: Iterable[float], mean_top_k: int) -> int:
    """
    Aggregate a list of 0..100 scores by taking the mean of the top-K values.

    Args:
        scores: Iterable of numeric scores (expected 0..100). Non-finite values are ignored.
        mean_top_k: K for "top-K". If K <= 0, returns 0. If K > len(valid_scores), uses all valid scores.

    Returns:
        Integer score in [0, 100].

    Notes:
        - Each score is clamped into [0, 100] before aggregation.
        - If there are no valid scores, returns 0.
    """
    # sanitize inputs
    clean: List[float] = []
    for s in scores:
        try:
            s = float(s)
            if math.isfinite(s):
                clean.append(max(0.0, min(100.0, s)))
        except (TypeError, ValueError):
            continue

    if not clean or mean_top_k <= 0:
        return 0

    clean.sort(reverse=True)
    k = min(mean_top_k, len(clean))
    topk = clean[:k]
    mean = sum(topk) / k
    return int(round(mean))


def top_k_indices(scores: Iterable[float], k: int) -> List[int]:
    """
    Helper: return original indices of the top-K scores (stable for ties by original order).
    """
    enumerated: List[Tuple[int, float]] = []
    for i, s in enumerate(scores):
        try:
            s = float(s)
            if math.isfinite(s):
                enumerated.append((i, max(0.0, min(100.0, s))))
        except (TypeError, ValueError):
            continue

    if not enumerated or k <= 0:
        return []

    # sort by score desc, then index asc for stability
    enumerated.sort(key=lambda pair: (-pair[1], pair[0]))
    return [i for i, _ in enumerated[: min(k, len(enumerated))]]

def choose_adaptive_k(
    checks: List[CheckResult],
    *,
    t_material: int = 25,          # score threshold for "material" checks
    min_k: int = 1,
    max_k: int = 5,
    enforce_diversity: bool = True,
    diversity_first_k: int = 3,    # try to ensure category diversity within the first K positions
    concentration_ratio: float = 0.55  # if top score / sum(top L) > this -> K=1 (concentrated risk)
) -> int:
    """
    Pick a sensible K for mean_top_k given a set of checks.

    Heuristics:
    - Count material checks (score >= t_material)
    - K = clamp(round(sqrt(M)), [min_k, max_k])
    - If risk is concentrated in the top issue -> K = 1
    - Optionally ensure category diversity among the first few picks (affects how you'll choose the top-K set)
    """
    # 1) material checks
    material = [c for c in checks if isinstance(c.score, int) and c.score >= t_material]
    M = len(material)

    if M == 0:
        return min_k  # nothing material; mean_top_k aggregator will yield ~0 anyway

    # 2) base K by sqrt(M) (rounded)
    K = max(min_k, min(max_k, int(round(math.sqrt(M)))))

    # 3) risk concentration test (on all checks, not only material)
    ordered = sorted(checks, key=lambda c: c.score, reverse=True)
    if ordered:
        top = ordered[0].score
        # compare top vs sum of top L (L = min(K*2, N)) to detect dominance
        L = min(max(2, K * 2), len(ordered))
        denom = sum(c.score for c in ordered[:L]) or 1
        if (top / denom) >= concentration_ratio:
            return 1  # one issue dominates → K=1

    # 4) (optional) if you plan to enforce diversity in selection,
    #    you typically don't change K here; you use diversity when choosing top-K indices.
    #    So we just return K.
    return K


def pick_topk_indices_with_diversity(
    checks: List[CheckResult],
    k: int,
    *,
    diversity_first_k: int = 3
) -> List[int]:
    """
    Select indices of top-k checks (by score) but try to ensure category diversity
    within the first 'diversity_first_k' positions.
    """
    indexed = list(enumerate(checks))
    # sort by score desc, then stable by index
    indexed.sort(key=lambda t: (-t[1].score, t[0]))

    if k <= 0 or not indexed:
        return []

    chosen: List[int] = []
    seen_cats: set[Category] = set()

    # first pass: fill up to diversity_first_k with unique categories if possible
    for i, c in indexed:
        if len(chosen) >= min(k, diversity_first_k):
            break
        if c.category not in seen_cats:
            chosen.append(i)
            seen_cats.add(c.category)

    # second pass: fill remaining slots by score
    if len(chosen) < k:
        for i, _c in indexed:
            if i not in chosen:
                chosen.append(i)
                if len(chosen) >= k:
                    break

    return chosen


def _get_parent_folder_from_key(key: str) -> Optional[str]:
    """Return the last folder name before the file, or None if at bucket root."""
    decoded = unquote_plus(key)              # e.g. 'invoices/2025/A%20B.pdf' -> 'invoices/2025/A B.pdf'
    parent_name = PurePosixPath(decoded).parent.name
    return parent_name or None


def _bedrock_safe_doc_name(raw: str) -> str:
    # drop extension and path
    name = os.path.splitext(os.path.basename(raw))[0]
    # keep only: alnum, space, hyphen, (), []
    name = re.sub(r"[^A-Za-z0-9\s\-\(\)\[\]]", " ", name)
    # collapse multiple spaces; trim
    name = re.sub(r"\s+", " ", name).strip()
    return name or "Document"