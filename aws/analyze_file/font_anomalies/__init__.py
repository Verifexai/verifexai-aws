from typing import Any, Dict, List, Optional

from aws.analyze_file.OCR.ocr_processor import OCRProcessor
from aws.analyze_file.font_anomalies.font_anomaly_detector import FontAnomalyDetector
from aws.analyze_file.font_anomalies.llm_relevance_classifier import LLMRelevanceClassifier
from aws.common.config.config import client_config, FONT_MODEL_ID
from aws.common.models.check_result import CheckResult
from aws.common.models.evidence import Evidence
from aws.common.utilities.enums import Category, Kind, EvidenceType
from aws.common.utilities.logger_manager import LoggerManager
from aws.common.utilities.utils import _make_id, _now_iso, aggregate_mean_top_k

__all__ = ["font_anomalies_check"]

ocr_processor = OCRProcessor()
font_detector = FontAnomalyDetector()

def font_anomalies_check(local_file_path: str, pages_data: List[List[Dict]] = None, bedrock:Optional[Any] = None) -> CheckResult:
    logger = LoggerManager().get_module_logger(__name__)

    # Extract OCR information from file
    if pages_data is None:
        logger.info("Extracting OCR/text from file: %s", local_file_path)
        pages_data = ocr_processor.extract(local_file_path)

    logger.info("Detecting font anomalies")
    font_anomalies = font_detector.detect_with_file(pages_data, local_file_path)

    logger.info("Classifying font anomalies via LLM")
    llm_classifier = LLMRelevanceClassifier(bedrock_client=bedrock, model_id=FONT_MODEL_ID)
    final_result = llm_classifier.classify(font_anomalies, local_file_path)

    checkResult = _build_font_check(final_result)
    return checkResult


def _build_font_check(results: List[Dict]):
    relevant_results: List[Dict[str, Any]] = [
        a for a in (results or [])
        if (a.get("relevance", {}).get("label", "") or "").upper() != "NOISE"
    ]

    per_scores = [_per_anomaly_score(a) for a in relevant_results]
    core_scores = [
        s for a, s in zip(relevant_results, per_scores)
        if (a.get("relevance", {}).get("label", "") or "").upper() == "CORE"
    ]
    if core_scores:
        agg_score = aggregate_mean_top_k(core_scores, mean_top_k=3)  # strongest CORE drives the check
    else:
        agg_score = aggregate_mean_top_k(per_scores, mean_top_k=3)

    description = "No font issue has been found."
    if  len(relevant_results) >= 1:
        texts = [f'"{item['text']}"' for item in relevant_results]
        description = f"There are font issues in {",".join(texts)}"

    # evidence
    evidences = []
    for result in relevant_results:
        evidences.append(Evidence(
            type=EvidenceType.TEXT,
            value=result.get("text", ""),
            page=result.get("page", 1),
            bbox=result.get("box"),
            extra={"font":result.get("font"), "text_relevance": result.get("relevance", {}).get("label", "")}
        ))

    check: CheckResult = CheckResult(
    id=_make_id("FontManipulationCheck"),
    category=Category.VISUAL_ANALYSIS,
    kind=Kind.FONT_MANIPULATION,
    title="Font Manipulation",
    description=description,
    score=agg_score,
    status=client_config.status_for(agg_score),
    evidence=evidences,
    tags=[],
    timestamp=_now_iso()
    )

    return check

def _per_anomaly_score(a: Dict[str, Any]) -> int:
    """
    Gentle mapping:
    base = ((t_doc - share)/t_doc)^2  (0..1), so far-from-threshold grows,
    near-threshold stays small. Then weight by label + confidence.
    Cap ancillary so footers don't dominate.
    """
    score = float(a.get("score", 0.0))                         # observed share for this font               # 0..1

    rel = a.get("relevance", {}) or {}
    label = (rel.get("label") or "").upper()        # 0.5..1.0

    # label weights (CORE >>> ANCILLARY). Noise is filtered earlier.
    weight = 1 if label == "CORE" else 0.5

    raw = score * weight                               # keep footers low-impact
    return min(int(round(raw)),80)