from typing import List, Optional, Dict, Any

from aws.analyze_file.metadata.metadata_factory import MetadataFactory
from aws.common.config.config import client_config
from aws.common.models.check_result import CheckResult
from aws.common.models.evidence import Evidence
from aws.common.utilities.enums import Category, Kind, EvidenceType, TerminationCertificateField, FileType
from aws.common.utilities.logger_manager import LoggerManager, METADATA
from aws.common.utilities.utils import _make_id, _now_iso

logger = LoggerManager.get_module_logger(METADATA)


def analyze_metadata(local_file_path: str,
                    file_type: FileType,
                    label_data: Dict[str, Any]=None) -> List[CheckResult]:
    """Run metadata checks for the given file and return CheckResult list."""
    logger.info("Analyzing metadata for %s", local_file_path)
    invoice_dates = _extract_termination_dates_texts(label_data)
    checks: List[CheckResult] = []

    scorer = MetadataFactory.get_metadata_scorer(local_file_path)
    if scorer is None:
        logger.warning("No metadata scorer available for %s", local_file_path)
        return checks

    try:
        metadata = scorer.run(invoice_dates=invoice_dates)
    except Exception as exc:
        logger.error("Metadata scorer execution failed for %s: %s", local_file_path, exc)
        return checks

    # Date validation
    creation = metadata.get("creation_date")
    modification = metadata.get("modification_date")
    if creation or modification:
        score = max(creation.get("score", 0) if isinstance(creation, dict) else 0,
                    modification.get("score", 0) if isinstance(modification, dict) else 0)
        description = creation.get("description", "") if isinstance(creation, dict) else ""
        evidence = []
        if creation:
            evidence.append(Evidence(type=EvidenceType.METADATA,
                                     value={"creation_date": creation.get("value") if isinstance(creation, dict) else creation}))
        if modification:
            evidence.append(Evidence(type=EvidenceType.METADATA,
                                     value={"modification_date": modification.get("value") if isinstance(modification, dict) else modification}))

        if invoice_dates:
            evidence.append(Evidence(type=EvidenceType.METADATA,
                                     value={"document dates:": ",".join(invoice_dates)}))

        checks.append(_build_check("TimestampCheck", Kind.TIMESTAMP_INCONSISTENT,
                                   "Timestamp Consistency", description, score, evidence))

    # Producer / software validation
    producer = metadata.get("producer") or metadata.get("software")
    if producer:
        evidence = [Evidence(type=EvidenceType.METADATA,
                              value={"producer": producer.get("value") if isinstance(producer, dict) else producer})]
        description = producer.get("description", "") if isinstance(producer, dict) else ""
        score = producer.get("score", 0) if isinstance(producer, dict) else 0
        checks.append(_build_check("ProducerCheck", Kind.SUSPICIOUS_PRODUCER,
                                   "Producer Validation", description, score, evidence))

    mismatch = metadata.get("producer_xmp_mismatch")
    if mismatch:
        evidence = [Evidence(type=EvidenceType.METADATA, value={"xmp_producer": mismatch.get("value")})]
        description = mismatch.get("description", "")
        score = mismatch.get("score", 0)
        checks.append(
            _build_check(
                "ProducerXMPMismatchCheck",
                Kind.PRODUCER_XMP_MISMATCH,
                "Producer/XMP Mismatch",
                description,
                score,
                evidence,
            )
        )

    # Digital signature validation (PDF only)
    signatures = metadata.get("signatures")
    if signatures:
        evidence = [Evidence(type=EvidenceType.METADATA, value={"signatures": signatures.get("value")})]
        description = signatures.get("description", "")
        score = signatures.get("score", 0)
        checks.append(_build_check("DigitalSignatureCheck", Kind.DIGITAL_SIGNATURE_INVALID,
                                   "Digital Signature Validation", description, score, evidence))

    # Annotation presence (PDF only)
    annotations = metadata.get("annotation")
    if annotations:
        details = annotations.get("details") or {}
        counts_by_type = details.get("counts_by_type") or {}
        top_suspicious = details.get("top_suspicious") or []
        raw_annots = annotations.get("value") or []

        # keep evidence compact; avoid dumping thousands of annots
        EVIDENCE_SAMPLE = 40
        evidence_payload = {
            "summary": {
                "total_annotations": len(raw_annots),
                "counts_by_type": counts_by_type,
                "suspicious_count": len(top_suspicious),
                "score": int(annotations.get("score", 0)),
            },
            # top_suspicious already trimmed upstream; trim again defensively
            "top_suspicious": top_suspicious[:20],
            # provide a small raw sample for debugging/triage
            "sample_annotations": raw_annots[:EVIDENCE_SAMPLE],
            "clean_file": annotations.get("original_file"),
        }

        evidence = [
            Evidence(
                type=EvidenceType.METADATA,
                value=evidence_payload,
            )
        ]

        description = annotations.get("description", "")
        score = int(annotations.get("score", 0))

        checks.append(
            _build_check(
                "AnnotationCheck",  # id suffix (kept for compatibility)
                Kind.ANNOTATION_PRESENT,  # existing enum
                "Annotation Analysis",  # clearer title
                description,
                score,
                evidence,
            )
        )

    image_only = metadata.get("image_only")
    if image_only:
        evidence = [Evidence(type=EvidenceType.METADATA, value={"image_only": image_only.get("value")})]
        description = image_only.get("description", "")
        score = image_only.get("score", 0)
        checks.append(
            _build_check(
                "ImageOnlyPDFCheck",
                Kind.IMAGE_ONLY_PDF,
                "Image-only PDF",
                description,
                score,
                evidence,
            )
        )

    logger.info("Metadata analysis produced %d checks", len(checks))
    return checks

def _extract_termination_dates_texts(label_data: Optional[Dict[str, Any]]) -> List[str]:
    """
    Collects the 'text' values (if present) for:
      - DOCUMENT_DATE
      - JOB_START_DATE
      - JOB_DEPARTURE_DATE
    Returns them as a list, in that order, skipping any missing values.
    """
    fields = [
        TerminationCertificateField.DOCUMENT_DATE.value,
        TerminationCertificateField.JOB_START_DATE.value,
        TerminationCertificateField.JOB_DEPARTURE_DATE.value,
    ]

    results: List[str] = []

    if not label_data:
        return results

    for field in fields:
        item = label_data.get(field)
        if item is None:
            continue

        # Expected shape from your pipeline:
        # { "label": <str>, "text": <str|None>, "original_text": <str|None>, "bbox": [...] }
        if isinstance(item, dict):
            v = item.get("text")
            if not v:
                # Fallbacks just in case:
                v = item.get("value") if isinstance(item.get("value"), str) else None
                if v is None and "value" in item:
                    # handle non-str value by stringifying
                    v = str(item["value"])
        else:
            # If the stored value isn't a dict, stringify it
            v = str(item)

        if v:
            results.append(v)

    return results

def _build_check(id_suffix: str, kind: Kind, title: str, description: str,
                 score: int, evidence: List[Evidence]) -> CheckResult:
    return CheckResult(
        id=_make_id(id_suffix),
        category=Category.FILE_METADATA,
        kind=kind,
        title=title,
        description=description,
        score=score,
        status=client_config.status_for(score),
        evidence=evidence,
        tags=[],
        timestamp=_now_iso()
    )
