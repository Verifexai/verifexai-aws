import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from PyPDF2 import PdfReader

from .metadata_base import MetadataBaseScorer, score_producer
from .metadata_utils import _name, _rect_area, _decode_annot_flags, _parse_pdf_date, _parse_invoice_date, \
    _parse_sign_dt, _safe_bool, _summarize_annotations, _join_msgs, _describe_reason, _sort_reasons, to_aware_utc

MAX_DATE_DIFF_DAYS = int(os.getenv("METADATA_MAX_DATE_DIFF_DAYS", "60"))



ANNOT_RISK_BASE = {
    "Link": 10,
    "Text": 75, "FreeText": 78, "Popup": 75,
    "Highlight": 50, "Underline": 50, "Squiggly": 55, "StrikeOut": 55, "Caret": 65,
    "Ink": 75, "Stamp": 60, "Redact": 85,
    "Widget": 45,  # form fields on an invoice tend to be odd
    "FileAttachment": 95, "Sound": 95, "Movie": 95, "RichMedia": 98,
}

# Catastrophic / fraud floors
CATASTROPHIC_INTEGRITY = 85   # broken signature => 100
FRAUD_FLOOR_TIME = 90          # any time anomaly => at least 95

SIGNATURES_WEIGHTS = {
    "untrusted_chain": 25,      # chain cannot build to a trusted root
    "indeterminate_chain": 10,  # trust not known / indeterminate
    "not_cover_entire": 8,      # signature doesn't cover ENTIRE_FILE
    "docmdp_fail": 8,           # DocMDP/diff policy failure
    "missing_sign_time": 15,     # signing time absent or unparsable
}


SUSPICIOUS_SCHEMES = ("javascript:", "data:", "file:", "ftp:")

def _score_single_annotation(a: Dict[str, Any],
                             page_area: Optional[float],
                             doc_creation: Optional[datetime],
                             doc_mod: Optional[datetime],
                             allowed_domains: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Returns {'score': int, 'reasons': [..]} for one annotation.
    """
    reasons = []
    subtype = _name(a.get("subtype"))
    base = ANNOT_RISK_BASE.get(subtype, 40)  # unknown types => medium
    risk = base

    # Flags
    flags = a.get("flags", {})
    if flags.get("Invisible") or flags.get("Hidden"):
        risk = max(risk, 90); reasons.append("Annotation hidden/invisible")
    if flags.get("NoView") and flags.get("Print"):
        risk = max(risk, 95); reasons.append("Print-only hidden annotation")

    # Action /URI /S
    action = a.get("action", {})
    action_s = _name(action.get("S"))
    uri = str(action.get("URI") or "") if action_s == "URI" else ""

    # High-risk action types
    if action_s in ("JavaScript", "Launch", "SubmitForm", "GoToR"):
        risk = max(risk, 95); reasons.append(f"Action {action_s}")

    # URI checks
    if uri:
        u = uri.strip().lower()
        if u.startswith(SUSPICIOUS_SCHEMES):
            risk = max(risk, 92); reasons.append(f"Suspicious scheme: {u.split(':',1)[0]}")
        else:
            # External domain sanity
            try:
                host = urlparse(uri).hostname or ""
                if allowed_domains and host and not any(host.endswith(d) for d in allowed_domains):
                    # don't slam benign links—just nudge unless many exist
                    risk = max(risk, 30); reasons.append(f"External link: {host}")
            except Exception:
                pass

    # Giant overlay rects
    rect_area = None
    if page_area and a.get("bbox"):
        rect_area = _rect_area(a["bbox"])
        if rect_area and page_area > 0:
            coverage = rect_area / page_area
            if coverage > 0.8:
                risk = max(risk, 92); reasons.append(f"Very large rect ({coverage:.0%})")
            elif coverage > 0.5:
                risk = max(risk, 85); reasons.append(f"Large rect ({coverage:.0%})")

    # Timestamps
    ann_m = _parse_pdf_date(a.get("modified"))
    if doc_creation and ann_m and ann_m < doc_creation:
        risk = max(risk, 85); reasons.append("Annotation predates document creation")
    if doc_mod and ann_m and ann_m > doc_mod:
        risk = max(risk, 88); reasons.append("Annotation newer than document ModDate")

    # Content presence on markup types
    contents = (a.get("contents") or "").strip()
    if subtype in {"Text","FreeText","Popup","Ink","Caret","Redact","Stamp"} and contents:
        reasons.append("User comment/markup present")

    return {"score": int(min(100, max(0, risk))), "reasons": reasons}

class PDFMetadataScorer(MetadataBaseScorer):
    def _extract_metadata(self) -> Dict[str, Any]:
        self.logger.info("Extracting PDF metadata from %s", self.file_path)
        try:
            reader = PdfReader(self.file_path)
        except Exception as exc:
            self.logger.error("Failed to read PDF metadata: %s", exc)
            return {}
        info = reader.metadata or {}
        data: Dict[str, Any] = {
            "creation_date": info.get("/CreationDate"),
            "modification_date": info.get("/ModDate"),
            "producer": info.get("/Producer") or info.get("/Creator"),
        }

        # XMP producer if available
        try:
            xmp = reader.xmp_metadata  # type: ignore[attr-defined]
            if xmp:
                xmp_producer = getattr(xmp, "producer", None) or getattr(xmp, "xmp_creator_tool", None)
                if xmp_producer:
                    data["xmp_producer"] = str(xmp_producer)
        except Exception as exc:
            self.logger.warning("Failed to parse XMP metadata: %s", exc)

        # annotations and text extraction to detect image-only PDFs
        annotations: List[Dict[str, Any]] = []
        text_found = False
        page_dims: Dict[int, Dict[str, float]] = {}

        for idx, page in enumerate(reader.pages, start=1):
            # text presence
            try:
                text = page.extract_text() or ""
                if text.strip():
                    text_found = True
            except Exception as exc:
                self.logger.error("Failed extracting text from page %d: %s", idx, exc)

            # page geometry
            try:
                mb = page.mediabox
                pw, ph = float(mb.width), float(mb.height)
                page_dims[idx] = {"width": pw, "height": ph, "area": pw * ph}
            except Exception:
                page_dims[idx] = {"width": 0.0, "height": 0.0, "area": 0.0}

            # annotations
            annots = page.get("/Annots")
            if not annots:
                continue

            for annot in annots:
                try:
                    obj = annot.get_object()
                    subtype = _name(obj.get("/Subtype"))
                    contents = str(obj.get("/Contents")) if obj.get("/Contents") else None
                    flags = _decode_annot_flags(obj.get("/F"))
                    action = obj.get("/A") or {}
                    bbox = obj.get("/Rect")
                    modified = obj.get("/M")  # D:yyyy...
                    # Normalize a few action fields for easy scoring
                    action_info = {}
                    if isinstance(action, dict):
                        action_info = {
                            "S": action.get("/S"),
                            "URI": action.get("/URI"),
                            "F": action.get("/F"),
                            "JS": action.get("/JS"),
                            "D": action.get("/D"),
                        }
                    annotations.append({
                        "page": idx,
                        "bbox": bbox,
                        "subtype": subtype,
                        "contents": contents,
                        "flags": flags,
                        "action": action_info,
                        "modified": modified,
                    })
                except Exception as exc:
                    self.logger.error("Failed to parse annotation on page %d: %s", idx, exc)
                    annotations.append({"page": idx, "subtype": None, "contents": None})
        if annotations:
            data["annotation"] = annotations
        data["image_only"] = not text_found

        # digital signatures
        signatures = []
        try:
            from pyhanko.pdf_utils.reader import PdfFileReader as HankoReader
            from pyhanko.sign.validation import ValidationContext, validate_pdf_signature
            from pyhanko.sign.validation.status import SignatureCoverageLevel
            from pyhanko.sign.validation import KeyUsageConstraints

            with open(self.file_path, "rb") as pdf_in:
                hanko_reader = HankoReader(pdf_in)

                key_usage = KeyUsageConstraints(
                    # pyHanko’s default needs non_repudiation; accept either bit for practicality
                    key_usage={'non_repudiation', 'digital_signature'},
                    match_all_key_usages=False
                )

                vc = ValidationContext(
                    # use OS trust store (default) — no trust_roots passed
                    allow_fetching=True,  # fetch AIA/OCSP/CRL if needed
                    retroactive_revinfo=True  # Acrobat-like revocation timing
                )
                for emb_sig in hanko_reader.embedded_signatures:
                    st = validate_pdf_signature(
                        emb_sig, vc,
                        key_usage_settings=key_usage,
                        skip_diff=True  # ignore DocMDP/diff policy
                    )
                    signatures.append({
                        "field": emb_sig.field_name,
                        "signed_at": st.signer_reported_dt.isoformat() if st.signer_reported_dt else None,
                        # crypto integrity of the byte ranges
                        "intact": bool(getattr(st, "intact", None)),
                        # chain builds to an OS-trusted root
                        "chain_trusted": bool(st.trusted),
                        # simple verdict most people expect
                        "trusted": bool(st.trusted) and bool(getattr(st, "intact", None)),
                        "covers_document": st.coverage == SignatureCoverageLevel.ENTIRE_FILE,
                        # pyHanko’s full-policy verdict (can be False due to stricter policies)
                        "valid_policy": bool(st.bottom_line),
                    })
        except ImportError:
            self.logger.warning("pyHanko not installed, skipping signature validation")
        except Exception as exc:
            self.logger.error("Failed to parse signatures: %s", exc)
        if signatures:
            data["signatures"] = signatures
        return data

    def _score_metadata(self, metadata: Dict[str, Any], invoice_dates: List[str]) -> Dict[str, Any]:
        self.logger.info("Scoring PDF metadata for %s", self.file_path)
        scored: Dict[str, Any] = {}
        scores: List[int] = []
        invoice_dt = _parse_invoice_date(invoice_dates)

        creation_raw = metadata.get("creation_date")
        modification_raw = metadata.get("modification_date")
        creation_dt = _parse_pdf_date(creation_raw) if creation_raw else None
        modification_dt = _parse_pdf_date(modification_raw) if modification_raw else None

        score = 0
        description = ""
        if creation_dt and modification_dt and modification_dt < creation_dt:
            score = 80
            description = "Modification date precedes creation date"
        elif invoice_dt and creation_dt:
            diff = abs((creation_dt - invoice_dt).days)
            if diff > MAX_DATE_DIFF_DAYS:
                score = 60
                description = f"Creation date is {diff} days from invoice date"
        if invoice_dt and modification_dt:
            diff = abs((modification_dt - invoice_dt).days)
            if diff > MAX_DATE_DIFF_DAYS:
                score = max(score, 60)
                description = f"Modification date is {diff} days from invoice date"
        if not creation_dt and not modification_dt:
            score = 20
            description = "Missing creation and modification dates"
        scored["creation_date"] = {"value": creation_raw, "score": score, "description": description}
        scored["modification_date"] = {"value": modification_raw, "score": score, "description": description}
        scores.append(score)

        producer = metadata.get("producer")
        if producer is not None:
            p_result = score_producer(producer)
            scored["producer"] = {"value": producer, "score": p_result["score"], "description": p_result["description"]}
            scores.append(p_result["score"])

        xmp_producer = metadata.get("xmp_producer")
        if not producer and xmp_producer:
            mismatch_score = 60
            scored["producer_xmp_mismatch"] = {
                "value": xmp_producer,
                "score": mismatch_score,
                "description": "Producer field blank but XMP producer present",
            }
            scores.append(mismatch_score)

        signatures = metadata.get("signatures")
        if signatures:
            issue_scores = []
            for s in signatures:
                reasons = []
                issue = 0

                # Parse signing time
                sign_dt = _parse_sign_dt(s)

                # Inputs from your validator payload
                intact = _safe_bool(s.get("intact"))  # cryptographic integrity (True/False/None)
                trusted = _safe_bool(s.get("trusted"))  # chain to trusted root (True/False/None)
                covers_document = bool(s.get("covers_document"))
                docmdp_ok = s.get("docmdp_ok")  # True/False/None

                # ---- 1) Catastrophic: cryptographic integrity failure => 100 ----
                if intact is False:
                    issue = CATASTROPHIC_INTEGRITY
                    reasons.append("integrity_fail")
                    s["issue_score"] = issue
                    s["issue_reasons"] = reasons
                    issue_scores.append(issue)
                    continue  # no need to consider anything else

                # ---- 2) Time anomalies => fraud floor 95 ----
                time_issue = False
                if sign_dt is None:
                    issue += SIGNATURES_WEIGHTS["missing_sign_time"];
                    reasons.append("missing_sign_time")
                else:
                    sign_dt = to_aware_utc(sign_dt)
                    creation_dt = to_aware_utc(creation_dt)
                    modification_dt = to_aware_utc(modification_dt)
                    invoice_dt = to_aware_utc(invoice_dt)

                    if creation_dt and sign_dt < creation_dt:
                        time_issue = True;
                        reasons.append("sign_before_creation")
                    if modification_dt and sign_dt > modification_dt:
                        time_issue = True;
                        reasons.append("sign_after_modification")
                    if invoice_dt:
                        diff = abs((sign_dt - invoice_dt).days)
                        if diff > MAX_DATE_DIFF_DAYS:
                            time_issue = True;
                            reasons.append("invoice_date_far")

                if time_issue:
                    issue = max(issue, FRAUD_FLOOR_TIME)  # enforce high score

                # ---- 3) Smaller penalties (trust, coverage, policy) ----
                if trusted is False:
                    issue += SIGNATURES_WEIGHTS["untrusted_chain"];
                    reasons.append("untrusted_chain")
                elif trusted is None:
                    issue += SIGNATURES_WEIGHTS["indeterminate_chain"];
                    reasons.append("indeterminate_chain")

                if not covers_document:
                    issue += SIGNATURES_WEIGHTS["not_cover_entire"];
                    reasons.append("not_cover_entire")

                if docmdp_ok is False:
                    issue += SIGNATURES_WEIGHTS["docmdp_fail"];
                    reasons.append("docmdp_fail")

                # Clamp and store per-signature diagnostics
                issue = max(0, min(100, issue))
                s["issue_score"] = issue
                s["issue_reasons"] = reasons
                s["signed_at"] = sign_dt.isoformat() if sign_dt else None

                issue_scores.append(issue)

            # Overall (keep "best signature wins" semantics)
            overall_issue = max(issue_scores) if issue_scores else 100
            sig_idx = issue_scores.index(overall_issue) if issue_scores else None
            chosen = signatures[sig_idx] if sig_idx is not None else None

            if overall_issue == 0 or not chosen:
                s_desc = "Valid digital signature"
            else:
                # Build precise messages from the chosen signature's reasons
                reasons_sorted = _sort_reasons(chosen.get("issue_reasons", []))
                # recover dates for rich messages
                sign_dt = chosen.get("signed_at")
                try:
                    sign_dt = datetime.fromisoformat(sign_dt) if isinstance(sign_dt, str) else sign_dt
                except Exception:
                    pass

                msgs = [
                    _describe_reason(
                        r,
                        sign_dt=sign_dt,
                        creation_dt=creation_dt,
                        modification_dt=modification_dt,
                        invoice_dt=invoice_dt,
                        max_diff_days=MAX_DATE_DIFF_DAYS,
                    )
                    for r in reasons_sorted
                ]

                # Stronger headline if catastrophic/fraud-like reasons present
                headline = "Cryptographically invalid" if "integrity_fail" in reasons_sorted \
                    else ("Likely fraudulent" if any(r in reasons_sorted for r in
                                                     ("sign_before_creation", "sign_after_modification",
                                                      "invoice_date_far"))
                          else "Issues detected in digital signature")

                s_desc = f"{headline}: {_join_msgs(msgs)}"

            scored["signatures"] = {
                "value": signatures,
                "score": overall_issue,
                "description": s_desc,
            }
            scores.append(overall_issue)

        annotations = metadata.get("annotation")
        if annotations:
            page_dims = metadata.get("page_dims", {})
            creation_dt = _parse_pdf_date(metadata.get("creation_date") or "")
            modification_dt = _parse_pdf_date(metadata.get("modification_date") or "")

            # Optional: vendor allow-list (domains treated as low risk for /Link)
            allowed_domains = None  # e.g., ["vendor.com", "paypal.com"]

            per_ann = []
            counts = _summarize_annotations(annotations)
            link_count = counts.get("Link", 0)

            max_score = 0
            suspicious = []

            for a in annotations:
                area = None
                dims = page_dims.get(a.get("page"), {})
                area = dims.get("area")
                res = _score_single_annotation(
                    a, area, creation_dt, modification_dt, allowed_domains=allowed_domains
                )
                # Nudge benign single links down; escalate many links
                if _name(a.get("subtype")) == "Link":
                    if link_count <= 3 and res["score"] <= 30:
                        res["score"] = min(res["score"], 15)
                    elif link_count > 8 and res["score"] < 40:
                        res["score"] = 40

                per_ann.append({**a, "risk": res["score"], "reasons": res["reasons"]})
                if res["score"] > max_score:
                    max_score = res["score"]
                if res["score"] >= 70:
                    suspicious.append(per_ann[-1])

            # Final annotation score is the worst offender (works well with your max-based final_score)
            a_score = int(max_score)
            if a_score == 0 and annotations:
                a_score = 10  # tiny baseline if only benign links

            a_desc_parts = [f"{len(annotations)} annotations found"]
            if counts:
                a_desc_parts.append("by type: " + ", ".join(f"{k}×{v}" for k, v in sorted(counts.items())))
            if suspicious:
                a_desc_parts.append(f"{len(suspicious)} suspicious (≥70)")

            scored["annotation"] = {
                "value": annotations,  # full raw list if you want it
                "score": a_score,
                "description": "; ".join(a_desc_parts),
                "details": {
                    "counts_by_type": counts,
                    "top_suspicious": suspicious[:20],  # cap to avoid huge outputs
                },
                "original_file": self.file_path,
            }
            scores.append(a_score)

        image_only = metadata.get("image_only")
        io_score = 60 if image_only else 0
        scored["image_only"] = {
            "value": bool(image_only),
            "score": io_score,
            "description": "PDF contains only images" if image_only else "Text content present",
        }
        scores.append(io_score)

        scored["final_score"] = max(scores) if scores else 0
        return scored