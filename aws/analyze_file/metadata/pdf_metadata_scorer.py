import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from PyPDF2 import PdfReader

from .metadata_base import MetadataBaseScorer, score_producer

MAX_DATE_DIFF_DAYS = int(os.getenv("METADATA_MAX_DATE_DIFF_DAYS", "60"))



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

ANNOT_RISK_BASE = {
    "Link": 10,
    "Text": 75, "FreeText": 78, "Popup": 75,
    "Highlight": 50, "Underline": 50, "Squiggly": 55, "StrikeOut": 55, "Caret": 65,
    "Ink": 75, "Stamp": 60, "Redact": 85,
    "Widget": 45,  # form fields on an invoice tend to be odd
    "FileAttachment": 95, "Sound": 95, "Movie": 95, "RichMedia": 98,
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
            root = reader.trailer.get("/Root", {})
            form = root.get("/AcroForm") if root else None
            fields = form.get("/Fields", []) if form else []
            for field in fields:
                field_obj = field.get_object()
                if field_obj.get("/FT") == "/Sig":
                    sig = field_obj.get("/V")
                    if sig:
                        sig_obj = sig.get_object()
                        byte_range = sig_obj.get("/ByteRange")
                        file_size = os.path.getsize(self.file_path)
                        valid = False
                        if byte_range and len(byte_range) == 4:
                            valid = byte_range[0] == 0 and byte_range[2] + byte_range[3] == file_size
                        signatures.append({
                            "name": sig_obj.get("/Name"),
                            "date": sig_obj.get("/M"),
                            "byte_range": byte_range,
                            "valid": valid,
                        })
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
            invalid = [s for s in signatures if not s.get("valid")]
            s_score = 0 if not invalid else 80
            s_desc = "Valid digital signature" if not invalid else "Invalid digital signature"
            scored["signatures"] = {"value": signatures, "score": s_score, "description": s_desc}
            scores.append(s_score)

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
