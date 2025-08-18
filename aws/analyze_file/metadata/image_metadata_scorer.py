import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from PIL import Image, ExifTags

from .metadata_base import MetadataBaseScorer, score_producer

MAX_DATE_DIFF_DAYS = int(os.getenv("METADATA_MAX_DATE_DIFF_DAYS", "60"))


def _parse_image_date(date_str: str) -> Optional[datetime]:
    if not date_str:
        return None
    for fmt in ("%Y:%m:%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def _parse_invoice_date(dates: List[str]) -> Optional[datetime]:
    for d in dates:
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%Y%m%d"):
            try:
                return datetime.strptime(d, fmt)
            except ValueError:
                continue
    return None


class ImageMetadataScorer(MetadataBaseScorer):
    def _extract_metadata(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        try:
            with Image.open(self.file_path) as img:
                exif = img.getexif() or {}
                tag_map = {ExifTags.TAGS.get(k, k): v for k, v in exif.items()}
                data["creation_date"] = tag_map.get("DateTimeOriginal") or tag_map.get("DateTime")
                data["modification_date"] = tag_map.get("DateTime")
                data["producer"] = tag_map.get("Software")
        except Exception:
            pass
        return data

    def _score_metadata(self, metadata: Dict[str, Any], invoice_dates: List[str]) -> Dict[str, Any]:
        scored: Dict[str, Any] = {}
        scores: List[int] = []
        invoice_dt = _parse_invoice_date(invoice_dates)

        creation_raw = metadata.get("creation_date")
        modification_raw = metadata.get("modification_date")
        creation_dt = _parse_image_date(creation_raw) if creation_raw else None
        modification_dt = _parse_image_date(modification_raw) if modification_raw else None

        score = 0
        description = ""
        if creation_dt and modification_dt and modification_dt < creation_dt:
            score = 80
            description = "Modification date precedes creation date"
        elif invoice_dt and creation_dt:
            diff = abs((creation_dt - invoice_dt).days)
            if diff > MAX_DATE_DIFF_DAYS:
                score = 60
                description = f"Creation date is {diff} days from document date"
        if invoice_dt and modification_dt:
            diff = abs((modification_dt - invoice_dt).days)
            if diff > MAX_DATE_DIFF_DAYS:
                score = max(score, 60)
                description = f"Modification date is {diff} days from document date"
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

        scored["final_score"] = max(scores) if scores else 0
        return scored
