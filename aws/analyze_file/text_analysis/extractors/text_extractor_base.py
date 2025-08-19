from __future__ import annotations

import json
from enum import Enum
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

import boto3
import fitz  # type: ignore
from PIL import Image

from aws.common.config.config import BEDROCK_REGION
from aws.common.utilities.date_utils import parse_date_multiple_formats
from aws.common.utilities.enums import TaxCertificateField, TerminationCertificateField
from aws.common.utilities.logger_manager import LoggerManager, ANALYZE_FILE
from aws.common.utilities.utils import _bedrock_safe_doc_name

class BaseTextExtractor:
    """Base text extractor leveraging Bedrock and OCR results.

    Subclasses should define ``prompt``, ``fields`` and ``date_fields``
    to describe the expected model output.
    """

    prompt: str = ""
    fields: List[Enum] = []
    date_fields: List[str] = []

    def __init__(
        self,
        *,
        bedrock_client: Optional[Any] = None,
        model_id: str = "anthropic.claude-3-haiku-20240307-v1:0",
    ) -> None:
        self.client = bedrock_client or boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)
        self.model_id = model_id
        self.logger = LoggerManager.get_module_logger(ANALYZE_FILE)

    # ------------------------------------------------------------------
    # Document loading and model invocation
    # ------------------------------------------------------------------
    def _load_document(self, path: str) -> Tuple[bytes, str]:
        if path.lower().endswith(".pdf"):
            with open(path, "rb") as file:
                data = file.read()
            doc = fitz.open(stream=data, filetype="pdf")
            _ = doc.load_page(0)
            doc.close()
            return data, "application/pdf"

        with Image.open(path) as image:
            image = image.convert("RGB")
            buffer = BytesIO()
            image.save(buffer, format="PNG", optimize=True)
            return buffer.getvalue(), "image/png"

    def _invoke_model(self, prompt: str, doc_bytes: bytes, media_type: str) -> Dict[str, Any]:
        if media_type == "application/pdf":
            content = [
                {"text": prompt},
                {
                    "document": {
                        "name": _bedrock_safe_doc_name("source.pdf"),
                        "format": "pdf",
                        "source": {"bytes": doc_bytes},
                    }
                },
            ]
        else:
            content = [
                {"text": prompt},
                {"image": {"format": "png", "source": {"bytes": doc_bytes}}},
            ]

        response = self.client.converse(
            modelId=self.model_id,
            messages=[{"role": "user", "content": content}],
            inferenceConfig={"maxTokens": 2000, "temperature": 0.2},
            additionalModelRequestFields={"stop_sequences": ["\n\nHuman:"]},
        )

        text_blocks = response.get("output", {}).get("message", {}).get("content", [])
        text = "".join(block.get("text", "") for block in text_blocks if "text" in block)
        try:
            return json.loads(text or "{}")
        except json.JSONDecodeError:
            start, end = text.find("{"), text.rfind("}")
            if start != -1 and end != -1:
                try:
                    return json.loads(text[start : end + 1])
                except Exception:
                    self.logger.error(f"Text Extractor json is not valid json:{text[start : end + 1]}")
                    pass
            return {}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _normalize(value: Any, *, is_date: bool = False) -> Optional[str]:
        if value is None:
            return None
        if is_date:
            try:
                return parse_date_multiple_formats(str(value)).strftime("%Y-%m-%d")
            except Exception:
                return None
        return str(value).strip()

    @staticmethod
    def _match_ocr(pages: List[List[Dict[str, Any]]], value: str) -> Tuple[Optional[str], Optional[List[float]]]:
        if not value:
            return None, None
        cleaned = value.replace(" ", "")
        best_token = None
        best_count = 0
        for page in pages:
            for token in page:
                token_text = str(token.get("text", ""))
                token_clean = token_text.replace(" ", "")
                if cleaned and cleaned in token_clean:
                    return token_text, token.get("bbox")
                count = sum(1 for w in value.split() if w and w in token_text)
                if count > best_count:
                    best_count = count
                    best_token = token
        if best_token:
            return best_token.get("text"), best_token.get("bbox")
        return None, None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def extract(
        self,
        file_path: str,
        pages_data: List[List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        doc_bytes, media_type = self._load_document(file_path)
        raw = self._invoke_model(self.prompt, doc_bytes, media_type)
        result: Dict[str, Any] = {}
        for field in self.fields:
            field_info = raw.get(field.value) or {}
            if isinstance(field_info, dict):
                raw_text = field_info.get("text")
                raw_value = field_info.get("value")
            else:
                raw_text = field_info
                raw_value = field_info
            normalized = self._normalize(raw_value, is_date=field.value in self.date_fields)
            search_value = raw_text or normalized
            original_text, bbox = self._match_ocr(pages_data, search_value or "")
            result[field.value] = {
                "label": field.value,
                "text": normalized,
                "original_text": original_text or raw_text,
                "bbox": bbox,
            }
        return result


