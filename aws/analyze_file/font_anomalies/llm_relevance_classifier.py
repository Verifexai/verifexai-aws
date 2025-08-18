from __future__ import annotations

import json
import re, os
from typing import Dict, List, Tuple, Optional, Any

import boto3
import fitz  # PyMuPDF

from aws.common.config.config import BEDROCK_REGION, FONT_MODEL_ID
from aws.common.utilities.logger_manager import LoggerManager, ANALYZE_FILE
from aws.common.utilities.utils import _bedrock_safe_doc_name

DOC_MAX_BYTES = 4_500_000
MAX_TOKENS_FOR_TEXT = 64

class LLMRelevanceClassifier:
    """
    LLM-only classifier that marks each font anomaly as:
      CORE    – important body content
      ANCILLARY – header/footer/contact/page-number, etc.
      NOISE   – OCR junk / unrelated

    It batches anomalies by page -> 1 LLM call per page. No heuristics.
    """

    def __init__(self,
                bedrock_client: Optional[Any] = None,
                model_id: str = "anthropic.claude-3-haiku-20240307-v1:0",):
        self.model_id = model_id
        self.client = bedrock_client or boto3.client("bedrock-runtime")
        self.logger = LoggerManager.get_module_logger(ANALYZE_FILE)

    # ---------- public API ----------
    def classify(self, anomalies: List[Dict], pdf_path: Optional[str]) -> List[Dict]:
        if not anomalies:
            return []

        # Group anomalies by page to do one request per page
        page_groups: Dict[int, List[Tuple[int, Dict]]] = {}
        for i, a in enumerate(anomalies):
            page = int(a.get("page", 1))
            page_groups.setdefault(page, []).append((i, a))

        results: List[Optional[Dict]] = [None] * len(anomalies)

        # Extract each page once if we’re attaching
        page_bytes_cache: Dict[int, Optional[bytes]] = {}
        if pdf_path and pdf_path.lower().endswith(".pdf"):
            for page in page_groups:
                try:
                    b = self._single_page_pdf_bytes(pdf_path, page)
                    if b and len(b) <= DOC_MAX_BYTES:
                        page_bytes_cache[page] = b
                    else:
                        page_bytes_cache[page] = None
                        if b:
                            self.logger.info("Page %s too large for attachment (%s bytes); skipping.", page, len(b))
                except Exception as e:
                    page_bytes_cache[page] = None
                    self.logger.warning("Failed to slice page %s: %s", page, e)

        # Make one Converse call per page
        page_heights = self._get_page_heights(pdf_path)
        for page, items in page_groups.items():
            texts = [a.get("text", "") for _, a in items]
            page_h = page_heights.get(page)  # compute once with fitz (page.rect.height)
            msgs = self._build_messages(items, page, page_bytes_cache.get(page), page_height=page_h)
            max_tokens = MAX_TOKENS_FOR_TEXT * len(texts)
            # Converse API call (official message schema + performanceConfig). :contentReference[oaicite:2]{index=2}
            kwargs = dict(
                modelId=self.model_id,
                messages=msgs,
                system=[{"text": self._system_prompt()}],
                inferenceConfig={"maxTokens": max_tokens, "temperature": 0.0, "topP": 0.9},
            )
            try:
                resp = self.client.converse(**kwargs)
                verdicts = self._parse_verdicts(resp, expected=len(texts))
            except Exception as e:
                self.logger.error("Bedrock converse failed: %s", e, exc_info=True)
                verdicts = [{"label": "ANCILLARY", "reason": "fallback", "confidence": 0.6}
                            for _ in texts]

            # merge back in original order
            for (idx, a), v in zip(items, verdicts):
                results[idx] = {**a, "relevance": v}

        # keep stable ordering from input
        return [r for r in results if r is not None]

    # ---------- payload builders ----------
    def _system_prompt(self) -> str:
        return (
            "You are a fast document triage function. "
            "For each snippet decide:\n"
            "CORE = important body content; "
            "ANCILLARY = headers/footers/contact/address/page numbers/disclaimers; "
            "NOISE = OCR garble or irrelevant.\n"
            "Documents may include Hebrew and English."
        )

    def _get_page_heights(self, pdf_path: str) -> Dict[int, float]:
        heights = {}
        try:
            with fitz.open(pdf_path) as doc:
                for i, page in enumerate(doc, 1):
                    heights[i] = float(page.rect.height)
        except Exception:
            pass
        return heights

    def _build_messages(
            self,
            items: List[Dict],  # anomaly dicts: {"text","box","page","font",...}
            page: int,
            page_pdf: Optional[bytes],
            page_height: Optional[float] = None,
            repeated_texts: Optional[set] = None  # texts that appear on multiple pages (optional)
    ) -> List[Dict]:
        """
        Build a compact, impact-first prompt so the model decides CORE/ANCILLARY/NOISE
        based on whether the snippet is essential to the document’s main purpose,
        not on its field type (phone/email/date/ID can be CORE or ANCILLARY).
        """
        import json, re

        def _region_hint(box):
            if not (box and page_height and page_height > 0):
                return "unknown", None
            y_mid = (box[1] + box[3]) / 2.0
            y_norm = y_mid / float(page_height)
            if y_norm <= 0.12:
                return "header", round(y_norm, 3)
            if y_norm >= 0.88:
                return "footer", round(y_norm, 3)
            return "body", round(y_norm, 3)

        # VERY light features; no strong assumptions about field types
        def _lite_features(s: str) -> dict:
            is_short = len(s.strip()) <= 2
            looks_garble = bool(re.search(r"[^\w\s\u0590-\u05FF.,:/@+\-\(\)\[\]]", s))
            digits = sum(ch.isdigit() for ch in s)
            return {
                "len": len(s),
                "digits": digits,
                "is_very_short": is_short,
                "looks_garble": looks_garble,
            }

        snippets = []
        for i, a in items:
            t = (a.get("text") or "").strip()
            region, y_norm = _region_hint(a.get("box"))
            feats = _lite_features(t)
            is_repeated = bool(repeated_texts and t in repeated_texts)
            snippets.append({
                "index": i,
                "text": t,
                "region_hint": region,  # header | body | footer | unknown
                "y_norm": y_norm,  # 0 top … 1 bottom (optional)
                "is_repeated": is_repeated,  # header/footer boilerplate tends to repeat
                **feats
            })

        system_rules = (
            "You are a document triage function for mixed Hebrew/English documents. "
            "First, briefly infer the page’s main purpose internally (do not output it). "
            "Then label each snippet using ONLY these definitions:\n\n"
            "CORE  = Removing it would change the document’s main meaning, validity, or outcome. "
            "Examples include identifiers of the parties or subject, decisive facts (who/what/when/where/amount), "
            "effective/signature details, or contact details that identify a specific party central to the action.\n"
            "ANCILLARY = Useful but peripheral to the main purpose: branding, headers/footers, boilerplate addresses or "
            "generic contact channels, page numbers, disclaimers, routing info, or anything repeated across pages.\n"
            "NOISE = OCR junk or fragments that carry no meaning.\n\n"
            "Guidance (type-agnostic):\n"
            "- IDs and dates are CORE. \n"
            "- If part of the text is CORE mark all by CORE"
            "- Footer/header region and repeated content are usually ANCILLARY unless clearly the main fact.\n"
            "- Body-region details tied to the actor/decision are often CORE.\n"
            "- If a snippet is very short or looks corrupted and adds no meaning → NOISE.\n\n"
            "Output STRICT JSON only."
        )

        # Tiny few-shot to disambiguate “same type, different role”
        fewshot = {
            "examples": [
                {
                    "snippets": [
                        {"text": "Client mobile: 050-1234567", "region_hint": "body", "is_repeated": False},
                        {"text": "Phone: +972-72-2216310  Website: example.com", "region_hint": "footer",
                         "is_repeated": True}
                    ],
                    "verdicts": [
                        {"label": "CORE", "reason": "party-specific contact for action", "confidence": 0.88},
                        {"label": "ANCILLARY", "reason": "generic company footer", "confidence": 0.93},
                        {"label": "CORE", "reason": "יאיר ת.ז. 312496730", "confidence": 0.93}
                    ]
                }
            ]
        }

        user_payload = {
            "task": "Classify snippets as CORE / ANCILLARY / NOISE by impact on main purpose.",
            "page": page,
            "snippets": snippets,
            "schema": {
                "verdicts": [
                    {"label": "CORE|ANCILLARY|NOISE", "confidence": "0..1"}
                ]
            },
            "requirements": [
                "Return JSON ONLY with key 'verdicts'.",
                "Order must match the input snippets."
            ],
            "examples": fewshot
        }

        content = [
            {"text": system_rules},
            {"text": json.dumps(user_payload, ensure_ascii=False)}
        ]
        if page_pdf:
            # use a safe document name (no dots/underscores)
            safe_name = _bedrock_safe_doc_name(f"page {page}")
            content.append({
                "document": {"name": safe_name, "format": "pdf", "source": {"bytes": page_pdf}}
            })

        return [{"role": "user", "content": content}]

    # ---------- response parsing ----------
    def _parse_verdicts(self, resp: Dict, expected: int) -> List[Dict]:
        parts = resp.get("output", {}).get("message", {}).get("content", [])
        text = "".join(p.get("text", "") for p in parts if "text" in p).strip()
        try:
            data = json.loads(text)
            verdicts = data.get("verdicts", [])
        except Exception:
            verdicts = []

        # normalize & pad if model returned fewer items
        out: List[Dict] = []
        for v in verdicts[:expected]:
            out.append({
                "label": str(v.get("label", "ANCILLARY")).upper(),
                "reason": v.get("reason", ""),
                "confidence": float(v.get("confidence", 0.7)),
            })
        while len(out) < expected:
            out.append({"label": "ANCILLARY", "reason": "default-pad", "confidence": 0.6})
        return out

    # ---------- pdf utils ----------
    def _single_page_pdf_bytes(self, pdf_path: str, page_num: int) -> bytes:
        src = fitz.open(pdf_path)
        dst = fitz.open()
        dst.insert_pdf(src, from_page=page_num - 1, to_page=page_num - 1)
        b = dst.tobytes()  # PDF bytes for that page
        dst.close()
        src.close()
        return b
