"""Font anomaly detection utilities.

This module provides a :class:`FontAnomalyDetector` which can detect
unusual fonts in OCR output either by looking at the declared font names
or, if that information is missing, by analysing image patches of the
text itself.

The original implementation relied purely on simple frequency counting
and distance-from-mean heuristics.  The current implementation adds a
few improvements aimed at reducing false positives:

* Font names are normalised (case/variant stripped) before statistics are
  computed so that common variants such as ``Arial-Bold`` do not appear as
  distinct fonts.
* An ``IsolationForest`` model is used for the image based detector when
  sufficient samples are available.  This tends to be more robust than a
  fixed standard deviation threshold.

Both of these changes help to improve the accuracy of the detector and to
avoid flagging legitimate content as suspicious.
"""

from __future__ import annotations

import math
import os
import re
from typing import Dict, List, Optional

import cv2
import fitz  # PyMuPDF
import numpy as np
from skimage.feature import hog, local_binary_pattern
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# ---------------------------------------------------------------------------
# Constants for image-based anomaly detection
# ---------------------------------------------------------------------------
LBP_POINTS = 8
LBP_RADIUS = 1
SIGMA_THRESHOLD = 2

ITA_TEXT_MARKERS = [
    r"רשות המסים בישראל",
    r"אגף מס הכנסה ומיסוי מקרקעין",
    r"בשם פקיד השומה|פקיד(?:\s)?השומה"
]
ITA_TEMPLATE_CODE_PATTERNS = [r"\bis\d{2,3}[a-z]?\b"]  # e.g., is97b, is105


class FontAnomalyDetector:
    """Detect anomalies in fonts or font rendering."""

    def __init__(self,
                 rare_k_max: int = 3,  # cap for "rare by count"
                 ci_z: float = 1.64,  # 90% CI; use 1.96 for 95% if you want stricter
                 alpha: float = 0.8,  # position inside the gap (0..1); higher = more sensitive
                 image_anomaly_contamination: float = 0.01,
                 ignore_text_patterns: Optional[List[str]] = None,
                 context_markers: Optional[List[str]] = None,
                 ignore_only_in_header_footer: bool = True,
                 header_ratio: float = 0.05,  # top 20% of page
                 footer_start_ratio: float = 0.95):

        self.rare_k_max = rare_k_max
        self.ci_z = ci_z
        self.alpha = max(0.0, min(0.95, alpha))  # keep below 1 so k*+1 isn’t flagged
        self.image_anomaly_contamination = image_anomaly_contamination
        self.ignore_text_patterns = ignore_text_patterns or ITA_TEMPLATE_CODE_PATTERNS
        self.context_markers = context_markers or ITA_TEXT_MARKERS
        self.ignore_only_in_header_footer = ignore_only_in_header_footer
        self.header_ratio = header_ratio
        self.footer_start_ratio = footer_start_ratio
    # ------------------------------------------------------------------
    # High level API
    # ------------------------------------------------------------------
    def detect_with_file(self, ocr_output, file_path: str) -> List[Dict]:
        """Detect font anomalies, loading image from ``file_path`` if needed."""

        has_font = any(word.get("font") is not None for page in ocr_output for word in page)
        image = None
        if not has_font:
            image = self._load_image_for_detection(file_path)
        return self.detect(ocr_output, image=image)

    def detect(self, ocr_output: List[List[Dict]], image: Optional[np.ndarray] = None) -> List[Dict]:
        """Detect anomalies directly from OCR output and optional image."""

        words_with_pages: List[Dict] = []
        for page_num, page in enumerate(ocr_output, 1):
            for w in page:
                w = dict(w)
                w["page"] = page_num
                words_with_pages.append(w)

        doc_has_ctx = self._has_context_markers(words_with_pages)
        page_heights = self._page_max_y_map(words_with_pages)

        words_filtered: List[Dict] = [
            w for w in words_with_pages
            if not self._should_ignore_word(w, doc_has_ctx, page_heights.get(w["page"], 0.0))
        ]

        has_font = any(w.get("font") is not None for w in words_with_pages)
        if has_font:
            return self._detect_font_anomaly(words_filtered)
        if image is not None:
            return self._detect_image_anomaly(words_filtered, image)
        raise ValueError("No font info and no image provided for anomaly detection.")

    # ------------------------------------------------------------------
    # Font name based anomaly detection
    # ------------------------------------------------------------------
    def _binom_cdf_le(self, k: int, n: int, p: float) -> float:
        # Sum P(X=0..k) for X~Binom(n,p)
        s = 0.0
        for i in range(k + 1):
            s += math.comb(n, i) * (p ** i) * ((1 - p) ** (n - i))
        return s

    def _detect_font_anomaly(self, words: List[Dict]) -> List[Dict]:
        font_counts: Dict[str, int] = {}
        total_with_font = 0
        for w in words:
            f = w.get("font")
            if f:
                norm = self._normalize_font_name(f)
                font_counts[norm] = font_counts.get(norm, 0) + 1
                total_with_font += 1

        t_doc = self._dynamic_doc_threshold(total_with_font, font_counts)
        results: List[Dict] = []

        for w in words:
            f = w.get("font")
            if not f:
                continue
            norm = self._normalize_font_name(f)
            cnt = font_counts[norm]
            present = cnt / total_with_font
            ub = self._wilson_upper_bound(cnt, total_with_font, z=self.ci_z)

            if ub < t_doc:
                # Binomial-tail severity (higher = more anomalous)
                pval = self._binom_cdf_le(cnt, total_with_font, t_doc)  # P(X<=cnt | p=t_doc)
                severity = int(round(100 * (1 - pval)))
                results.append({
                    "text": w["text"],
                    "box": w["bbox"],
                    "page": w["page"],
                    "font": f,
                    "present": float(present),
                    "p_value": float(pval),
                    "score": severity,  # <-- use this for UI
                    "reason": f"count={cnt}, N={total_with_font}, UB={ub:.2%} < thr={t_doc:.2%}"
                })
        return results

    @staticmethod
    def _normalize_font_name(font: str) -> str:
        """Normalise a font name to reduce stylistic variants.

        Examples::

            'Arial-Bold' -> 'arial'
            'Helvetica+Italic' -> 'helvetica'
        """

        base = font.lower()
        base = re.sub(r"^[^+]+\+", "", base)  # drop subset prefix if present
        base = re.split(r"[-,]", base)[0]
        return base

    # ------------------------------------------------------------------
    # Image based anomaly detection
    # ------------------------------------------------------------------
    def _detect_image_anomaly(self, words: List[Dict], image: np.ndarray) -> List[Dict]:
        boxes = [self._xywh_from_bbox(w["bbox"]) for w in words]
        texts = [w["text"] for w in words]
        pages = [w["page"] for w in words]

        features = []
        for (x, y, w_, h_) in boxes:
            word_img = image[y : y + h_, x : x + w_]
            feats = self.extract_features(word_img, image, x, y, w_, h_)
            features.append(feats)

        features = np.array(features)
        scaler = StandardScaler()
        features_scaled = scaler.fit_transform(features)

        suspicious: List[Dict] = []
        if len(features_scaled) >= 10:
            # Use a more robust outlier detector when we have enough data.
            model = IsolationForest(
                contamination=self.image_anomaly_contamination, random_state=42
            )
            preds = model.fit_predict(features_scaled)
            scores = -model.decision_function(features_scaled)
            for i, (pred, score) in enumerate(zip(preds, scores)):
                if pred == -1:
                    suspicious.append(
                        {
                            "text": texts[i],
                            "box": words[i]["bbox"],
                            "page": pages[i],
                            "score": float(score),
                            "reason": "image feature anomaly",
                        }
                    )
            return suspicious

        # Fallback to a simple sigma rule for small samples.
        mean_vec = features_scaled.mean(axis=0)
        distances = np.linalg.norm(features_scaled - mean_vec, axis=1)
        threshold = distances.mean() + SIGMA_THRESHOLD * distances.std()
        for i, dist in enumerate(distances):
            if dist > threshold:
                suspicious.append(
                    {
                        "text": texts[i],
                        "box": words[i]["bbox"],
                        "page": pages[i],
                        "score": float(dist),
                        "reason": "image feature anomaly",
                    }
                )
        return suspicious

    def _has_context_markers(self, words: List[Dict]) -> bool:
        """Detect if document looks like an ITA letter (or any provided context markers)."""
        text = " ".join((w.get("text") or "") for w in words)
        return any(re.search(p, text, re.IGNORECASE) for p in self.context_markers)

    def _page_max_y_map(self, words: List[Dict]) -> Dict[int, float]:
        """Map page -> max y (approx page height in OCR coords)."""
        max_y: Dict[int, float] = {}
        for w in words:
            y1 = float(w.get("bbox", [0, 0, 0, 0])[3])
            p = int(w.get("page", 1))
            max_y[p] = max(y1, max_y.get(p, 0.0))
        return max_y

    def _is_in_header_footer(self, bbox, page_height: float) -> bool:
        """Heuristic header/footer location test using relative bands."""
        if not bbox or page_height <= 0:
            return False
        x0, y0, x1, y1 = bbox
        header_band = self.header_ratio * page_height
        footer_band_start = self.footer_start_ratio * page_height
        return (y1 <= header_band) or (y0 >= footer_band_start)

    def _should_ignore_word(self, w: Dict, has_ctx: bool, page_height: float) -> bool:
        """Return True if the word should be ignored (e.g., 'is97b' footer code)."""
        if not has_ctx:
            return False
        txt = (w.get("text") or "").strip()
        if not txt:
            return False
        if not any(re.search(p, txt, re.IGNORECASE) for p in self.ignore_text_patterns):
            return False
        if self.ignore_only_in_header_footer:
            return self._is_in_header_footer(w.get("bbox"), page_height)
        return True
    # ------------------------------------------------------------------
    # Feature extraction utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _wilson_upper_bound(count: int, total: int, z: float = 1.64) -> float:
        if total <= 0:
            return 0.0
        phat = count / total
        denom = 2 * (total + z * z)
        center = 2 * total * phat + z * z
        rad = z * math.sqrt(z * z + 4 * total * phat * (1 - phat))
        return (center + rad) / denom

    def _dynamic_doc_threshold(self, total_words: int, font_counts: Dict[str, int]) -> float:
        """
        Count-anchored per-doc threshold:
        - If there is a singleton font -> flag only singletons.
        - Else flag up to the least-common count, capped at rare_k_max (default 3).
        Implemented by placing the threshold between Wilson UBs of k* and k*+1.
        """
        if total_words <= 0 or not font_counts:
            return 1.0  # degenerate doc => nothing flagged downstream

        if len(font_counts) == 1:
            # Only one font in the doc -> nothing is "rare"
            return 0.0

        # k* = rarest observed count, but not above 3 (or rare_k_max)
        k_min = min(font_counts.values())
        k_star = min(self.rare_k_max, max(1, k_min))

        # Wilson UBs for k* and k*+1
        ub_k = self._wilson_upper_bound(k_star, total_words, z=self.ci_z)
        ub_next = self._wilson_upper_bound(min(k_star + 1, total_words), total_words, z=self.ci_z)

        # Numerical guard (shouldn't happen, but keep safe)
        if ub_next <= ub_k:
            ub_next = min(1.0, ub_k + 1.0 / max(1, total_words))

        # Place threshold inside the gap; alpha<1 keeps k*+1 un-flagged.
        t_doc = ub_k + self.alpha * (ub_next - ub_k)

        # Optional tiny clamp to avoid extremes on tiny/huge docs
        max_cap = 0.35  # or whatever you like; even 0.5 is fine
        t_doc = min(t_doc, max_cap)
        if t_doc <= ub_k:
            t_doc = ub_k + 1e-6
        return float(max(0.001, t_doc))


    @staticmethod
    def extract_features(word_img, full_img, x, y, w, h):
        if word_img.size == 0 or min(word_img.shape[:2]) < 2:
            word_img = np.zeros((32, 32, 3), dtype=np.uint8)

        gray = cv2.cvtColor(word_img, cv2.COLOR_BGR2GRAY)
        resized = cv2.resize(gray, (32, 32))

        mean_intensity = np.mean(resized)
        edges = cv2.Canny(resized, 50, 150)
        stroke_density = np.sum(edges) / (32 * 32)
        aspect_ratio = w / h if h > 0 else 1

        lbp = local_binary_pattern(resized, LBP_POINTS, LBP_RADIUS, method="uniform")
        (hist, _) = np.histogram(
            lbp.ravel(), bins=np.arange(0, LBP_POINTS + 3), range=(0, LBP_POINTS + 2)
        )
        hist = hist.astype("float")
        hist /= hist.sum() + 1e-6

        hog_features = hog(
            resized, orientations=9, pixels_per_cell=(8, 8), cells_per_block=(1, 1), feature_vector=True
        )

        pad = 3
        y0, y1 = max(0, y - pad), min(full_img.shape[0], y + h + pad)
        x0, x1 = max(0, x - pad), min(full_img.shape[1], x + w + pad)
        bg_patch = cv2.cvtColor(full_img[y0:y1, x0:x1], cv2.COLOR_BGR2GRAY)
        bg_mean, bg_var = np.mean(bg_patch), np.var(bg_patch)

        return np.hstack(
            [
                [mean_intensity, stroke_density, aspect_ratio, bg_mean, bg_var],
                hist,
                hog_features,
            ]
        )

    @staticmethod
    def _load_image_for_detection(file_path: str) -> Optional[np.ndarray]:
        """Load an image from ``file_path`` if it's an image/PDF."""

        ext = os.path.splitext(file_path)[1].lower()
        if ext in [".jpg", ".jpeg", ".png"]:
            return cv2.imread(file_path)
        if ext == ".pdf":
            doc = fitz.open(file_path)
            page = doc[0]
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
            image = cv2.imdecode(
                np.frombuffer(pix.tobytes("png"), np.uint8), cv2.IMREAD_COLOR
            )
            doc.close()
            return image
        return None

    @staticmethod
    def _xywh_from_bbox(bbox):
        x0, y0, x1, y1 = bbox
        return int(x0), int(y0), int(x1 - x0), int(y1 - y0)


