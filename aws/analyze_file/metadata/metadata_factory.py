import mimetypes
import os
from typing import Optional

from .metadata_base import MetadataBaseScorer
from .pdf_metadata_scorer import PDFMetadataScorer
from .image_metadata_scorer import ImageMetadataScorer


class MetadataFactory:
    """Create appropriate metadata scorer based on file type."""

    @staticmethod
    def get_metadata_scorer(file_path: str) -> Optional[MetadataBaseScorer]:
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        mime_type, _ = mimetypes.guess_type(file_path)
        ext = os.path.splitext(file_path)[1].lower()

        if ext == ".pdf" or mime_type == "application/pdf":
            return PDFMetadataScorer(file_path)

        if ext in {".jpg", ".jpeg", ".png", ".bmp"} or (mime_type and mime_type.startswith("image/")):
            return ImageMetadataScorer(file_path)

        return None
