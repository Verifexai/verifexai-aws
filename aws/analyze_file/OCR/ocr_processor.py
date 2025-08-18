import re
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor
import fitz  # PyMuPDF
import unicodedata
from PIL import Image
import pytesseract
from aws.common.utilities.logger_manager import LoggerManager, ANALYZE_FILE
import shutil
from bidi.algorithm import get_display

_tesseract_path = shutil.which("tesseract")
if _tesseract_path:
    pytesseract.pytesseract.tesseract_cmd = _tesseract_path

class OCRProcessor:
    """Extract text information from a file using PDF parsing or OCR."""

    def __init__(self, lang: str = 'heb+eng', max_workers: Optional[int] = None) -> None:
        self.lang = lang
        self.logger = LoggerManager.get_module_logger(ANALYZE_FILE)
        self.max_workers = max_workers
        self.HEBREW_RE = re.compile(r'[\u0590-\u05FF]')

    def extract(self, file_path: str) -> List[List[Dict]]:
        """Extract text from a file. Returns a list of pages with dict entries."""
        try:
            if self._is_pdf(file_path):
                self.logger.info('File identified as PDF: %s', file_path)
                pdf_result = self._extract_from_pdf(file_path)
                if any(pdf_result):
                    return pdf_result
                self.logger.info('No text found in PDF, falling back to OCR.')
                return self._extract_via_ocr_from_pdf(file_path)
            self.logger.info('File is not PDF, performing OCR.')
            return [self._extract_from_image(Image.open(file_path))]
        except Exception as exc:
            self.logger.error('Failed to extract text from %s: %s', file_path, exc, exc_info=True)
            return []


    @staticmethod
    def _is_pdf(file_path: str) -> bool:
        return file_path.lower().endswith('.pdf')

    def fix_bidi(self, text: str) -> str:
        # normalize first to clean up any funky encodings
        text = unicodedata.normalize('NFKC', text)
        # only run BiDi if there's Hebrew in the span
        return get_display(text, base_dir='R') if self.HEBREW_RE.search(text) else text

    def _extract_from_pdf(self, file_path: str) -> list[list[dict]]:
        """Extract embedded text directly from a PDF, with RTL fix."""
        try:
            import fitz
            with fitz.open(file_path) as doc:
                def _process_page(page) -> list[dict]:
                    page_entries: list[dict] = []
                    # sort=True improves block/line ordering (not RTL itself)
                    text_dict = page.get_text('dict', sort=True)  # see note below
                    for block in text_dict.get('blocks', []):
                        for line in block.get('lines', []):
                            for span in line.get('spans', []):
                                text = span.get('text', '').strip()
                                if len(text) < 3:
                                    continue
                                fixed = self.fix_bidi(text)
                                entry = {
                                    'text': fixed,  # display-friendly
                                    'text_raw': text,  # keep original too
                                    'bbox': list(span.get('bbox')),
                                    'font': span.get('font'),
                                }
                                page_entries.append(entry)
                    return page_entries

                # ⚠️ PyMuPDF and threads don’t play well. Prefer multiprocessing.
                # If you keep threads, weirdness can be intermittent.
                pages = [_process_page(doc[i]) for i in range(len(doc))]
        except Exception as exc:
            self.logger.error('PDF text extraction error: %s', exc, exc_info=True)
            return []
        return pages

    def _extract_via_ocr_from_pdf(self, file_path: str) -> List[List[Dict]]:
        try:
            with fitz.open(file_path) as doc:
                def _process_page(page) -> List[Dict]:
                    pix = page.get_pixmap()
                    image = Image.frombytes('RGB', [pix.width, pix.height], pix.samples)
                    return self._extract_from_image(image)

                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    pages = list(executor.map(_process_page, doc))
        except Exception as exc:
            self.logger.error('OCR from PDF failed: %s', exc, exc_info=True)
            return []
        return pages

    def _extract_from_image(self, image: Image.Image) -> List[Dict]:
        entries: List[Dict] = []
        try:
            data = pytesseract.image_to_data(image, lang=self.lang, output_type=pytesseract.Output.DICT)
            n_items = len(data['text'])
            for i in range(n_items):
                text = str(data['text'][i]).strip()
                try:
                    conf = int(data['conf'][i])
                except Exception:
                    conf = -1
                if len(text) < 3 or conf <= -1:
                    continue
                bbox = [
                    data['left'][i],
                    data['top'][i],
                    data['left'][i] + data['width'][i],
                    data['top'][i] + data['height'][i],
                ]
                entries.append({'text': text, 'bbox': bbox, 'font': None})
        except Exception as exc:
            self.logger.error('Image OCR failed: %s', exc, exc_info=True)
        return entries
