import os
from typing import Tuple
from PIL import Image
import fitz  # PyMuPDF

from aws.common.config.config import FileConfig

class ImageConverter:
    def __init__(self, max_dimension: int = 2000, dpi: int = 200) -> None:
        self.max_dimension = max_dimension
        self.dpi = dpi

    def convert_file(self, file_path: str) -> Tuple[str, int, int]:
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()
        os.makedirs(FileConfig.UPLOAD_IMAGE_FOLDER, exist_ok=True)
        base = os.path.splitext(os.path.basename(file_path))[0].replace(" ", "_")
        output_path = f"{FileConfig.UPLOAD_IMAGE_FOLDER}/{base}.jpg"

        if ext == ".pdf":
            return self._convert_pdf_to_jpg(file_path, output_path)
        if ext in {".png", ".jpg", ".jpeg", ".bmp"}:
            return self._convert_image_to_jpg(file_path, output_path)
        raise ValueError(f"Unsupported file type: {ext}")

    def _convert_pdf_to_jpg(self, pdf_path: str, output_path: str) -> Tuple[str, int, int]:
        size = os.path.getsize(pdf_path)
        if size == 0:
            raise ValueError("PDF file is empty")

        doc = fitz.open(pdf_path)
        page = doc.load_page(0)  # first page
        zoom = self.dpi / 72  # PDF default is 72 DPI
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat, alpha=False)

        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        if img.width > self.max_dimension or img.height > self.max_dimension:
            img = self._resize_image(img)
        img.save(output_path, "JPEG", quality=90, dpi=(self.dpi, self.dpi))
        return output_path, img.width, img.height

    def _convert_image_to_jpg(self, image_path: str, output_path: str) -> Tuple[str, int, int]:
        image = Image.open(image_path)
        if image.mode != "RGB":
            image = image.convert("RGB")
        if image.width > self.max_dimension or image.height > self.max_dimension:
            image = self._resize_image(image)
        image.save(output_path, "JPEG", quality=90)
        return output_path, image.width, image.height

    def _resize_image(self, image: Image.Image) -> Image.Image:
        width, height = image.size
        if width > height:
            ratio = self.max_dimension / width
        else:
            ratio = self.max_dimension / height
        new_size = (int(width * ratio), int(height * ratio))
        return image.resize(new_size, Image.LANCZOS)
