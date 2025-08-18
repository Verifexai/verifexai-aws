import dspy
from typing import Literal

from aws.common.utilities.enums import FileType


class FileFilterSignature(dspy.Signature):
    image: dspy.Image = dspy.InputField(desc="Image of the document")
    file_type: FileType.to_literal() = dspy.OutputField(desc=f"File classification")
    # language: str = dspy.OutputField(desc="Detected language short code, for example: eng, heb, etc...")
    # country: Literal['ISRAEL', 'USA', 'OTHER'] = dspy.OutputField(desc="Detected the country origin of this document")

    @classmethod
    def get_instructions(cls) -> str:
        return f"""You are an AI document classifier. 
                Given a document image or OCR text, classify it into one of these known document types:
                {FileType.descriptions()}"""

