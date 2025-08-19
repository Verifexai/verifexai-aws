"""Public interface for text analysis extraction and validation."""

from typing import Any, Dict, List, Optional

from .extractors import (
    EmploymentTerminationTextExtractor,
    TaxCertificateTextExtractor,
)
from .validators import run_validator
from aws.common.models.check_result import CheckResult
from aws.common.utilities.enums import FileType

__all__ = [
    "EmploymentTerminationTextExtractor",
    "TaxCertificateTextExtractor",
    "text_analysis_check",
    "text_analysis_extract",
    "run_validator",
]


def text_analysis_check(*, file_type: FileType, label_data: Dict[str, Any]) -> List[CheckResult]:
    """Run text analysis validations based on ``file_type``.

    Parameters
    ----------
    file_type:
        The type of document being processed.
    label_data:
        Structured data extracted by the appropriate TextExtractor subclass.
    """

    return run_validator(file_type=file_type, label_data=label_data)


def text_analysis_extract(
    local_file_path: str,
    file_type: FileType,
    pages_data: List[List[Dict]],
    bedrock_client: Optional[Any] = None,
) -> Dict[str, Any]:

    if file_type == FileType.TaxCertificate:
        extractor = TaxCertificateTextExtractor(bedrock_client=bedrock_client)
    elif file_type == FileType.TerminationCertificate:
        extractor = EmploymentTerminationTextExtractor(bedrock_client=bedrock_client)
    else:
        raise ValueError("Unsupported file type")
    return extractor.extract(local_file_path, pages_data)
