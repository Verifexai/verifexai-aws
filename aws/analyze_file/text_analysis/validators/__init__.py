"""Validator utilities and factory for text analysis."""

from typing import Any, Dict, List

from aws.common.models.check_result import CheckResult
from aws.common.utilities.enums import FileType

from .base_validator import BaseValidator
from .tax_certificate_validator import TaxCertificateValidator
from .termination_certificate_validator import TerminationCertificateValidator

__all__ = [
    "BaseValidator",
    "run_validator",
    "TaxCertificateValidator",
    "TerminationCertificateValidator",
]


def run_validator(*, file_type: FileType, label_data: Dict[str, Any]) -> List[CheckResult]:
    """Instantiate and execute validator based on ``file_type``."""
    if file_type == FileType.TaxCertificate:
        return TaxCertificateValidator(label_data).validate_data()
    if file_type == FileType.TerminationCertificate:
        return TerminationCertificateValidator(label_data).validate_data()
    return []
