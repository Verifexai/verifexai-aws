"""Public interface for text analysis validations."""

from typing import Any, Dict, List

from aws.analyze_file.text_analysis.termination_certificate.termination_certificate_validator import (
    TerminationCertificateValidator,
)
from aws.analyze_file.text_analysis.tex_certificate.tax_certificate_validator import (
    TaxCertificateValidator,
)
from aws.common.models.check_result import CheckResult
from aws.common.utilities.enums import FileType

__all__ = ["text_analysis_check"]


def text_analysis_check(*, file_type: FileType, label_data: Dict[str, Any]) -> List[CheckResult]:
    """Run text analysis validations based on ``file_type``.

    Parameters
    ----------
    file_type:
        The type of document being processed.
    label_data:
        Structured data extracted by :class:`TextExtractor`.
    """

    if file_type == FileType.TaxCertificate:
        validator = TaxCertificateValidator(label_data)
        return validator.validate_data()
    if file_type == FileType.TerminationCertificate:
        validator = TerminationCertificateValidator(label_data)
        return validator.validate_data()
    return []

