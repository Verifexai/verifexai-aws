"""Validation routines for termination certificate extraction output."""

from __future__ import annotations

from typing import Any, Dict, List

from aws.common.models.check_result import CheckResult
from aws.common.utilities.enums import Kind, TerminationCertificateField

from .base_validator import BaseValidator


class TerminationCertificateValidator(BaseValidator):
    """Run field validations on extracted termination certificate data."""

    def __init__(self, data: Dict[str, Any]):
        super().__init__(data)

    def validate_data(self) -> List[CheckResult]:
        checks: List[CheckResult] = []
        checks.append(self._check_departure_before_document())
        checks.append(self._check_start_before_departure())
        checks.append(self._check_worker_id_format())
        return checks

    # Checks -----------------------------------------------------------------
    def _check_departure_before_document(self) -> CheckResult:
        doc_date = self._date_from_field(TerminationCertificateField.DOCUMENT_DATE.value)
        departure = self._date_from_field(TerminationCertificateField.JOB_DEPARTURE_DATE.value)
        valid = doc_date and departure and departure <= doc_date
        score = 0 if valid else 90
        evidence = [
            self._evidence(TerminationCertificateField.DOCUMENT_DATE.value),
            self._evidence(TerminationCertificateField.JOB_DEPARTURE_DATE.value),
        ]
        return self._build_result(
            "DepartureBeforeDocument",
            Kind.DATE_CONSISTENCY,
            "Departure before document date",
            "Job departure precedes document date" if valid else "Departure date after document date",
            score,
            evidence,
        )

    def _check_start_before_departure(self) -> CheckResult:
        """
        Rules:
          - If there isn't a start date → valid, score 20.
          - If any date missing (with start present → i.e., departure missing) → issue, score 80.
          - If start <= departure → issue, score 90.
          - Otherwise (start > departure) → valid, score 0.
        Note: The first rule takes precedence if both dates are missing.
        """
        start = self._date_from_field(TerminationCertificateField.JOB_START_DATE.value)
        departure = self._date_from_field(TerminationCertificateField.JOB_DEPARTURE_DATE.value)

        if start is None:
            score = 20
            details = "Start date missing; treated as acceptable"
        elif departure is None:
            score = 80
            details = "Departure date missing"
        elif start >= departure:
            score = 0
            details = "Start is on/after departure"
        else:
            score = 0
            details = "Start is before departure"

        evidence = [
            self._evidence(TerminationCertificateField.JOB_START_DATE.value),
            self._evidence(TerminationCertificateField.JOB_DEPARTURE_DATE.value),
        ]

        return self._build_result(
            "StartBeforeDeparture",
            Kind.DATE_CONSISTENCY,
            "Start date vs departure date",
            details,
            score,
            evidence,
        )

    def _check_worker_id_format(self) -> CheckResult:
        import re

        raw = (self.data.get(TerminationCertificateField.WORKER_ID.value) or {}).get("text")
        valid = bool(re.fullmatch(r"\d{9}", str(raw or "")))
        score = 0 if valid else 60
        evidence = [self._evidence(TerminationCertificateField.WORKER_ID.value)]
        return self._build_result(
            "WorkerIDFormat",
            Kind.ID_FORMAT,
            "Worker ID format",
            "Worker ID is 9 digits" if valid else "Worker ID is not 9 digits",
            score,
            evidence,
        )
