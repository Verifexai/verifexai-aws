"""Validation routines for tax certificate extraction output."""

from __future__ import annotations

from datetime import datetime, date
from typing import Dict, List, Optional, Any
from aws.analyze_file.text_analysis.tex_certificate.withholding_helper import _norm_number, \
    _load_known_deduction_numbers
from aws.common.config.config import client_config
from aws.common.models.check_result import CheckResult
from aws.common.models.evidence import Evidence
from aws.common.utilities.enums import (
    Category,
    EvidenceType,
    Kind,
    Status,
    TaxCertificateField,
)
from aws.common.utilities.hebrew_date_parser import HebrewDateUtil
from aws.common.utilities.utils import _make_id, _now_iso

class TaxCertificateValidator:
    """Run field validations on extracted tax certificate data."""

    def __init__(self, data: Dict[str, Any]):
        self.data = data

    # Helper -----------------------------------------------------------------
    def _evidence(self, field: str) -> Evidence:
        info = self.data.get(field) or {}
        value = info.get("text") if isinstance(info, dict) else info
        bbox = info.get("bbox") if isinstance(info, dict) else None
        return Evidence(type=EvidenceType.FIELD, value={field: value}, bbox=bbox)

    def _date_from_field(self, field: str) -> Optional[date]:
        info = self.data.get(field) or {}
        value = info.get("text") if isinstance(info, dict) else info
        if not value:
            return None
        try:
            return datetime.strptime(str(value), "%Y-%m-%d").date()
        except Exception:
            return None

    def validate_data(self) -> List[CheckResult]:
        checks: List[CheckResult] = []
        checks.append(self._check_hebrew_english_dates())
        checks.append(self._check_departure_before_document())
        checks.append(self._check_deduction_file_number())
        checks.append(self._check_tax_officer_blacklist())
        return checks

    # Checks -----------------------------------------------------------------
    def _check_hebrew_english_dates(self) -> CheckResult:
        eng_date = self._date_from_field(TaxCertificateField.DOCUMENT_DATE.value)
        heb_raw = self.data.get(TaxCertificateField.DOCUMENT_DATE_HEBREW.value, {}).get("text")
        heb_date = HebrewDateUtil.parse(heb_raw) if heb_raw else None
        valid = eng_date and heb_date and eng_date == heb_date
        score = 0 if valid else 100
        evidence = [
            self._evidence(TaxCertificateField.DOCUMENT_DATE.value),
            self._evidence(TaxCertificateField.DOCUMENT_DATE_HEBREW.value),
        ]
        return self._build_result(
            "HebrewEnglishDateMatch",
            Kind.DATE_CONSISTENCY,
            "Hebrew and English dates match",
            "Dates are consistent" if valid else "Mismatch between Hebrew and English dates",
            score,
            evidence,
        )

    def _check_departure_before_document(self) -> CheckResult:
        doc_date = self._date_from_field(TaxCertificateField.DOCUMENT_DATE.value)
        departure_date = self._date_from_field(TaxCertificateField.JOB_DEPARTURE_DATE.value)
        valid = doc_date and departure_date and departure_date <= doc_date
        score = 0 if valid else 90
        evidence = [
            self._evidence(TaxCertificateField.DOCUMENT_DATE.value),
            self._evidence(TaxCertificateField.JOB_DEPARTURE_DATE.value),
        ]
        return self._build_result(
            "DepartureBeforeDocument",
            Kind.DATE_CONSISTENCY,
            "Departure before document date",
            "Job departure precedes document date" if valid else "Departure date after document date",
            score,
            evidence,
        )

    def _check_deduction_file_number(self) -> CheckResult:
        raw = (self.data.get(TaxCertificateField.DEDUCTION_FILE_NUMBER.value) or {}).get("text")
        normalized = _norm_number(raw)

        known_set, number_to_company = _load_known_deduction_numbers()
        valid = normalized in known_set if normalized else False
        evidence = [self._evidence(TaxCertificateField.DEDUCTION_FILE_NUMBER.value)]
        if valid:
            label = number_to_company.get(normalized)
            if label:
                evidence.append(Evidence(type=EvidenceType.METRIC, value={"matched_company": label}))

        score = 0 if valid else 70
        return self._build_result(
            "DeductionFileNumber",
            Kind.REGISTRY_MATCH,
            "Deduction file number validity",
            "Known deduction file number" if valid else "Unknown deduction file number",
            score,
            evidence,
        )

    def _check_tax_officer_blacklist(self) -> CheckResult:
        name = (self.data.get(TaxCertificateField.TAX_OFFICER_NAME.value) or {}).get("text")
        blacklist = set(client_config.tax_officer_blacklist)
        valid = name not in blacklist if name else True
        score = 0 if valid else 100
        evidence = [self._evidence(TaxCertificateField.TAX_OFFICER_NAME.value)]
        return self._build_result(
            "TaxOfficerBlacklist",
            Kind.REGISTRY_MATCH,
            "Tax officer not blacklisted",
            "Name not in blacklist" if valid else "Tax officer is blacklisted",
            score,
            evidence,
        )


    # Builder ----------------------------------------------------------------
    def _build_result(
        self,
        id_suffix: str,
        kind: Kind,
        title: str,
        description: str,
        score: int,
        evidence: List[Evidence],
        *,
        status: Optional[Status] = None,
    ) -> CheckResult:
        return CheckResult(
            id=_make_id(id_suffix),
            category=Category.CROSS_SOURCE_VERIFICATION,
            kind=kind,
            title=title,
            description=description,
            score=score,
            status=status or client_config.status_for(score),
            evidence=evidence,
            tags=[],
            timestamp=_now_iso(),
        )