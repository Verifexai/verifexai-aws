from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import Any, Dict, List, Optional

from aws.common.config.config import client_config
from aws.common.models.check_result import CheckResult
from aws.common.models.evidence import Evidence
from aws.common.utilities.enums import Category, EvidenceType, Kind, Status
from aws.common.utilities.utils import _make_id, _now_iso


class BaseValidator(ABC):
    """Base class for text analysis validators."""

    def __init__(self, data: Dict[str, Any]):
        self.data = data

    # Helpers -----------------------------------------------------------------
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

    # Public ------------------------------------------------------------------
    @abstractmethod
    def validate_data(self) -> List[CheckResult]:
        """Run validations on ``self.data`` and return results."""
        raise NotImplementedError
