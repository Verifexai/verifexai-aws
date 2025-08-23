from __future__ import annotations

from typing import Any, Dict, List, Tuple

from boto3.dynamodb.conditions import Attr, Key

from aws.common.config.config import client_config
from aws.common.models.check_result import CheckResult
from aws.common.models.evidence import Evidence
from aws.common.utilities.dynamodb_manager import DynamoDBManager
from aws.common.utilities.enums import (
    Category,
    EvidenceType,
    FileType,
    Kind,
)
from aws.common.utilities.logger_manager import LoggerManager, ANALYZE_FILE
from aws.common.utilities.utils import _make_id, _now_iso


class HistoryFileChecks:
    """Checks that rely on historical data stored in DynamoDB."""

    _FIELD_MAPPING: Dict[FileType, Tuple[str, ...]] = {
        FileType.TaxCertificate: ("worker_name", "worker_id"),
        FileType.TerminationCertificate: ("worker_name", "worker_id"),
    }

    def __init__(self, dynamodb: DynamoDBManager | None = None) -> None:
        self.dynamodb = dynamodb or DynamoDBManager()
        self.logger = LoggerManager.get_module_logger(ANALYZE_FILE)

    # ------------------------------------------------------------------
    def check_duplicate_file(
        self, *, file_type: FileType, label_data: Dict[str, Any]
    ) -> CheckResult:
        """Return a check result indicating whether the file is a duplicate."""

        self.logger.info("Checking for duplicate file of type %s", file_type)
        table = self.dynamodb.labels_table
        filter_expression = None

        for field, info in label_data.items():
            field_value = info.get("text") if isinstance(info, dict) else None
            if field_value is None:
                continue

            condition = Attr(f"labels.{field}.text").eq(field_value)
            filter_expression = (
                condition if filter_expression is None else filter_expression & condition
            )

        duplicate = None
        if filter_expression is not None:
            try:
                response = table.query(
                    KeyConditionExpression=Key("file_type").eq(file_type.value),
                    FilterExpression=filter_expression,
                    ProjectionExpression="doc_id",
                    Limit=1,
                )
                items = response.get("Items", [])
                duplicate = items[0] if items else None
                if duplicate:
                    self.logger.info("Duplicate document found: %s", duplicate.get("doc_id"))
                else:
                    self.logger.info("No duplicate document found")
            except Exception as exc:
                self.logger.error("Failed to query DynamoDB for duplicate check: %s", exc)
        else:
            self.logger.warning("Insufficient label data for duplicate check")

        score = 100 if duplicate else 0
        evidence: List[Evidence] = []
        description = "Exact duplicate found" if duplicate else "No duplicate found"

        if duplicate:
            evidence.append(
                Evidence(
                    type=EvidenceType.FIELD,
                    value={"doc_id": duplicate.get("doc_id")},
                )
            )

        return CheckResult(
            id=_make_id("ExactDuplicateCheck"),
            category=Category.HISTORICAL_PATTERNS,
            kind=Kind.EXACT_DUPLICATE,
            title="Duplicate Document Check",
            description=description,
            score=score,
            status=client_config.status_for(score),
            evidence=evidence,
            tags=[],
            timestamp=_now_iso(),
        )

    # ------------------------------------------------------------------
    def get_worker_history_files(
        self, *, file_type: FileType, label_data: Dict[str, Any]
    ) -> None:
        """Fetch historical files for a worker based on file type."""

        self.logger.info("Fetching worker history for file type %s", file_type)
        fields = self._FIELD_MAPPING.get(file_type)
        if not fields:
            self.logger.warning("No field mapping configured for file type %s", file_type)
            return None

        table = self.dynamodb.labels_table
        filter_expression = None

        for field in fields:
            field_value = label_data.get(field, {}).get("text")
            if field_value is None:
                self.logger.warning("Missing label data for field %s", field)
                return None

            condition = Attr(f"labels.{field}.text").eq(field_value)
            filter_expression = (
                condition if filter_expression is None else filter_expression & condition
            )

        try:
            response = table.query(
                KeyConditionExpression=Key("file_type").eq(file_type.value),
                FilterExpression=filter_expression,
                ProjectionExpression="doc_id, labels",
            )
            items = response.get("Items", [])
            self.logger.info("Found %d historical files", len(items))
        except Exception as exc:
            self.logger.error("Failed to query DynamoDB for worker history: %s", exc)
            items = []

        for _item in items:
            # Placeholder for future validation logic
            pass

        return None
