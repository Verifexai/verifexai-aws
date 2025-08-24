import json
import os
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

from aws.common.utilities.logger_manager import LoggerManager
from aws.common.utilities.utils import convert_floats

_DYNAMODB_LOGGER = LoggerManager.get_module_logger("DynamoDBManager")


class DynamoDBManager:
    """Minimal DynamoDB helper for persisting analysis results.

    Attributes
    ----------
    labels_table : boto3.resources.factory.dynamodb.Table
        Table for document labels.
    checks_table : boto3.resources.factory.dynamodb.Table
        Table for document check results.
    """

    def __init__(self, dynamodb:Optional[Any]) -> None:
        labels_table_name = os.getenv("DDB_LABELS_TABLE", "document-labels")
        checks_table_name = os.getenv("DDB_CHECKS_TABLE", "document-check-results")
        self.labels_table = dynamodb.Table(labels_table_name)
        self.checks_table = dynamodb.Table(checks_table_name)
        _DYNAMODB_LOGGER.info(
            "Initialized DynamoDB tables: labels=%s, checks=%s",
            labels_table_name,
            checks_table_name,
        )

    def save_labels(
            self,
            *,
            file_type: str,
            doc_id: str,
            s3_path: str,
            bucket: str,
            labels: Dict[str, Any],
    ) -> None:
        """Persist extracted label data in DynamoDB.

        Parameters
        ----------
        file_type: str
            Document type used as the sort key.
        doc_id: str
            Unique document identifier (partition key).
        s3_path: str
            Full S3 key of the uploaded file.
        labels: Dict[str, Any]
            Extracted label data to persist.
        """
        item = {
            "doc_id": doc_id,
            "file_type": file_type,
            "s3_path": s3_path,
            "bucket": bucket,
            "labels": convert_floats(labels),
        }
        try:
            self.labels_table.put_item(Item=item)
        except ClientError as exc:
            _DYNAMODB_LOGGER.error("Failed to store labels: %s", exc)

    def save_check_results(
            self, *,
            file_type: str,
            doc_id: str,
            s3_path: str,
            bucket: str,
            fraud_report_json: str
    ) -> None:
        """Persist fraud report JSON in DynamoDB following the doc_id/file_type schema."""
        report_dict = json.loads(fraud_report_json)
        item = {
            "doc_id": doc_id,
            "file_type": file_type,
            "risk_level": report_dict.get("overall", {}).get("risk_score"),
            "created_at": report_dict.get("document", {}).get("created_at"),
            "s3_path": s3_path,
            "bucket": bucket,
            "checks": convert_floats(report_dict),
        }
        try:
            self.checks_table.put_item(Item=item)
        except ClientError as exc:
            _DYNAMODB_LOGGER.error("Failed to store check results: %s", exc)
