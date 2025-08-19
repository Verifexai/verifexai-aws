import json
import os
from typing import Any, Dict

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

    def __init__(self, region_name: str | None = None) -> None:
        aws_access_kcey_id = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

        resource = boto3.resource("dynamodb", region_name=region_name, aws_access_key_id=aws_access_kcey_id,
                                  aws_secret_access_key=aws_secret_access_key)
        labels_table_name = os.getenv("DDB_LABELS_TABLE", "document-labels")
        checks_table_name = os.getenv("DDB_CHECKS_TABLE", "document-check-results")
        self.labels_table = resource.Table(labels_table_name)
        self.checks_table = resource.Table(checks_table_name)
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
            Document type used as the partition key.
        doc_id: str
            Unique document identifier (sort key).
        s3_path: str
            Full S3 key of the uploaded file.
        labels: Dict[str, Any]
            Extracted label data to persist.
        """
        item = {
            "file_type": file_type,
            "doc_id": doc_id,
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
        """Persist fraud report JSON in DynamoDB."""
        item = {
            "file_type": file_type,
            "doc_id": doc_id,
            "s3_path": s3_path,
            "bucket": bucket,
            "checks": convert_floats(json.loads(fraud_report_json)),
        }
        try:
            self.checks_table.put_item(Item=item)
        except ClientError as exc:
            _DYNAMODB_LOGGER.error("Failed to store check results: %s", exc)
