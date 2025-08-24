
import os
import json
import base64
from typing import Any, Dict, Optional, List

import boto3
from boto3.dynamodb.conditions import Attr

from aws.common.config.config import BEDROCK_REGION
from aws.common.utilities.logger_manager import LoggerManager

_logger = LoggerManager.get_module_logger("GetCheckResults")

_dynamodb = boto3.resource("dynamodb",region_name=BEDROCK_REGION)

_table_name = os.getenv("DDB_CHECKS_TABLE", "document-check-results")
_table = _dynamodb.Table(_table_name)

_DEFAULT_LIMIT = 10


def _decode_pagination_token(token: Optional[str]) -> Optional[Dict[str, Any]]:
    """Decode a base64-encoded pagination token."""
    if not token:
        return None
    try:
        return json.loads(base64.b64decode(token).decode("utf-8"))
    except Exception as exc:  # pragma: no cover - defensive
        _logger.warning("Failed to decode pagination token: %s", exc)
        return None


def _encode_pagination_token(key: Dict[str, Any]) -> str:
    """Encode LastEvaluatedKey as base64 string."""
    return base64.b64encode(json.dumps(key).encode("utf-8")).decode("utf-8")


def lambda_handler(event, context):  # pragma: no cover - AWS entrypoint
    """Lambda entry point for fetching document check results.

    Parameters
    ----------
    event: dict
        API Gateway event with optional query parameters:
        - limit: number of items to return
        - page_token: pagination token from previous call
        - risk_level: filter results by risk_level
        - sort_by: field to sort by ("risk_level" or "created_at")
        - sort_order: "asc" or "desc"
    context: LambdaContext
        AWS Lambda context (unused)
    """
    params = event.get("queryStringParameters") or {}
    limit = int(params.get("limit", _DEFAULT_LIMIT))
    risk_level = params.get("risk_level")
    sort_by = params.get("sort_by", "created_at")
    sort_order = params.get("sort_order", "desc").lower()
    page_token = params.get("page_token")

    scan_kwargs: Dict[str, Any] = {
        "Limit": limit,
        "ProjectionExpression": "doc_id, file_type, created_at, risk_level",
    }
    exclusive_start_key = _decode_pagination_token(page_token)
    if exclusive_start_key:
        scan_kwargs["ExclusiveStartKey"] = exclusive_start_key
    if risk_level:
        scan_kwargs["FilterExpression"] = Attr("risk_level").gt(risk_level)

    response = _table.scan(**scan_kwargs)
    items: List[Dict[str, Any]] = response.get("Items", [])

    reverse = sort_order == "desc"
    if sort_by in {"risk_level", "created_at"}:
        items.sort(key=lambda x: x.get(sort_by, ""), reverse=reverse)

    last_key = response.get("LastEvaluatedKey")
    body = {
        "items": items,
        "next_page_token": _encode_pagination_token(last_key) if last_key else None,
    }
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": body,
    }