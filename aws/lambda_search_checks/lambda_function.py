import base64
import json
import os
from typing import Dict, List

import boto3
from boto3.dynamodb.conditions import Attr

from aws.common.utilities.logger_manager import LoggerManager

logger = LoggerManager.get_module_logger("SearchChecks")


def _search_checks(table, search_text: str) -> List[Dict[str, str]]:
    """Scan the table using DynamoDB filter expressions for the search text."""

    filter_expr = (
        Attr("doc_id").contains(search_text)
        | Attr("file_type").contains(search_text)
        | Attr("created_at").contains(search_text)
        | Attr("risk_level").contains(search_text)
        | Attr("checks").contains(search_text)
    )

    kwargs: Dict[str, any] = {
        "FilterExpression": filter_expr,
        "ProjectionExpression": "doc_id, file_type, created_at, risk_level",
    }

    results: List[Dict[str, str]] = []
    while True:
        response = table.scan(**kwargs)
        results.extend(response.get("Items", []))
        if "LastEvaluatedKey" not in response:
            break
        kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    return results


def lambda_handler(event, context):
    """Lambda entry point for searching document check results."""
    logger.info("Event received: %s", event)
    search_param = ""
    if "queryStringParameters" in event:
        params = event.get("queryStringParameters") or {}
        search_param = params.get("search", "")
    if not search_param and event.get("body"):
        body = event.get("body")
        if event.get("isBase64Encoded"):
            body = base64.b64decode(body).decode("utf-8")
        data = json.loads(body)
        search_param = data.get("search", "")
    if not search_param:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "missing search parameter"}),
        }

    table_name = os.getenv("DDB_CHECKS_TABLE", "document-check-results")
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    matches = _search_checks(table, search_param)
    return {"statusCode": 200, "body": json.dumps(matches)}
