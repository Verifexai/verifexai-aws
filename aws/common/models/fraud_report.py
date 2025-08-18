from typing import List, Dict, Any
from pydantic import BaseModel

from aws.common.models.check_result import CheckResult
from aws.common.models.document_info import DocumentInfo
from aws.common.models.overall import Overall


class FraudReport(BaseModel):
    platform_version: str
    run_id: str
    document: DocumentInfo
    overall: Overall
    checks: List[CheckResult]
    meta: Dict[str, Any] = {}