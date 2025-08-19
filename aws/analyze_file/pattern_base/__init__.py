"""Pattern-based historical checks for document analysis."""
from typing import List, Dict, Any

from .history_file_checks import HistoryFileChecks
from ...common.models.check_result import CheckResult
from ...common.utilities.dynamodb_manager import DynamoDBManager
from ...common.utilities.enums import FileType

__all__ = ["HistoryFileChecks"]


def pattern_base_check(file_type: FileType, label_data: Dict[str, Any], dynamodb: DynamoDBManager):
    checks: List[CheckResult] = []

    history_check = HistoryFileChecks(dynamodb=dynamodb)
    check_duplicate = history_check.check_duplicate_file(file_type=file_type, label_data=label_data)
    if check_duplicate:
        checks.append(check_duplicate)

    check_pattern = history_check.get_worker_history_files(file_type=file_type, label_data=label_data)
    if check_pattern:
        checks.append(check_pattern)

    return checks
