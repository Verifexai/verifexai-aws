import base64
import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, Union

import boto3
from aws.analyze_file.file_processor import download_file_from_s3
from aws.analyze_file.text_analysis import text_analysis_check
from aws.analyze_file.text_analysis.text_extractor import TextExtractor
from aws.analyze_file.font_anomalies import font_anomalies_check
from aws.analyze_file.metadata import analyze_metadata_check
from aws.analyze_file.OCR.ocr_processor import OCRProcessor
from aws.common.config.config import BEDROCK_REGION, FileConfig
from aws.common.models.check_result import CheckResult, CheckOutput
from aws.common.models.document_info import DocumentInfo
from aws.common.utilities.enums import FileType
from aws.common.utilities.logger_manager import LoggerManager, ANALYZE_FILE
from aws.common.utilities.utils import _now_iso, _create_fraud_report, _get_parent_folder_from_key
from aws.common.utilities.dynamodb_manager import DynamoDBManager

ocr_processor = OCRProcessor()
bedrock = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)
text_extractor = TextExtractor(bedrock_client=bedrock)
logger = LoggerManager.get_module_logger(ANALYZE_FILE)
dynamodb_manager = DynamoDBManager()


def _run_checks(local_file_path: str, pages_data, label_data, file_type: FileType) -> List[CheckResult]:
    """Run all checks concurrently and return a flat list of CheckResults."""
    check_functions: List[Callable[[], CheckOutput]] = [
        lambda: font_anomalies_check(local_file_path=local_file_path, pages_data=pages_data, bedrock=bedrock),
        lambda: analyze_metadata_check(local_file_path,label_data=label_data, file_type=file_type),
        lambda: text_analysis_check(file_type=file_type, label_data=label_data)
    ]
    checks: List[CheckResult] = []
    with ThreadPoolExecutor(max_workers=len(check_functions)) as executor:
        futures = [executor.submit(fn) for fn in check_functions]
        for future in as_completed(futures):
            result = future.result()
            if isinstance(result, list):
                checks.extend(result)
            elif result:
                checks.append(result)
    return checks


def _process_record(
    local_file_path: str,
    file_name: uuid.UUID,
    file_ext: str,
    file_type: FileType,
    source: str,
    s3_key: str,
) -> None:
    """Run extraction, checks and logging for a given file."""
    # Extract OCR information from file
    pages_data = ocr_processor.extract(local_file_path)
    # Extract structured text fields
    label_data = text_extractor.extract(local_file_path, file_type, pages_data)

    # Persist label data
    label_item = dict(label_data)
    label_item["file_path"] = s3_key
    dynamodb_manager.save_labels(
        file_type=file_type.value,
        doc_id=str(file_name),
        s3_path=s3_key,
        labels=label_item,
    )

    # Checks
    checks = _run_checks(local_file_path, pages_data, label_data, file_type)
    fraud_report = _create_fraud_report(
        checks=checks,
        documentInfo=DocumentInfo(
            doc_id=str(file_name),
            source=source,
            mime_type=file_ext,
            num_pages=len(pages_data),
            created_at=_now_iso(),
        ),
    )
    dynamodb_manager.save_check_results(
        file_type=file_type.value,
        doc_id=str(file_name),
        fraud_report_json=fraud_report.model_dump_json(),
    )
    logger.info("Fraud report: %s", fraud_report.model_dump_json())


def _process_s3_record(record: dict) -> None:
    """Process an individual S3 record and run checks."""
    s3_record = record.get("s3", {})
    bucket_name = s3_record["bucket"]["name"]
    original_key = s3_record["object"]["key"]

    parent_folder = _get_parent_folder_from_key(original_key)
    file_type = FileType.from_parent_folder(parent_folder)
    logger.info("Detected file type: %s", file_type.value)

    # Load file into Lambda local file system
    local_file_path, file_name, file_ext = download_file_from_s3(bucket_name, original_key)
    _process_record(local_file_path, file_name, file_ext, file_type, "s3", original_key)


def _process_api_record(event: dict) -> None:
    """Process a request coming from API Gateway.

    Expected body structure:
        {
            "file_name": "example.pdf",
            "file_content": "<base64 encoded content>",
            "file_type": "tax-assessor-certificate"  # folder name
        }
    """
    body = event.get("body", "{}")
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")

    data = json.loads(body)
    file_content_b64 = data["file_content"]
    original_name = data.get("file_name", "uploaded_file")
    parent_folder = data.get("file_type")

    file_type = FileType.from_parent_folder(parent_folder)
    file_ext = os.path.splitext(original_name)[1]

    file_uuid = uuid.uuid4()
    safe_file_name = f"{file_uuid}{file_ext}"
    os.makedirs(FileConfig.TEMP_FILE_PATH, exist_ok=True)
    local_file_path = os.path.join(FileConfig.TEMP_FILE_PATH, safe_file_name)

    with open(local_file_path, "wb") as f:
        f.write(base64.b64decode(file_content_b64))

    # Save to S3
    s3_key = f"{FileConfig.RAW_PREFIX}{parent_folder}/{safe_file_name}" if parent_folder else safe_file_name
    s3_client = boto3.client("s3")
    s3_client.upload_file(local_file_path, FileConfig.S3_BUCKET, s3_key)
    logger.info("Saved file to s3://%s/%s", FileConfig.S3_BUCKET, s3_key)

    _process_record(local_file_path, file_uuid, file_ext, file_type, "api", s3_key)

def lambda_handler(event, context):
    """Lambda entry point for analyzing files."""
    logger.info("Event received: %s", event)

    try:
        if "Records" in event:
            for record in event.get("Records", []):
                _process_s3_record(record)
        else:
            _process_api_record(event)
    except Exception as exc:
        logger.error("Lambda processing failed: %s", exc, exc_info=True)
        raise

    return {
        "statusCode": 200
    }


if __name__ == '__main__':
    pass
    # start_time = time.time()
    #
    # aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    # aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    #
    # bedrock = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION,aws_access_key_id=aws_access_key_id,
    #                        aws_secret_access_key=aws_secret_access_key)
    # ocr_processor = OCRProcessor()
    # text_extractor = TextExtractor(bedrock_client=bedrock)
    # local_file_path = "test3.pdf"
    # filetype = FileType.TerminationCertificate
    # pages_data = ocr_processor.extract(local_file_path)
    # label_data = text_extractor.extract(file_type=filetype, pages_data=pages_data, file_path=local_file_path)
    # checks = _run_checks(local_file_path, pages_data, label_data, filetype)
    # fraud_report = _create_fraud_report(
    #     checks=checks,
    #     documentInfo=DocumentInfo(
    #         doc_id=str("Gsdg"),
    #         source="s3",
    #         mime_type=".pdf",
    #         num_pages=len(pages_data),
    #         created_at=_now_iso(),
    #     ),
    # )
    # end_time = time.time()
    # execution_time = end_time - start_time
    # print(f"took {execution_time:.4f} seconds to execute")
    #
    # print("Fraud report: %s", fraud_report.model_dump_json())
    #

