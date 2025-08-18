import os
import uuid
import boto3
import urllib.parse
from boto3.s3.transfer import TransferConfig
from typing import List
from aws.common.config.config import FileConfig

s3_client = boto3.client('s3')

def download_file_from_s3(bucket_name: str, original_key: str) -> str:
    """
    Downloads a file from S3 to a local path with a UUID-based filename.
    Returns: (local_path, decoded_key)
    """
    decoded_key = urllib.parse.unquote_plus(original_key)
    print(f"[DOWNLOAD] Decoded S3 key: {decoded_key}")

    file_ext = os.path.splitext(decoded_key)[1]
    file_name = uuid.uuid4()
    safe_file_name = f"{file_name}{file_ext}"

    os.makedirs(FileConfig.TEMP_FILE_PATH, exist_ok=True)
    local_path = os.path.join(FileConfig.TEMP_FILE_PATH, safe_file_name)

    try:
        print(f"[DOWNLOAD] Downloading s3://{bucket_name}/{decoded_key} to {local_path}")
        transfer_config = TransferConfig(
            multipart_threshold=25 * 1024 * 1024,
            max_concurrency=20,
            use_threads=True,
        )
        s3_client.download_file(bucket_name, decoded_key, local_path, Config=transfer_config)
        print(f"[SUCCESS] Downloaded file to: {local_path}")
    except Exception as e:
        print(f"[ERROR] Download failed: {e}")
        raise

    return local_path, file_name, file_ext


def upload_files_to_s3(file_paths: List[str]) -> List[dict]:
    """
    Uploads a list of local file paths to S3 with safe UUID-based keys.
    Returns: List of metadata dicts per file uploaded
    """
    uploaded_files = []

    for path in file_paths:
        if not os.path.isfile(path):
            print(f"[SKIP] File does not exist: {path}")
            continue

        filename = os.path.basename(path)
        extension = os.path.splitext(filename)[1]
        processed_key = f"{FileConfig.EXTRACT_PREFIX}{uuid.uuid4()}{extension}"

        try:
            print(f"[UPLOAD] Uploading {path} to s3://{FileConfig.S3_BUCKET}/{processed_key}")
            s3_client.upload_file(path, FileConfig.S3_BUCKET, processed_key)
            print(f"[SUCCESS] Uploaded to: {processed_key}")

            uploaded_files.append({
                'local_path': path,
                'image_path': processed_key,
            })
        except Exception as e:
            print(f"[ERROR] Upload failed for {path}: {e}")
            raise

    return uploaded_files
