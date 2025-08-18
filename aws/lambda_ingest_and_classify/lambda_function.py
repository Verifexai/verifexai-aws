import os
import boto3
import urllib.parse
import uuid
from aws.common.config.config import FileConfig
from aws.lambda_ingest_and_classify.analyze_classify import analyze_file

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    print("Event received:", event)

    # 1️Extract bucket and object key
    record = event['detail']
    bucket_name = record['bucket']['name']
    original_key = record['object']['key']

    # Decode S3 key (handles Hebrew and spaces)
    decoded_key = urllib.parse.unquote_plus(original_key)
    print(f"Processing S3 object: {bucket_name}/{decoded_key}")

    # 2️Generate safe filename (UUID + original extension)
    file_ext = os.path.splitext(decoded_key)[1]  # e.g. ".pdf"
    safe_file_name = f"{uuid.uuid4()}{file_ext}"

    # Local temp path
    os.makedirs(FileConfig.TEMP_FILE_PATH, exist_ok=True)
    file_path = os.path.join(FileConfig.TEMP_FILE_PATH, safe_file_name)

    # 3️Download file from S3
    try:
        s3_client.download_file(bucket_name, decoded_key, file_path)
        print(f"File downloaded to {file_path}")
    except Exception as e:
        print(f"Error downloading {decoded_key}: {e}")
        raise

    # 4️Analyze file
    result = analyze_file(file_path)

    # 5️Upload processed file with safe name
    image_key = os.path.basename(result.get('file_path', ''))
    processed_key = f"{FileConfig.EXTRACT_PREFIX}{uuid.uuid4()}{os.path.splitext(image_key)[1]}"
    if FileConfig.S3_BUCKET and image_key:
        s3_client.upload_file(result['file_path'], FileConfig.S3_BUCKET, processed_key)
        result['image_path'] = processed_key

    # 6️Return metadata
    result['original_file'] = decoded_key
    result['file_path'] = original_key  # original path in S3
    return result
