import os
from dotenv import load_dotenv

from aws.common.config.client_config import ClientConfig
from aws.common.utilities.enums import LLMType
from tempfile import mkdtemp

load_dotenv()

os.environ['DSP_CACHEDIR'] = mkdtemp()
os.environ['DSPY_CACHEDIR'] = mkdtemp()

BEDROCK_REGION = "eu-central-1"
FONT_MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"
EXTRACT_TEXT_ID = "anthropic.claude-3-haiku-20240307-v1:0"

# LLM Config
class LLMConfig:
    LLM_API_KEY = os.environ.get('LLM_API_KEY')
    LLM_TYPE_FILTER = LLMType.value_of(
        os.environ.get('LLM_TYPE_FILTER', 'CLAUDE_4_SONNET')
    )
    DEFAULT_STATE = os.environ.get('DEFAULT_STATE', 'ISRAEL')
    DEFAULT_LANGUAGE = os.environ.get('DEFAULT_LANGUAGE', 'eng')

# File Config
class FileConfig:
    UPLOAD_FOLDER = '/tmp/files'
    UPLOAD_IMAGE_FOLDER = f'{UPLOAD_FOLDER}/images'
    TEMP_FILE_PATH = f'{UPLOAD_FOLDER}/tmp'
    S3_BUCKET = os.environ.get('S3_BUCKET')
    RAW_PREFIX = 'raw/'
    EXTRACT_PREFIX = 'extract/'
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Logging Config
class LoggingConfig:
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()

# Client Config
client_config_file = os.environ.get('CLIENT_CONFIG_FILE', '../../client_config.json')
client_config = ClientConfig.from_file(client_config_file)
