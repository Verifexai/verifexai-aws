import datetime
from aws.common.config.config import LLMConfig
from aws.common.image_processing.image_converter import ImageConverter
from aws.common.utilities.enums import FileType
from aws.common.utilities.logger_manager import LoggerManager, ANALYZE_FILE
from aws.lambda_ingest_and_classify.filter_analysis.filter_analysis_llm import FilterAnalysisLLM

filterAnalyzer = FilterAnalysisLLM(llm_type=LLMConfig.LLM_TYPE_FILTER)


def analyze_file(file_path):
    logger = LoggerManager.get_module_logger(ANALYZE_FILE)
    logger.info('filter analyze_file start')
    response_json = {}

    image_file_path, img_width, img_height = ImageConverter().convert_file(file_path)

    filter_result = filterAnalyzer.analyze(image_file_path)
    logger.info(f'Filter result: {filter_result}')

    file_type = FileType.from_value(filter_result.get('file_type', FileType.OTHER))
    response_json['file_type'] = file_type.value

    # state = State[filter_result.get('country', LLMConfig.DEFAULT_STATE)]
    # response_json['state'] = state.value

    # lang = filter_result.get('language', LLMConfig.DEFAULT_LANGUAGE)
    # response_json['language'] = lang

    response_json['file_path'] = image_file_path
    response_json['img_width'] = img_width
    response_json['img_height'] = img_height

    return response_json


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    file_path = 'files/pango.pdf'
    response = analyze_file(file_path=file_path)
    end_time = datetime.datetime.now()
    print(response)
    print(end_time - start_time)
