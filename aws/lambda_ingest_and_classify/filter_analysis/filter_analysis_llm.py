import dspy
from aws.common.utilities.enums import LLMType
from aws.common.utilities.logger_manager import LoggerManager, IMAGE_ANALYSIS
from aws.lambda_ingest_and_classify.filter_analysis.file_filter_signature import FileFilterSignature


class FilterAnalysisLLM:
    def __init__(self, llm_type: LLMType):
        self._llm_type = llm_type
        self._init_llm()
        self.logger = LoggerManager.get_module_logger(IMAGE_ANALYSIS)

    def _init_llm(self):
        self._lm = dspy.LM(
            self._llm_type.value,
            temperature=0,
            max_tokens=100,
            cache = True
        )
        self._signature = FileFilterSignature.with_instructions(FileFilterSignature.get_instructions())
        dspy.configure(lm=self._lm)

    def analyze(self, image_path):
        self.logger.info('FilterAnalysisLLM analyze')
        image_input = dspy.Image.from_file(image_path)
        module = dspy.Predict(self._signature)
        output = module(image=image_input)
        return output.toDict()
