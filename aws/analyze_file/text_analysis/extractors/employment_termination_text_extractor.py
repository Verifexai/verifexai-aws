from aws.analyze_file.text_analysis.extractors import BaseTextExtractor
from aws.common.utilities.enums import TerminationCertificateField

TERMINATION_PROMPT = """Extract Israeli employment termination certificate (אישור סיום העסקה) fields.
If Hebrew text needs gershayim/geresh, use the Unicode characters: U+05F4 (״) and U+05F3 (׳). Do not use ASCII " or ' inside values.
Return ONLY a single valid JSON object. No prose, no code fences following structure:
{
  "document_date": {"text": "date as shown", "value": "YYYY-MM-DD"},
  "worker_name": {"text": "name", "value": "name"},
  "worker_id": {"text": "id", "value": "9digits"},
  "company_name": {"text": "name", "value": "name"},
  "job_start_date": {"text": "date as shown, "value": "YYYY-MM-DD"},
  "job_departure_date": {"text": "date as shown", "value": "YYYY-MM-DD "},
  "approver_name": {"text": "name", "value": "name"}
}

return job_start_date or job_departure_date only if exist on the document
"""


class EmploymentTerminationTextExtractor(BaseTextExtractor):
    """Extractor for Israeli employment termination certificates."""

    prompt = TERMINATION_PROMPT
    fields = list(TerminationCertificateField)
    date_fields = [
        TerminationCertificateField.DOCUMENT_DATE.value,
        TerminationCertificateField.JOB_START_DATE.value,
        TerminationCertificateField.JOB_DEPARTURE_DATE.value,
    ]