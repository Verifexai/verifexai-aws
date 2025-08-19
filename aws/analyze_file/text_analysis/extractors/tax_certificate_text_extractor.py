from aws.analyze_file.text_analysis.extractors import BaseTextExtractor
from aws.common.utilities.enums import TaxCertificateField

TAX_PROMPT = """Extract Israeli tax certificate (אישור פקיד שומה) fields.
If Hebrew text needs gershayim/geresh, use the Unicode characters: U+05F4 (״) and U+05F3 (׳). Do not use ASCII " or ' inside values.
Return ONLY a single valid JSON object. No prose, no code fences following structure:
{
  "document_date": {"text": "date as shown", "value": "YYYY-MM-DD"},
  "document_date_hebrew": {"text": "hebrew date", "value": "Same as text"},
  "job_departure_date": {"text": "date as shown", "value": "YYYY-MM-DD"},
  "deduction_file_number": {"text": "raw number", "value": "number"},
  "worker_name": {"text": "name", "value": "name"},
  "worker_id": {"text": "id", "value": "9digits"},
  "company_name": {"text": "company name", "value": "company name"},
  "company_number": {"text": "id", "value": "9digits"},
  "compensation_amount": {"text": "amount", "value": "number"}
}
"""

class TaxCertificateTextExtractor(BaseTextExtractor):
    """Extractor for Israeli tax certificate documents."""

    prompt = TAX_PROMPT
    fields = list(TaxCertificateField)
    date_fields = [
        TaxCertificateField.DOCUMENT_DATE.value,
        TaxCertificateField.JOB_DEPARTURE_DATE.value,
    ]