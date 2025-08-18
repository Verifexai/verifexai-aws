from enum import Enum
from typing import Literal, Optional
from urllib.parse import unquote_plus


class LLMType(Enum):
    """Supported Bedrock models."""

    CLAUDE_4_SONNET = 'bedrock/arn:aws:bedrock:eu-central-1:162174360193:inference-profile/eu.anthropic.claude-sonnet-4-20250514-v1:0'
    CLAUDE_3_HAIKU = 'bedrock/anthropic.claude-3-haiku-20240307-v1:0'
    CLAUDE_3_SONNET = 'bedrock/anthropic.claude-3-sonnet-20240229-v1:0'
    NOVA_LITE = 'bedrock/amazon.nova-lite-v1:0'


    @classmethod
    def value_of(cls, value):
        for k, v in cls.__members__.items():
            if k == value:
                return v
        else:
            raise ValueError(f"'{cls.__name__}' enum not found for '{value}'")


class EnumWithDescription(Enum):
    def __new__(cls, value, description):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.description = description
        return obj

    @classmethod
    def to_literal(cls):
        return Literal[tuple(member.value for member in cls)]

    @classmethod
    def descriptions(cls, separator: str = "\n") -> str:
        return separator.join(f"{member.value}: {member.description}" for member in cls)

    @classmethod
    def from_value(cls, value: str):
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"{value} is not a valid {cls.__name__}")

class State(Enum):
    ISRAEL = "ISRAEL"
    USA = "USA"

# class FileType(EnumWithDescription):
#     # File161 = ('Form_161_Retirement', "An official Israeli tax form (Form 161), The first page mentions 'הודעה על פרישה מעבודה' or 'טופס 161', the document has 2–6 pages")
#     FileSoma = ('Tax_Assessment_Certificate', "A certificate issued by the רשות המיסים בישראל (Tax Assessor) confirming an individual’s tax status or deductions. the first page mentions 'רשות המסים בישראל' or 'פקיד שומה', includes 'ניכוי מס במקור', is usually 1 page, and looks like an official letter.")
#     FIleTerminationWork = ('Employment_Termination_Certificate', "A document issued by the employer confirming that an employee has left the company. the document mentions 'אישור עזיבת עבודה' or 'סיום העסקה'")
#     # INVOICE = ('Invoice', 'An invoice is a document issued by a seller to a buyer - mostly contains price and services')
#     # GOVERNMENT_FILE = ("Government_file","An official document or set of documents created or held by a government agency or department")
#     # GENERAL_DOCUMENT = ('General_document', 'A document that doesnt fit into any other category.')
#     OTHER = ('Other', 'Uncategorized or miscellaneous file type (Not document)')

class FileType(Enum):
    TaxCertificate = "TaxCertificate"
    TerminationCertificate = "TerminationCertificate"
    Other = "Other"

    @classmethod
    def from_parent_folder(cls, parent_folder: Optional[str]) -> "FileType":
        """Return enum value based on the immediate parent folder name."""
        if not parent_folder:
            return cls.Other

        # Normalize: decode, trim slashes/space, and casefold for safe matching
        name = unquote_plus(parent_folder).strip().strip("/").casefold()

        mapping = {
            "tax-assessor-certificate": cls.TaxCertificate,
            "employment-termination-certificate": cls.TerminationCertificate,
        }
        return mapping.get(name, cls.Other)


class TaxCertificateField(Enum):
    DOCUMENT_DATE = "document_date"
    DOCUMENT_DATE_HEBREW = "document_date_hebrew"
    JOB_DEPARTURE_DATE = "job_departure_date"
    DEDUCTION_FILE_NUMBER = "deduction_file_number"
    WORKER_NAME = "worker_name"
    WORKER_ID = "worker_id"
    COMPANY_NAME = "company_name"
    COMPANY_NUMBER = "company_number"
    COMPENSATION_AMOUNT = "compensation_amount"
    TAX_OFFICER_NAME = "tax_officer_name"


class TerminationCertificateField(Enum):
    DOCUMENT_DATE = "document_date"
    WORKER_NAME = "worker_name"
    WORKER_ID = "worker_id"
    COMPANY_NAME = "company_name"
    JOB_START_DATE = "job_start_date"
    JOB_DEPARTURE_DATE = "job_departure_date"
    APPROVER_NAME = "approver_name"

class Category(str, Enum):
    # keep these aligned with your public API categories
    SYNTHETIC_MEDIA = "synthetic_media"                # AI-Generated Media
    VISUAL_ANALYSIS = "visual_analysis"                # Visual Document Analysis
    FILE_METADATA = "file_metadata"                    # File Metadata & Provenance
    CROSS_SOURCE_VERIFICATION = "cross_source_verification"  # Cross-Source Verification
    HISTORICAL_PATTERNS = "historical_patterns"        # Historical Pattern Analysis

class Status(str, Enum):
    PASS_ = "pass"
    WARN = "warn"
    FAIL = "risk"
    ERROR = "error"
    SKIPPED = "skipped"

class EvidenceType(str, Enum):
    TEXT = "text"                 # a raw text snippet
    FIELD = "field"               # named field/value (e.g., start_date)
    METRIC = "metric"             # numeric metric (similarity, score)
    STAT = "stat"                 # statistical value (share, mean, etc.)
    METADATA = "metadata"         # file metadata (XMP/EXIF/PDF)

class Severity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class Kind(str, Enum):
    # Visual / fonts
    FONT_MANIPULATION = "font_manipulation"
    # Text validations
    DATE_CONSISTENCY = "date_consistency"
    AMOUNT_CONSISTENCY = "amount_consistency"
    SUM_CHECK = "sum_check"
    ID_FORMAT = "id_format"
    ADDRESS_VALIDITY = "address_validity"
    NAME_ENTITY_MISMATCH = "name_entity_mismatch"

    # Metadata / provenance
    SUSPICIOUS_PRODUCER = "suspicious_producer"
    TIMESTAMP_INCONSISTENT = "timestamp_inconsistent"
    DIGITAL_SIGNATURE_INVALID = "digital_signature_invalid"
    ANNOTATION_PRESENT = "annotation_present"
    IMAGE_ONLY_PDF = "image_only_pdf"
    PRODUCER_XMP_MISMATCH = "producer_xmp_mismatch"
    HASH_MISMATCH = "hash_mismatch"

    # Duplicates / history
    EXACT_DUPLICATE = "exact_duplicate"
    NEAR_DUPLICATE_HASH = "near_duplicate_hash"
    CONTENT_REUSE_PATTERN = "content_reuse_pattern"

    # Cross-source verification
    REGISTRY_MATCH = "registry_match"
    EXTERNAL_AMOUNT_MISMATCH = "external_amount_mismatch"
    EXTERNAL_STATUS_MISMATCH = "external_status_mismatch"

    # Synthetic media
    SYNTHETIC_MEDIA_DETECTED = "synthetic_media_detected"
