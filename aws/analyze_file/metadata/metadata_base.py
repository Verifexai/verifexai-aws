from __future__ import annotations
import re
from typing import Any, Dict, List, Optional

from aws.common.utilities.logger_manager import LoggerManager, METADATA


# ---------------------------------------------------------------------------
# Producer scoring
# ---------------------------------------------------------------------------
# Regex patterns grouped by risk class. Order matters; the first match wins.
_PRODUCER_RULES = [
    # Class A – ERP/Accounting/E-invoicing engines (WHITELIST / LOW RISK)
    (
        [
            # Existing
            r"SAP.*(Adobe Document Services|ADS|SmartForms|NetWeaver)",
            r"Oracle.*(BI Publisher|E[- ]Business Suite|JDE|JD Edwards|PeopleSoft|NetSuite)",
            r"Microsoft Dynamics.*(365|AX|NAV|GP)",
            r"Intuit.*(QuickBooks)",
            r"Xero",
            r"Sage(?:\s|$|\d)|Sage Intacct",
            r"Zoho (Books|Invoice)",
            r"Odoo",
            r"Coupa|SAP Ariba",
            r"Workday",
            r"Stripe.*Invoice|Shopify.*Invoice|Square.*Invoice",
            r"Bill\.com",

            # IBM z/OS Infoprint / AFP->PDF transform variants
            r"IBM Print Transform from AFP to PDF.*Infoprint Server.*z/OS",
            r"IBM.*Infoprint Server.*z/OS",
            r"Infoprint Server.*AFP.*PDF",
            r"\bafpxpdf\b",  # some environments expose this component name

            # OpenText Exstream (formerly HP Exstream)
            r"(OpenText|HP)\s*Exstream",

            # Quadient/GMC Inspire
            r"(GMC|Quadient)\s*Inspire",

            # Pitney Bowes EngageOne / DOC1
            r"(Pitney\s*Bowes\s*)?EngageOne|\bDOC1\b",

            # === Added: Government / e-invoicing rails (country-specific) ===
            r"\bFatturaPA\b|\bSDI\b|\bSistema di Interscambio\b",  # Italy
            r"\bFacturae\b",                                         # Spain
            r"\bPeppol\b",                                           # EU network id
            r"\bKSeF\b",                                             # Poland
            r"\bGSTN\b|\bIRP\b"                                      # India (e-invoice IRP/GSTN)
        ],
        5,
        "ERP or invoicing engine",
    ),

    # Class B – E-signature / governed doc systems (trusted but not origin)
    (
        [r"DocuSign", r"Adobe (Sign|Acrobat Sign)", r"(HelloSign|Dropbox Sign)", r"PandaDoc"],
        10,
        "E-signature or governed document system",
    ),

    # Class C – Office/Dev PDF libs/OS engines (neutral)
    (
        [
            r"iText|iTextSharp|iText7",
            r"mPDF|TCPDF|FPDF|dompdf|pdfmake",
            r"ReportLab",
            r"Apache FOP|BIRT",
            r"wkhtmltopdf|Qt.*print.*engine",
            r"Prince",
            r"WeasyPrint",
            r"pdfTeX|LuaTeX|XeTeX|LaTeX",
            r"Microsoft (Word|Excel|PowerPoint)",
            r"Microsoft® (Word|Excel|PowerPoint)",
            r"LibreOffice|OpenOffice",
            r"Skia/PDF|Chrom(e|ium)",
            r"Quartz PDFContext|Mac OS X.*Quartz",
        ],
        25,
        "Office or PDF software",
    ),

    # Class D – Virtual printers & general PDF editors (risky for origin docs)
    (
        [
            r"Microsoft Print to PDF",
            r"PDFCreator|Bullzip|PrimoPDF|CutePDF|doPDF|PDF24|PDFill",
            r"Nitro PDF (Creator|Pro)|Wondershare PDFelement",
            r"Foxit (Reader|Phantom) PDF (Printer|Editor)",
            r"Qoppa PDF.*",
            r"PDF-XChange (Editor|Printer)",
        ],
        40,
        "Virtual printer or PDF editor",
    ),

    # Class E – Scanners & mobile scan apps (often post-processed images)
    (
        [
            r"HP|Canon|Epson|Brother|Xerox|Ricoh|Kyocera|Konica Minolta|Sharp|Fuji Xerox|OKI",
            r"ABBYY FineReader|Kofax|Power PDF|OmniPage|Readiris|Nuance",
            r"CamScanner|Genius Scan|Scanbot|Adobe Scan|Office Lens|Scanner Pro|Tiny Scanner|Notebloc",
        ],
        55,
        "Scanner or mobile scan app",
    ),

    # Class F – Graphics/Design tools (high risk for origin invoices)
    (
        [
            r"Adobe Photoshop|Adobe Illustrator|CorelDRAW",
            r"Affinity (Photo|Designer)",
            r"GIMP|Inkscape|Canva",
        ],
        65,
        "Graphics or design tool",
    ),
]


_UNKNOWN_PRODUCER = re.compile(r"^$|unknown|null|-|N/A", re.IGNORECASE)


def score_producer(producer: Optional[str]) -> Dict[str, Any]:
    """Return a risk score and description for a producer string."""
    if not producer or _UNKNOWN_PRODUCER.search(producer.strip()):
        return {"score": 50, "description": "Producer missing or unknown"}

    for patterns, score, desc in _PRODUCER_RULES:
        for pattern in patterns:
            if re.search(pattern, producer, re.IGNORECASE):
                return {"score": score, "description": desc}

    return {"score": 50, "description": "Producer not recognized"}


class MetadataBaseScorer:
    """Base class for metadata scorers."""

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.logger = LoggerManager.get_module_logger(METADATA)

    def run(
        self,
        invoice_dates: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        metadata = self._extract_metadata()
        return self._score_metadata(metadata, invoice_dates or [])

    def _extract_metadata(self) -> Dict[str, Any]:  # pragma: no cover - abstract
        raise NotImplementedError

    def _score_metadata(
        self, metadata: Dict[str, Any], invoice_dates: List[str]
    ) -> Dict[str, Any]:  # pragma: no cover - abstract
        raise NotImplementedError
