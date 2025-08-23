from typing import Dict, Any

from aws.analyze_file.metadata.pdf_metadata_scorer import PDFMetadataScorer
from aws.common.utilities.enums import FileType


def analyze_metadata_check(local_file_path: str,
                    file_type: FileType,
                    label_data: Dict[str, Any]=None):
    from metadata_checks import analyze_metadata as _analyze_metadata

    return _analyze_metadata(local_file_path=local_file_path,file_type=file_type,label_data=label_data)


__all__ = ["analyze_metadata_check"]

if __name__ == '__main__':
    print("start")
    file_path = "יואב אליעזר טרנטר מוזר - טופס אישור העסקה.pdf"
    result = analyze_metadata_check(file_path,file_type=FileType.TaxCertificate)
    print(result)