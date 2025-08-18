from typing import Dict, Any

from utilities.enums import FileType


def analyze_metadata_check(local_file_path: str,
                    file_type: FileType,
                    label_data: Dict[str, Any]=None):
    from .metadata_checks import analyze_metadata as _analyze_metadata

    return _analyze_metadata(local_file_path=local_file_path,file_type=file_type,label_data=label_data)


__all__ = ["analyze_metadata_check"]
