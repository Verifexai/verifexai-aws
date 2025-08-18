
from typing import List, Optional, Any, Dict
from pydantic import BaseModel

from aws.common.utilities.enums import EvidenceType


class Evidence(BaseModel):
    type: EvidenceType
    value: Any
    page: Optional[int] = None
    bbox: Optional[List[float]] = None  # [x0,y0,x1,y1]
    extra: Optional[Dict[str, Any]] = None

