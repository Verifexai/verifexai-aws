from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import datetime

from aws.common.models.evidence import Evidence
from aws.common.utilities.enums import Category, Status, Kind


class CheckResult(BaseModel):
    id: str
    category: Category
    kind: Kind
    title: str
    description: str
    score: int = Field(ge=0, le=100)     # 0=valid, 100=fraud
    status: Status
    evidence: List[Evidence]
    tags: Optional[List[str]] = None
    timestamp: datetime
