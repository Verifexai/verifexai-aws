from typing import Optional
from pydantic import BaseModel
from datetime import datetime

class DocumentInfo(BaseModel):
    doc_id: str
    source: Optional[str]
    mime_type: Optional[str]
    num_pages: Optional[int]
    created_at: Optional[datetime]
