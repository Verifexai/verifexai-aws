from typing import  Literal
from pydantic import BaseModel, Field

class Overall(BaseModel):
    risk_score: int = Field(ge=0, le=100)
    severity: Literal["low","medium","high","critical"]
    summary_text: str
