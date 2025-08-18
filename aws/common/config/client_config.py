# client_config.py

from __future__ import annotations
from typing import List
from pydantic import BaseModel, Field, ValidationError, root_validator, constr
import json
import os

from aws.common.utilities.enums import Severity, Status


class ClientConfig(BaseModel):
    schema_version: constr(strip_whitespace=True, min_length=1) = "1.0"
    bands: Bands
    tax_officer_blacklist: List[str] = Field(default_factory=list)

    # ---------- convenience loaders ----------
    @classmethod
    def from_file(cls, path: str) -> "ClientConfig":
        if not os.path.exists(path):
            raise FileNotFoundError(f"Config file not found: {path}")
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in config file '{path}': {e}") from e

        try:
            return cls.parse_obj(data)
        except ValidationError as e:
            # Re-raise as ValueError with concise message (friendlier for callers)
            raise ValueError(f"Config validation failed: {e}") from e

    # ---------- mapping helpers ----------
    def severity_for(self, score: int) -> Severity:
        """Return Severity enum for a 0..100 score."""
        self._assert_score(score)
        for band in self.bands.severity:
            if score >= band.min:
                try:
                    return Severity(band.label.lower())
                except ValueError:
                    raise ValueError(f"Unknown severity label '{band.label}' in config")

        # fallback
        return Severity(self.bands.severity[-1].label.lower())

    def status_for(self, score: int) -> Status:
        """Return Status enum for a 0..100 score."""
        self._assert_score(score)
        for band in self.bands.status:
            if score >= band.min:
                try:
                    return Status(band.label.lower())
                except ValueError:
                    print(f"Unknown status label '{band.label}' in config")
                    return Status('error')

        # fallback
        return Status(self.bands.status[-1].label.lower())

    @staticmethod
    def _assert_score(score: int) -> None:
        if not isinstance(score, int) or score < 0 or score > 100:
            raise ValueError(f"Score must be an integer in [0, 100], got: {score}")


# --------------------------------------
# CLIENT CONFIG MODELS
# --------------------------------------
class Band(BaseModel):
    min: int = Field(ge=0, le=100, description="Minimum score (0..100) for this band")
    label: constr(strip_whitespace=True, min_length=1)

class Bands(BaseModel):
    severity: List[Band]
    status: List[Band]
