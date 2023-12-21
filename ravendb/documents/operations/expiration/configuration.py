from __future__ import annotations
from typing import Optional, Dict, Any


class ExpirationConfiguration:
    def __init__(self, disabled: Optional[bool] = None, delete_frequency_in_sec: Optional[int] = None):
        self.disabled = disabled
        self.delete_frequency_in_sec = delete_frequency_in_sec

    def to_json(self) -> Dict[str, Any]:
        return {"Disabled": self.disabled, "DeleteFrequencyInSec": self.delete_frequency_in_sec}

    @classmethod
    def from_json(cls, json_dict) -> ExpirationConfiguration:
        return cls(json_dict["Disabled"], json_dict["DeleteFrequencyInSec"])
