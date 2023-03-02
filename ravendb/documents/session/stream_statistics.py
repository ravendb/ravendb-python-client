from __future__ import annotations
from datetime import datetime
from typing import Optional, Dict


class StreamQueryStatistics:
    def __init__(
        self,
        index_name: Optional[str] = None,
        stale: Optional[bool] = None,
        index_timestamp: Optional[datetime] = None,
        total_results: Optional[int] = None,
        result_etag: Optional[int] = None,
    ):
        self.index_name = index_name
        self.stale = stale
        self.index_timestamp = index_timestamp
        self.total_results = total_results
        self.result_etag = result_etag

    @classmethod
    def from_json(cls, json_dict: Dict) -> StreamQueryStatistics:
        return cls(
            json_dict["IndexName"],
            json_dict["Stale"],
            datetime.fromisoformat(json_dict["IndexTimestamp"]),
            json_dict["TotalResults"],
            json_dict["ResultEtag"],
        )

    def to_json(self) -> Dict:
        return {
            "IndexName": self.index_name,
            "Stale": self.stale,
            "IndexTimestamp": self.index_timestamp.isoformat(),
            "TotalResults": self.total_results,
            "ResultEtag": self.result_etag,
        }
