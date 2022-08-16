from __future__ import annotations
from typing import Union, Optional, List, Dict


class GetDocumentResult:
    def __init__(self):
        self.includes: Union[None, dict] = None
        self.results: Union[None, list] = None
        self.counter_includes: Union[None, dict] = None
        self.time_series_includes: Union[None, dict] = None
        self.compare_exchange_value_includes: Union[None, dict] = None
        self.next_page_start: Union[None, int] = None


class GetDocumentsResult:
    def __init__(
        self,
        includes: Optional[dict] = None,
        results: Optional[List[Dict]] = None,
        counter_includes: Optional[dict] = None,
        time_series_includes: Optional[dict] = None,
        compare_exchange_includes: Optional[dict] = None,
        next_page_start: Optional[int] = None,
    ):
        self.includes = includes
        self.results = results
        self.counter_includes = counter_includes
        self.time_series_includes = time_series_includes
        self.compare_exchange_includes = compare_exchange_includes
        self.next_page_start = next_page_start

    @classmethod
    def from_json(cls, json_dict: dict) -> GetDocumentsResult:
        return cls(
            json_dict.get("Includes", None),
            json_dict.get("Results", None),
            json_dict.get("CounterIncludes", None),
            json_dict.get("TimeSeriesIncludes", None),
            json_dict.get("CompareExchangeValueIncludes", None),
            json_dict.get("NextPageStart", None),
        )
