from __future__ import annotations
from typing import Optional


class GetDocumentsResult:
    def __init__(
        self,
        includes: Optional[dict] = None,
        results: Optional[list] = None,
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

    @staticmethod
    def from_json(json_dict: dict) -> GetDocumentsResult:
        return GetDocumentsResult(
            json_dict.get("Includes", None),
            json_dict.get("Results", None),
            json_dict.get("CounterIncludes", None),
            json_dict.get("TimeSeriesIncludes", None),
            json_dict.get("CompareExchangeIncludes", None),
            json_dict.get("NextPageStart", None),
        )
