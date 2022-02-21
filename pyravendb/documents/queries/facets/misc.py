from __future__ import annotations

import enum
from typing import Union

from pyravendb import constants


class FacetAggregation(enum.Enum):
    NONE = "None"
    MAX = "Max"
    MIN = "Min"
    AVERAGE = "Average"
    SUM = "Sum"


class FacetOptions:
    @staticmethod
    def default_options() -> FacetOptions:
        return FacetOptions()

    def __init__(self):
        self.page_size: int = constants.int_max
        self.start: Union[None, int] = None
        self.term_sort_mode: FacetTermSortMode = FacetTermSortMode.VALUE_ASC
        self.include_remaining_terms: Union[None, bool] = None


class FacetResult:
    def __init__(self):
        self.name: Union[None, str] = None
        self.values = []
        self.remaining_terms = []
        self.remaining_terms_count: Union[None, int] = None
        self.remaining_hits: Union[None, int] = None

    @staticmethod
    def from_json(json_dict: dict) -> FacetResult:
        x = FacetResult()
        x.name = json_dict.get("Name", None)
        x.values = json_dict.get("Values", None)
        x.remaining_terms = json_dict.get("RemainingTerms", None)
        x.remaining_terms_count = json_dict.get("RemainingTermsCount", None)
        x.remaining_hits = json_dict.get("RemainingHits", None)
        return x


class FacetTermSortMode(enum.Enum):
    VALUE_ASC = "ValueAsc"
    VALUE_DESC = "ValueDesc"
    COUNT_ASC = "CountAsc"
    COUNT_DESC = "CountDesc"
