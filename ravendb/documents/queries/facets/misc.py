from __future__ import annotations

import enum
from typing import Union, List, Dict

from ravendb import constants


class FacetAggregation(enum.Enum):
    NONE = "None"
    MAX = "Max"
    MIN = "Min"
    AVERAGE = "Average"
    SUM = "Sum"


class FacetOptions:
    @classmethod
    def default_options(cls) -> FacetOptions:
        return cls()

    def __init__(self):
        self.page_size: int = constants.int_max
        self.start: Union[None, int] = None
        self.term_sort_mode: FacetTermSortMode = FacetTermSortMode.VALUE_ASC
        self.include_remaining_terms: bool = False

    def to_json(self) -> dict:
        return {
            "PageSize": self.page_size,
            "Start": self.start,
            "TermSortMode": self.term_sort_mode,
            "IncludeRemainingTerms": self.include_remaining_terms,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> FacetOptions:
        options = cls()
        options.page_size = json_dict["PageSize"]
        options.start = json_dict["Start"]
        options.term_sort_mode = FacetTermSortMode(json_dict["TermSortMode"])
        options.include_remaining_terms = json_dict["IncludeRemainingTerms"]
        return options


class FacetResult:
    def __init__(self):
        self.name: Union[None, str] = None
        self.values: List[FacetValue] = []
        self.remaining_terms: List[str] = []
        self.remaining_terms_count: Union[None, int] = None
        self.remaining_hits: Union[None, int] = None

    @classmethod
    def from_json(cls, json_dict: dict) -> FacetResult:
        x = cls()
        x.name = json_dict.get("Name", None)
        x.values = [FacetValue.from_json(jv) for jv in json_dict.get("Values", None)]
        x.remaining_terms = json_dict.get("RemainingTerms", None)
        x.remaining_terms_count = json_dict.get("RemainingTermsCount", None)
        x.remaining_hits = json_dict.get("RemainingHits", None)
        return x


class FacetValue:
    def __init__(self, name: str, range_: str, count_: int, sum_: int, max_: int, min_: int, average_: int):
        self.name = name
        self.range_ = range_
        self.count_ = count_
        self.sum_ = sum_
        self.max_ = max_
        self.min_ = min_
        self.average_ = average_

    def __str__(self):
        msg = f"{self.range_} - Count: {self.count_}, "
        if self.sum_ is not None:
            msg += f"Sum: {self.sum_},"
        if self.max_ is not None:
            msg += f"Max: {self.max_},"
        if self.min_ is not None:
            msg += f"Min: {self.min_},"
        if self.sum_ is not None:
            msg += f"Average: {self.average_},"
        if self.name is not None:
            msg += f"Name: {self.name},"

        return msg.removesuffix(";")

    @classmethod
    def from_json(cls, json_dict: dict) -> FacetValue:
        return cls(
            json_dict.get("Name"),
            json_dict.get("Range"),
            json_dict.get("Count"),
            json_dict.get("Sum"),
            json_dict.get("Max"),
            json_dict.get("Min"),
            json_dict.get("Average"),
        )


class FacetTermSortMode(enum.Enum):
    VALUE_ASC = "ValueAsc"
    VALUE_DESC = "ValueDesc"
    COUNT_ASC = "CountAsc"
    COUNT_DESC = "CountDesc"

    def __str__(self):
        return self.value
