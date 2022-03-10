from __future__ import annotations
import datetime
from typing import TypeVar, Generic, Union, Optional, Dict

from ravendb import constants
from ravendb.documents.queries.query import ProjectionBehavior
from ravendb.documents.queries.utils import HashCalculator

_T = TypeVar("_T")


class IndexQueryBase(Generic[_T]):
    def __init__(self):
        self.__page_size = constants.int_max
        self.__page_size_set = False
        self.query: Union[None, str] = None
        self.query_parameters: Union[None, Parameters] = None
        self.projection_behavior: Union[None, ProjectionBehavior] = None
        self.start: Union[None, int] = None
        self.wait_for_non_stale_results: Union[None, bool] = None
        self.wait_for_non_stale_results_timeout: Union[None, datetime.timedelta] = None

    def __str__(self):
        return self.query

    def __eq__(self, other):
        if self == other:
            return True
        if other is None or self.__class__ is not type(other):
            return False

        other: IndexQueryBase

        if self.page_size != other.__page_size:
            return False
        if self.__page_size_set != other.__page_size_set:
            return False
        if self.start != other.start:
            return False
        if self.wait_for_non_stale_results != other.wait_for_non_stale_results:
            return False
        if (self.query != other.query) if self.query is not None else (other.query is not None):
            return False
        return (
            self.wait_for_non_stale_results_timeout == other.wait_for_non_stale_results_timeout
            if self.wait_for_non_stale_results_timeout is not None
            else other.wait_for_non_stale_results_timeout is None
        )

    def __hash__(self):
        return hash(
            (
                self.page_size,
                self.query,
                self.start,
                self.wait_for_non_stale_results,
                self.wait_for_non_stale_results_timeout,
            )
        )

    @property
    def is_page_size_set(self) -> bool:
        return self.__page_size_set

    @property
    def page_size(self) -> int:
        return self.__page_size

    @page_size.setter
    def page_size(self, page_size: int):
        self.__page_size = page_size
        self.__page_size_set = True

    def to_json(self) -> dict:
        return {
            "PageSize": self.__page_size,
            "PageSizeSet": self.__page_size_set,
            "Query": self.query,
            "QueryParameters": self.query_parameters,
            "ProjectionBehavior": self.projection_behavior,
            "Start": self.start,
            "WaitForNonStaleResults": self.wait_for_non_stale_results,
            "WaitForNonStaleResultsTimeout": self.wait_for_non_stale_results_timeout,
        }


class IndexQueryWithParameters(Generic[_T], IndexQueryBase[_T]):
    def __init__(self):
        super().__init__()
        self.skip_duplicate_checking: Union[None, bool] = None

    def __eq__(self, other):
        if self == other:
            return True
        if other is None or self.__class__ is not type(other):
            return False
        if self.__class__.__bases__ != type(other).__bases__:
            return False

    def __hash__(self):
        return hash(
            (
                self.page_size,
                self.query,
                self.start,
                self.wait_for_non_stale_results,
                self.wait_for_non_stale_results_timeout,
                self.skip_duplicate_checking,
            )
        )

    def to_json(self) -> dict:
        json_dict = IndexQueryBase.to_json(self)
        json_dict.update({"SkipDuplicateChecking": self.skip_duplicate_checking})
        return json_dict


class Parameters(dict):
    def __init__(self, m: Optional[Union[Parameters, Dict[str, object]]] = None):
        if m is not None:
            super().__init__(m)


class IndexQuery(IndexQueryWithParameters[Parameters]):
    def __init__(self, query: Optional[str] = None):
        super().__init__()
        self.disable_caching: Union[None, bool] = None
        self.query = query

    def get_query_hash(self) -> str:
        hasher = HashCalculator()
        try:
            hasher.write(self.query)
            hasher.write(self.wait_for_non_stale_results)
            hasher.write(self.skip_duplicate_checking)
            hasher.write(
                (self.wait_for_non_stale_results_timeout.total_seconds() * 1000)
                if self.wait_for_non_stale_results_timeout
                else 0
            )
            hasher.write(self.start)
            hasher.write(self.page_size)
            hasher.write_parameters(self.query_parameters)
            return hasher.hash
        except Exception as e:
            raise RuntimeError("Unable to calculate hash", e)

    def __eq__(self, other):
        if self == other:
            return True
        if other is None or self.__class__ is not type(other):
            return False
        if self.__class__.__bases__ != type(other).__bases__:
            return False

    def __hash__(self):
        return hash(
            (
                self.page_size,
                self.query,
                self.start,
                self.wait_for_non_stale_results,
                self.wait_for_non_stale_results_timeout,
                self.skip_duplicate_checking,
                self.disable_caching,
            )
        )

    def to_json(self) -> dict:
        json_dict = IndexQueryWithParameters.to_json(self)
        json_dict.update({"DisableCaching": self.disable_caching, "Query": self.query})
        return json_dict
