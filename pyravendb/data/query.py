from pyravendb.tools.utils import Utils
from abc import ABCMeta
from enum import Enum
import json
import sys


class EscapeQueryOptions(Enum):
    EscapeAll = 0
    AllowPostfixWildcard = 1
    AllowAllWildcards = 2
    RawQuery = 3


class QueryOperator(Enum):
    OR = "OR"
    AND = "AND"

    def __str__(self):
        return self.value


class OrderingType(Enum):
    str = 0
    long = " AS long"
    float = " AS double"
    alpha_numeric = " AS alphaNumeric"

    def __str__(self):
        return self.value


class IndexQueryBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, query, query_parameters=None, start=0, page_size=None,
                 wait_for_non_stale_results=False, wait_for_non_stale_results_timeout=None, cutoff_etag=None):
        """
        @param str query: Actual query that will be performed.
        @param dict query_parameters: Parameters to the query
        @param int start:  Number of records that should be skipped.
        @param int page_size:  Maximum number of records that will be retrieved.
        @param bool wait_for_non_stale_results: True to wait in the server side to non stale result
        @param None or float cutoff_etag: Gets or sets the cutoff etag.
        """
        self._page_size = sys.maxsize if page_size is None else page_size
        self._page_size_set = False
        if page_size is not None:
            self.page_size = page_size
        self.query = query
        self.query_parameters = query_parameters
        self.start = start
        self.wait_for_non_stale_results = wait_for_non_stale_results
        self.wait_for_non_stale_results_timeout = wait_for_non_stale_results_timeout
        self.cutoff_etag = cutoff_etag

    def __str__(self):
        return self.query

    @property
    def page_size(self):
        return self._page_size

    @page_size.setter
    def page_size(self, value):
        self._page_size = value
        self._page_size_set = True

    @property
    def page_size_set(self):
        return self._page_size_set


class IndexQuery(IndexQueryBase):
    def __init__(self, query="", query_parameters=None, start=0, includes=None, show_timings=False,
                 skip_duplicate_checking=False, **kwargs):
        super(IndexQuery, self).__init__(query=query, query_parameters=query_parameters, start=start, **kwargs)
        self.allow_multiple_index_entries_for_same_document_to_result_transformer = kwargs.get(
            "allow_multiple_index_entries_for_same_document_to_result_transformer", False)
        self.includes = includes if includes is not None else []
        self.show_timings = show_timings
        self.skip_duplicate_checking = skip_duplicate_checking

    def get_custom_query_str_variables(self):
        return ""

    def to_json(self):
        data = {"Query": self.query, "CutoffEtag": self.cutoff_etag}
        if self._page_size_set and self._page_size >= 0:
            data["PageSize"] = self.page_size
        if self.wait_for_non_stale_results:
            data["WaitForNonStaleResults"] = self.wait_for_non_stale_results
        if self.start > 0:
            data["Start"] = self.start
        if self.wait_for_non_stale_results_timeout is not None:
            data["WaitForNonStaleResultsTimeout"] = Utils.timedelta_to_str(self.wait_for_non_stale_results_timeout)
        if self.allow_multiple_index_entries_for_same_document_to_result_transformer:
            data[
                "AllowMultipleIndexEntriesForSameDocumentToResultTransformer"] = \
                self.allow_multiple_index_entries_for_same_document_to_result_transformer
        if self.show_timings:
            data["ShowTimings"] = self.show_timings
        if self.skip_duplicate_checking:
            data["SkipDuplicateChecking"] = self.skip_duplicate_checking
        if len(self.includes) > 0:
            data["Includes"] = self.includes

        data["QueryParameters"] = self.query_parameters if self.query_parameters is not None else None
        return data

    def get_query_hash(self):
        return hash((self.query,self.wait_for_non_stale_results, self.wait_for_non_stale_results_timeout,
                     self.show_timings, self.cutoff_etag, self.cutoff_etag, self.start, self.page_size,
                     json.dumps(self.query_parameters)))

class FacetQuery(IndexQueryBase):
    def __init__(self, query="", facet_setup_doc=None, facets=None, start=0, page_size=None, **kwargs):
        """
        @param facets: list of facets (mutually exclusive with FacetSetupDoc).
        :type list of Facet
        @param facet_setup_doc: Id of a facet setup document that can be found in database containing facets.
        :type str
        """
        super().__init__(query=query, start=start, page_size=page_size, **kwargs)
        self.facets = {} if facets is None else facets
        self.facet_setup_doc = facet_setup_doc

    def get_query_hash(self):
        return hash((self.query,self.wait_for_non_stale_results, self.wait_for_non_stale_results_timeout,
                    self.cutoff_etag, self.start,self.page_size, json.dumps(self.query_parameters),
                    self.facet_setup_doc,json.dumps([facet.to_json() for facet in self.facets])))

    def to_json(self):
        data = {"Query": self.query, "CutoffEtag": self.cutoff_etag}
        if self._page_size_set and self._page_size >= 0:
            data["PageSize"] = self.page_size
        if self.start > 0:
            data["Start"] = self.start
        data["Facets"] = [facet.to_json() for facet in self.facets]
        data["FacetSetupDoc"] = self.facet_setup_doc
        data["QueryParameters"] = self.query_parameters if self.query_parameters is not None else None
        data["WaitForNonStaleResults"] = self.wait_for_non_stale_results
        data["WaitForNonStaleResultsTimeout"] = Utils.timedelta_to_str(self.wait_for_non_stale_results_timeout) \
            if self.wait_for_non_stale_results_timeout is not None else None
        return data


class FacetMode(Enum):
    default = "Default"
    ranges = "Ranges"

    def __str__(self):
        return self.value


class FacetAggregation(Enum):
    none = "None"
    count = "Count"
    max = "Max"
    min = "Min"
    average = "Average"
    sum = "Sum"

    def __str__(self):
        return self.value


class FacetTermSortMode(Enum):
    value_asc = "ValueAsc"
    value_desc = "ValueDesc"
    hits_asc = "HitsAsc"
    hits_desc = "HitsDesc"

    def __str__(self):
        return self.value


class Facet(object):
    def __init__(self, name=None, display_name=None, ranges=None, mode=FacetMode.default,
                 aggregation=FacetAggregation.none, aggregation_field=None, aggregation_type=None, max_result=None,
                 term_sort_mode=FacetTermSortMode.value_asc, include_remaining_terms=False):
        """
        @param name: Name of facet.
        :type str
        @param display_name: Display name of facet. Will return {name} if None.
        :type str
        @param list ranges: List of facet ranges
        ex. [10 TO NULL] is > 10, [NULL TO 10} is <=10, [10 TO 30] is 10 < x > 30
        @param mode: Mode of a facet (Default, ranges).
        :type FacetMode
        @param aggregation: Flags indicating type of facet aggregation
        :type FacetAggregation
        @param aggregation_field: Field on which aggregation will be performed
        :type str
        @param aggregation_type: Type of field on which aggregation will be performed.
        :type str
        @param max_result: Maximum number of results to return.
        :type int
        @param term_sort_mode: Indicates how terms should be sorted.
        :type FacetTermSortMode
        @param include_remaining_terms: Indicates if remaining terms should be included in results.
        :type bool
        """
        self.name = name
        self.display_name = name if display_name is None else display_name
        self.ranges = [] if ranges is None else ranges
        self.mode = mode
        self.aggregation = aggregation
        self.aggregation_field = aggregation_field
        self.aggregation_type = aggregation_type
        self.max_result = max_result
        self.term_sort_mode = term_sort_mode
        self.include_remaining_terms = include_remaining_terms

    def to_json(self):
        data = {"Mode": str(self.mode), "Aggregation": str(self.aggregation),
                "AggregationField": self.aggregation_field,
                "AggregationType": self.aggregation_type, "Name": self.name, "TermSortMode": str(self.term_sort_mode),
                "IncludeRemainingTerms": self.include_remaining_terms}

        if self.max_result is not None:
            data["MaxResults"] = self.max_result

        if self.display_name is not None and self.display_name != self.name:
            data["DisplayName"] = self.display_name

        if len(self.ranges) > 0:
            data["Ranges"] = self.ranges

        return data
