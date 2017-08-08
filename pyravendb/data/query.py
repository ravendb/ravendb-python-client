from abc import ABCMeta
from datetime import timedelta
from enum import Enum
from pyravendb.tools.utils import Utils
import sys


class QueryOperator(Enum):
    OR = "OR"
    AND = "AND"


class IndexQueryBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, query, query_parameters=None, start=0, wait_for_non_stale_result_as_of_now=False,
                 wait_for_non_stale_results=False, wait_for_non_stale_results_timeout=None):
        """
        @param query: Actual query that will be performed.
        :type str
        @param query_parameters: Parameters to the query
        :type dict
        @param start:  Number of records that should be skipped.
        :type int
        @param default_operator: The operator of the query (AND or OR) the default value is OR
        :type Enum.QueryOperator
        @param fields_to_fetch: Array of fields that will be fetched.
        :type list
        @parm default_field: Default field to use when querying directly on the Lucene query
        :type str
        @parm wait_for_non_stale_result_as_of_now:  Used to calculate index staleness
        :type bool

        """
        self._page_size = sys.maxsize
        self._page_size_set = False
        self.query = query
        self.query_parameters = query_parameters
        self.start = start
        self.wait_for_non_stale_results = wait_for_non_stale_results
        self.wait_for_non_stale_results_timeout = wait_for_non_stale_results_timeout
        if self.wait_for_non_stale_results and not self.wait_for_non_stale_results_timeout:
            self.wait_for_non_stale_results_timeout = timedelta.max
        self.wait_for_non_stale_result_as_of_now = wait_for_non_stale_result_as_of_now

    def __str__(self):
        return self.query

    @property
    def page_size(self):
        return self._page_size

    @page_size.setter
    def page_size(self, value):
        self._page_size = value
        self._page_size_set = True


class IndexQuery(IndexQueryBase):
    def __init__(self, query="", query_parameters=None, start=0, default_operator=None, includes=None, transformer=None,
                 transformer_parameters=None, show_timings=False, skip_duplicate_checking=False, **kwargs):
        super(IndexQuery, self).__init__(query=query, query_parameters=query_parameters, start=start, **kwargs)
        self.allow_multiple_index_entries_for_same_document_to_result_transformer = kwargs.get(
            "allow_multiple_index_entries_for_same_document_to_result_transformer", False)
        self.transformer = transformer
        self.transformer_parameters = transformer_parameters
        self.includes = includes if includes is not None else []
        self.show_timings = show_timings
        self.skip_duplicate_checking = skip_duplicate_checking

    def get_custom_query_str_variables(self):
        return ""

    def to_json(self):
        data = {"Query": self.query}
        if self._page_size_set and self._page_size >= 0:
            data["PageSize"] = self.page_size
        if self.wait_for_non_stale_result_as_of_now:
            data["WaitForNonStaleResultsAsOfNow"] = self.wait_for_non_stale_result_as_of_now
        if self.start > 0:
            data["Start"] = self.start
        if self.wait_for_non_stale_results_timeout is not None:
            data["WaitForNonStaleResultsTimeout"] = str(self.wait_for_non_stale_results_timeout)
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
        if self.transformer:
            data["Transformer"] = self.transformer
            if self.transformer_parameters is not None:
                data["TransformerParameters"] = self.transformer_parameters

        data["QueryParameters"] = self.query_parameters if self.query_parameters is not None else None
        return data


class FacetQuery(IndexQueryBase):
    def __init__(self, query="", index_name=None, facets=None, facet_setup_doc=None, start=0,
                 **kwargs):
        """
        @param index_name: Index name to run facet query on.
        :type str
        @param facets: list of facets (mutually exclusive with FacetSetupDoc).
        :type list of Facet
        @param facet_setup_doc: Id of a facet setup document that can be found in database containing facets.
        :type str
        """
        super().__init__(query=query, start=start, **kwargs)
        self.index_name = index_name
        self.facets = {} if facets is None else facets
        self.facet_setup_doc = facet_setup_doc
        self._facets_as_json = None

    def calculate_http_method(self):
        if len(self.facets) == 0:
            return "GET"
        return "POST"

    def get_query_string(self, method):
        path = ""
        if self.start != 0:
            path += self.start
        if self._page_size_set:
            path += "&pageSize=" + self.page_size
        if self.query:
            path += "&query=" + Utils.quote_key(self.query)
        if self.default_field:
            path += "&defaultField=" + Utils.quote_key(self.default_field)
        if self.default_operator != QueryOperator.OR:
            path += "&operator=AND"
        if self.is_distinct:
            path += "&distinct=true"
        if len(self.fields_to_fetch) > 0:
            "&fetch=".join([Utils.quote_key(field) for field in self.fields_to_fetch if field is not None])
        if self.wait_for_non_stale_result_as_of_now:
            path += "&waitForNonStaleResultsAsOfNow=true"
        if self.wait_for_non_stale_results_timeout:
            path += "&waitForNonStaleResultsTimeout={0}".format(self.wait_for_non_stale_results_timeout)
        if self.facet_setup_doc:
            path += "&facetDoc=" + self.facet_setup_doc
        if method == "GET" and len(self.facets) > 0:
            path += "&facets=" + self.serialize_facets_to_json()
        path += "&op=facets"

        return path

    def serialize_facets_to_json(self):
        return {"Facets": [facet.to_json() for facet in self.facets]}


class FacetMode(Enum):
    default = 0
    ranges = 1

    def __str__(self):
        return self.name


class FacetAggregation(Enum):
    none = 0,
    count = 1,
    max = 2,
    min = 4,
    average = 8,
    sum = 16


class FacetTermSortMode(Enum):
    value_asc = 0
    value_desc = 1
    hits_asc = 2
    hits_desc = 3

    def __str__(self):
        return self.name


class Facet(object):
    def __init__(self, name=None, display_name=None, ranges=None, mode=FacetMode.ranges,
                 aggregation=FacetAggregation.none, aggregation_field=None, aggregation_type=None, max_result=None,
                 term_sort_mode=FacetTermSortMode.value_asc, include_remaining_terms=False):
        """
        @param name: Name of facet.
        :type str
        @param display_name: Display name of facet. Will return {name} if None.
        :type str
        @param ranges: List of facet ranges
        :type list of str
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
        data = {"Mode": self.mode, "Aggregation": self.aggregation, "AggregationField": self.aggregation_field,
                "AggregationType": self.aggregation_type, "Name": self.name, "TermSortMode": self.term_sort_mode,
                "IncludeRemainingTerms": self.include_remaining_terms}

        if self.max_result is not None:
            data["MaxResults"] = self.max_result

        if self.display_name is not None and self.display_name != self.name:
            data["DisplayName"] = self.display_name

        if len(self.rangers) > 0:
            data["Ranges"] = self.ranges

        return data
