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

    def __init__(self, query, start=0, default_operator=None, fields_to_fetch=None,
                 wait_for_non_stale_result_as_of_now=False,
                 wait_for_non_stale_results=False,
                 wait_for_non_stale_results_timeout=None, default_field=None):
        """
        @param query: Actual query that will be performed (Lucene syntax).
        :type str
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
        self.is_distinct = False
        self.query = query
        self.start = start
        self.fields_to_fetch = fields_to_fetch
        self.default_operator = default_operator
        self.wait_for_non_stale_results = wait_for_non_stale_results
        self.wait_for_non_stale_results_timeout = wait_for_non_stale_results_timeout
        if self.wait_for_non_stale_results and not self.wait_for_non_stale_results_timeout:
            self.wait_for_non_stale_results_timeout = timedelta(minutes=15)
        self.default_field = default_field
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
    def __init__(self, query="", start=0, default_operator=None, includes=None, transformer=None, show_timings=False,
                 **kwargs):
        super().__init__(query, start, default_operator, **kwargs)
        self.allow_multiple_index_entries_for_same_document_to_result_transformer = kwargs.get(
            "allow_multiple_index_entries_for_same_document_to_result_transformer", False)
        self.transformer = transformer
        self.transformer_parameters = kwargs.get("transformer_parameters", None)
        self.sorted_fields = kwargs.get("sorted_fields", [])
        self.includes = includes if includes is not None else []
        self.show_timings = show_timings
        self.skip_duplicate_checking = kwargs.get("skip_duplicate_checking", False)

    def get_custom_query_str_variables(self):
        return ""

    def get_query_string(self, include_query=True):
        path = "?"
        if self.query is not None and include_query:
            path += self.query
        if self.default_field:
            path += "&defaultField={0}".format(self.default_field)
        if self.default_operator != QueryOperator.OR:
            path += "&operator=AND"
        custom_vars = self.get_custom_query_str_variables()
        if vars:
            path += custom_vars if custom_vars.startswith("&") else "&" + custom_vars

        if self.start != 0:
            path += "&start={0}".format(self.start)
        if self._page_size_set:
            path += "&pageSize={0}".format(self.page_size)
        if self.allow_multiple_index_entries_for_same_document_to_result_transformer:
            path += "&allowMultipleIndexEntriesForSameDocumentToResultTransformer=true"
        if self.is_distinct:
            path += "&distinct=true"
        if self.show_timings:
            path += "&showTimings=true"
        if self.skip_duplicate_checking:
            path += "&skipDuplicateChecking=true"
        if len(self.fields_to_fetch) > 0:
            "&fetch=".join([Utils.quote_key(field) for field in self.fields_to_fetch if field is not None])
        if len(self.includes) > 0:
            "&include=".join([Utils.quote_key(include) for include in self.includes if include is not None])
        if len(self.sorted_fields) > 0:
            "&sort=".join([Utils.quote_key(sort) for sort in self.sorted_fields if sort is not None])
        if self.transformer:
            path += "&transformer={0}".format(Utils.quote_key(self.transformer))
        if self.transformer_parameters is not None:
            for key, value in self.transformer_parameters.items():
                path += "&tp-{0}={1}".format(key, value)
        if self.wait_for_non_stale_result_as_of_now:
            path += "&waitForNonStaleResultsAsOfNow=true"
        if self.wait_for_non_stale_results_timeout:
            path += "&waitForNonStaleResultsTimeout={0}".format(self.wait_for_non_stale_results_timeout)
