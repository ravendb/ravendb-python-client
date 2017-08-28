from pyravendb.raven_operations.query_operation import QueryOperation
from pyravendb.custom_exceptions.exceptions import *
from pyravendb.data.query import IndexQuery, QueryOperator, EscapeQueryOptions
from pyravendb.tools.utils import Utils
from datetime import timedelta
import time
import re


class _Token:
    def __init__(self, field_name=None, value=None, token=None, write=None, **kwargs):
        self.__dict__.update({"field_name": field_name, "value": value, "token": token, "write": write, **kwargs})


class Query(object):
    where_operators = {'equals': 'equals', 'greater_than': 'greater_than',
                       'greater_than_or_equal': 'greater_than_or_equal', 'less_than': 'less_than',
                       'less_than_or_equal': 'less_than_or_equal', 'in': 'in', 'all_in': 'all_in',
                       'between': 'between', 'search': 'search', 'lucene': 'lucene', 'starts_with': 'starts_with',
                       'ends_with': 'ends_with', 'exists': 'exists'}

    def __init__(self, session):
        self.session = session
        # Those argument will be initialize when class is called see __call__
        # for be able to query with the same instance of the Query class
        self.query_builder = None
        self.negate = None
        self.fields_to_fetch_token = None
        self._select_tokens = None
        self._from_token = None
        self._group_by_tokens = None
        self._order_by_tokens = None
        self._where_tokens = None
        self.includes = None
        self._current_clause_depth = None
        self.is_intersect = None
        self.the_wait_for_non_stale_results = False
        self.fetch = None

    def __call__(self, object_type=None, index_name=None, collection_name=None, is_map_reduce=False,
                 with_statistics=False, metadata_only=False, default_operator=None, wait_for_non_stale_results=False,
                 timeout=None, nested_object_types=None):
        """
        @param Type object_type: The type of the object we want to track the entity too
        @param str index_name: The index name we want to apply
        @param str collection_name: Name of the collection (mutually exclusive with indexName)
        @param bool is_map_reduce: Whether we are querying a map/reduce index(modify how we treat identifier properties)
        @param nested_object_types: A dict of classes for nested object the key will be the name of the class and the
        value will be the object we want to get for that attribute
        @param bool with_statistics: Make it True to get the query statistics as well
        @param QueryOperator default_operator: The default query operator (OR or AND)
        @param bool wait_for_non_stale_results: Instructs the query to wait for non stale results
        @param float timeout: The time to wait for non stale result
        """
        if not index_name:
            index_name = "dynamic"
            if object_type is not None:
                index_name += "/{0}".format(self.session.conventions.default_transform_plural(object_type.__name__))
        self.index_name = index_name
        self.collection_name = collection_name
        self.object_type = object_type
        self.nested_object_types = nested_object_types
        self.is_map_reduce = is_map_reduce
        self._with_statistics = with_statistics
        self.default_operator = default_operator
        self.metadata_only = metadata_only
        self.query_builder = ""
        self.query_parameters = {}
        self.negate = False
        self.fields_to_fetch_token = {}
        self._select_tokens = []  # List[_Token]
        self._from_token = None
        self._group_by_tokens = []  # List[_Token]
        self._order_by_tokens = []  # List[_Token]
        self._where_tokens = []  # List[_Token]
        self.includes = ()
        self._current_clause_depth = 0
        self.is_intersect = False
        self.start = 0
        self.page_size = None
        self.cutoff_etag = None
        self.wait_for_non_stale_results = wait_for_non_stale_results
        self.timeout = timeout if timeout is not None else self.session.conventions.timeout
        self.fetch = None

        return self

    def __iter__(self):
        return iter(self._execute_query())

    def __str__(self):
        if self._current_clause_depth != 0:
            raise InvalidOperationException(
                "A clause was not closed correctly within this query, current clause depth = {0}".format(
                    self._current_clause_depth))

        # SELECT build
        select_token_length = len(self._select_tokens)
        if select_token_length > 0:
            index = 0
            self.query_builder += "SELECT "
            if select_token_length == 1 and self._select_tokens[0].token == "distinct":
                self.query_builder += "DISTINCT *"
                return

            for token in self._select_tokens:
                if index > 0 and self._select_tokens[index - 1].token != "distinct":
                    self.query_builder += ","
                self._add_space_if_needed()
                self.query_builder += token.write
                index += 1

        # FROM build
        self._add_space_if_needed()
        if self.index_name == "dynamic":
            self.query_builder += "FROM "
            if Utils.contains_any(self.collection_name, [' ', '\t', '\r', '\n', '\v']):
                if '"' in self.collection_name:
                    raise ValueError("Collection name cannot contain a quote, but was: " + self.collection_name)
                self.query_builder += '"' + self.collection_name + '"'
            else:
                self.query_builder += self.collection_name
        else:
            self.query_builder += "FROM INDEX '" + self.index_name + "'"

        # WITH build
        if self.includes is not None and len(self.includes) > 0:
            self.query_builder += " WITH "
            first = True
            for include in self.includes:
                if not first:
                    self.query_builder += ","
                first = False
                required_quotes = False
                if not re.match("^[A-Za-z0-9_]+$", include):
                    required_quotes = True
                self.query_builder += "include("
                if required_quotes:
                    self.query_builder += "'{0}'".format(include.replace("'", "\\'"))
                else:
                    self.query_builder += include
                self.query_builder += ")"

        # GroupBy build
        if len(self._group_by_tokens) > 0:
            self.query_builder += " GROUP BY "
            index = 0
            for token in self._group_by_tokens:
                if index > 0:
                    self.query_builder += ","
                self.query_builder += token.write
                index += 1

        # WHERE build
        if len(self._where_tokens) > 0:
            self.query_builder += " WHERE "
            if self.is_intersect:
                self.query_builder += "intersect("

            for token in self._where_tokens:
                self._add_space_if_needed()
                self.query_builder += token.write

            if self.is_intersect:
                self.query_builder += ") "

        # ORDER BY build
        if len(self._order_by_tokens) > 0:
            self.query_builder += " ORDER BY "
            first = True
            for token in self._order_by_tokens:
                if not first and self._order_by_tokens[len(self._order_by_tokens - 1)] is not None:
                    self.query_builder += ", "
                first = False
                self.query_builder += token.write

    @staticmethod
    def _get_rql_write_case(token):
        return {"in": " IN ($" + token.value + ")",
                "all_in": " ALL IN ($" + token.value + ")",
                "between": "BETWEEN $" + token.value,
                "equals": " = $" + token.value,
                "greater_then": " > $" + token.value,
                "greater_then_or_equal": " >= $" + token.value,
                "less_than": " < $" + token.value,
                "less_than_or_equal": " <= $" + token.value,
                "search": ", $" + token.value + ", AND)" if getattr(token, "search_operator",
                                                                    None) == QueryOperator.AND else ")",
                "lucene": ", $" + token.value + ")",
                "starts_with": ", $" + token.value + ")",
                "ends_with": ", $" + token.value + ")",
                "exists": ")"}.get(token.token)

    @staticmethod
    def rql_where_write(token):
        where_rql = ""
        boost = getattr(token, "boost", None)
        if boost:
            where_rql += "boost("
        fuzzy = getattr(token, "fuzzy", None)
        if fuzzy:
            where_rql += "fuzzy("
        proximity = getattr(token, "proximity", None)
        if proximity:
            where_rql += "proximity("
        exact = getattr(token, "exact", False)
        if exact:
            where_rql += "exact("

        if token.token in ["search", "lucene", "starts_with", "end_with", "exists"]:
            where_rql += token.token + "("

        where_rql += token.field_name + Query._get_rql_write_case(token)

        if exact:
            where_rql += ")"
        if proximity:
            where_rql += "," + proximity + ")"
        if fuzzy:
            where_rql += "," + fuzzy + ")"
        if boost:
            where_rql += "," + boost + ")"

        return where_rql

    def select(self, *args):
        """
        Fetch only the required fields from the server

        @param args: The name of the terms you like to acquire

        """
        if args:
            self.fetch = args
        return self

    def _add_space_if_needed(self):
        if self.query_builder.endswith(("(", ")", ",")):
            self.query_builder += " "

    def negate_if_needed(self, field_name):
        if self.negate:
            self.negate = False

            if len(self._where_tokens) == 0:
                if field_name is not None:
                    self.where_exists(field_name)
                else:
                    self.where_true()

                self.and_also()

    def add_query_parameter(self, value):
        parameter_name = "p{0}".format(len(self.query_parameters))
        self.query_parameters[parameter_name] = value
        return parameter_name

    def _add_operator_if_needed(self):
        if len(self._where_tokens) > 0:
            query_operator = None
            last_token = self._where_tokens[-1]
            if last_token is not None and last_token.token in Query.where_operators:
                query_operator = QueryOperator.AND if self.default_operator == QueryOperator.AND else QueryOperator.OR
            if last_token.hasattr("search_operator"):
                # default to OR operator after search if AND was not specified explicitly
                query_operator = QueryOperator.OR

            if query_operator:
                self._where_tokens.append({_Token(write=str(query_operator))})

    def intersect(self):
        last = self._where_tokens[-1]
        if last.token in Query.where_operators:
            self.is_intersect = True
            self._where_tokens.append(_Token(value=None, token="intersect", write=","))
            return self
        else:
            raise InvalidOperationException("Cannot add INTERSECT at this point.")

    def where_equals(self, field_name, value, exact=False):
        """
        To get all the document that equal to the value in the given field_name

        @param str field_name: The field name in the index you want to query.
        @param value: The value will be the fields value you want to query
        @param bool exact: If True getting exact match of the query
        """
        if field_name is None:
            raise ValueError("None field_name is invalid")

        field_name = Utils.escape_if_needed(field_name)
        if isinstance(value, timedelta):
            value = Utils.timedelta_tick(value)

        self._add_operator_if_needed()
        self.negate_if_needed(field_name)

        parameter_name = self.add_query_parameter(value)
        token = _Token(field_name=field_name, value=parameter_name, token="equals", exact=exact)
        token.write = self.rql_where_write(token)
        self._where_tokens.append(token)

        return self

    def where_exists(self, field_name):
        field_name = Utils.escape_if_needed(field_name)

        self._add_operator_if_needed()
        self.negate_if_needed(field_name)
        self._where_tokens.append(_Token(field_name=field_name, value=None, token="exists", write=")"))
        return self

    def where_true(self):
        self._add_operator_if_needed()
        self.negate_if_needed(None)

        self._where_tokens.append(_Token(token="true_token", write="true"))
        return self

    def where(self, exact=False, **kwargs):
        """
        To get all the document that equal to the value within kwargs with the specific key

        @param bool exact: If True getting exact match of the query
        @param kwargs: the keys of the kwargs will be the fields name in the index you want to query.
        The value will be the the fields value you want to query
        (if kwargs[field_name] is a list it will behave has the where_in method)
        """
        for field_name in kwargs:
            if isinstance(kwargs[field_name], list):
                self.where_in(field_name, kwargs[field_name])
            else:
                self.where_equals(field_name, kwargs[field_name], exact)
        return self

    def search(self, field_name, search_terms, escape_query_options=EscapeQueryOptions.RawQuery, boost=1):
        """
        for more complex text searching

        @param field_name:The field name in the index you want to query.
        :type str
        @param search_terms: The terms you want to query
        :type str
        @param escape_query_options: The way we should escape special characters
        :type EscapeQueryOptions
        @param boost: This feature gives user the ability to manually tune the relevance level of matching documents
        :type numeric
        """
        if boost < 0:
            raise ArgumentOutOfRangeException("boost", "boost factor must be a positive number")

        search_terms = Utils.quote_key(str(search_terms))
        search_terms = self._lucene_builder(search_terms, "search", escape_query_options)
        self.query_builder += "{0}:{1}".format(field_name, search_terms)
        if boost != 1:
            self.query_builder += "^{0}".format(boost)
        return self

    def where_ends_with(self, field_name, value):
        """
        To get all the document that ends with the value in the giving field_name

        @param field_name:The field name in the index you want to query.
        :type str
        @param value: The value will be the fields value you want to query
        :type str
        """
        if field_name is None:
            raise ValueError("None field_name is invalid")

        if value is not None and not isinstance(value, str) and field_name is not None:
            field_name = self.session.conventions.range_field_name(field_name, type(value).__name__)

        lucene_text = self._lucene_builder(value, action="end_with")
        self.query_builder += "{0}:*{1}".format(field_name, lucene_text)
        return self

    def where_starts_with(self, field_name, value):
        """
        To get all the document that starts with the value in the giving field_name

        @param field_name:The field name in the index you want to query.
        :type str
        @param value: The value will be the fields value you want to query
        :type str
        """
        if field_name is None:
            raise ValueError("None field_name is invalid")

        if value is not None and not isinstance(value, str) and field_name is not None:
            field_name = self.session.conventions.range_field_name(field_name, type(value).__name__)

        lucene_text = self._lucene_builder(value, action="end_with")
        self.query_builder += "{0}:{1}*".format(field_name, lucene_text)
        return self

    def where_in(self, field_name, values):
        """
        Check that the field has one of the specified values

        @param field_name: Name of the field
        :type str
        @param values: The values we wish to query
        :type list
        """
        if field_name is None:
            raise ValueError("None field_name is invalid")
        lucene_text = self._lucene_builder(values, action="in")
        if lucene_text is None:
            self.query_builder += "@emptyIn<{0}>:(no-result)".format(field_name)
        else:
            self.query_builder += "@in<{0}>:{1}".format(field_name, lucene_text)
        return self

    def where_between(self, field_name, start, end):
        if isinstance(start, timedelta):
            start = Utils.timedelta_tick(start)
        if isinstance(end, timedelta):
            end = Utils.timedelta_tick(end)

        value = start or end
        if self.session.conventions.uses_range_type(value) and not field_name.endswith("_Range"):
            field_name = self.session.conventions.range_field_name(field_name, type(value).__name__)

        lucene_text = self._lucene_builder([start, end], action="between")
        self.query_builder += "{0}:{1}".format(field_name, lucene_text)
        return self

    def where_between_or_equal(self, field_name, start, end):
        if isinstance(start, timedelta):
            start = Utils.timedelta_tick(start)
        if isinstance(end, timedelta):
            end = Utils.timedelta_tick(end)

        value = start or end
        if self.session.conventions.uses_range_type(value) and not field_name.endswith("_Range"):
            field_name = self.session.conventions.range_field_name(field_name, type(value).__name__)
        lucene_text = self._lucene_builder([start, end], action="equal_between")
        self.query_builder += "{0}:{1}".format(field_name, lucene_text)
        return self

    def where_greater_than(self, field_name, value):
        return self.where_between(field_name, value, None)

    def where_greater_than_or_equal(self, field_name, value):
        return self.where_between_or_equal(field_name, value, None)

    def where_less_than(self, field_name, value):
        return self.where_between(field_name, None, value)

    def where_less_than_or_equal(self, field_name, value):
        return self.where_between_or_equal(field_name, None, value)

    def where_not_none(self, field_name):
        if len(self.query_builder) > 0:
            self.query_builder += ' '
        self.query_builder += '('
        self.where_equals(field_name, '*').and_also().add_not().where_equals(field_name, None)
        self.query_builder += ')'
        return self

    def order_by(self, fields_name):
        if isinstance(fields_name, list):
            for field_name in fields_name:
                self._sort_fields.add(field_name)
        else:
            self._sort_fields.add(fields_name)
        return self

    def order_by_descending(self, fields_name):
        if isinstance(fields_name, list):
            for i in range(len(fields_name)):
                fields_name[i] = "-{0}".format(fields_name[i])
        else:
            fields_name = "-{0}".format(fields_name)
        self.order_by(fields_name)
        return self

    def and_also(self):
        last = self._where_tokens[-1]
        if last:
            if isinstance(last.value, QueryOperator):
                raise InvalidOperationException("Cannot add AND, previous token was already an QueryOperator.")

            self._where_tokens.append(_Token(value=QueryOperator.AND, write="AND"))

        return self

    def or_else(self):
        if len(self.query_builder) > 0:
            self.query_builder += " OR"
        return self

    def negate_next(self):
        """
        Negate the next operation
        """

        self.negate = not self.negate

    @property
    def not_(self):
        self.negate_next()
        return self

    def _execute_query(self):
        self.session.increment_requests_count()
        conventions = self.session.conventions
        c = timedelta
        c.seconds
        end_time = time.time() + self.timeout.seconds
        self.__str__()
        while True:
            index_query = IndexQuery(query=self.query_builder, query_parameters=self.query_parameters, start=self.start,
                                     page_size=self.page_size, cutoff_etag=self.cutoff_etag,
                                     wait_for_non_stale_results=self.wait_for_non_stale_results,
                                     wait_for_non_stale_results_timeout=self.timeout)

            query_command = QueryOperation(session=self.session, index_name=self.index_name, index_query=index_query,
                                           metadata_only=self.metadata_only).create_request()
            response = self.session.requests_executor.execute(query_command)
            if response is None:
                return []

            if response["IsStale"] and self.wait_for_non_stale_results:
                if time.time() > end_time:
                    raise ErrorResponseException("The index is still stale after reached the timeout")
                    time.sleep(0.1)
                continue
            break

        results = []
        response_results = response.pop("Results")
        response_includes = response.pop("Includes")

        for result in response_results:
            entity, metadata, original_metadata = Utils.convert_to_entity(result, self.object_type, conventions,
                                                                          self.nested_object_types,
                                                                          fetch=False if not self.fetch else True)
            if not self.fetch:
                self.session.save_entity(key=original_metadata.get("@id", None), entity=entity,
                                         original_metadata=original_metadata,
                                         metadata=metadata, document=result)
            results.append(entity)
        self.session.save_includes(response_includes)
        if self._with_statistics:
            return results, response
        return results
