from pyravendb.raven_operations.query_operation import QueryOperation
from pyravendb.custom_exceptions.exceptions import *
from pyravendb.data.query import IndexQuery, QueryOperator, EscapeQueryOptions, OrderingType
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
                       'between': 'between', 'search': 'search', 'lucene': 'lucene', 'startsWith': 'startsWith',
                       'endsWith': 'endsWith', 'exists': 'exists'}

    def __init__(self, session):
        """
        These argument will be initialized when class is called (see __call__)
        for be able to query with the same instance of the Query class
        """

        self.session = session

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
        self._query = None
        self.start = None
        self.page_size = None

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
        @param timedelta timeout: The time to wait for non stale result
        """
        if not index_name:
            index_name = "dynamic"
            if object_type is not None:
                index_name += "/{0}".format(self.session.conventions.default_transform_plural(object_type.__name__))
        self.index_name = index_name
        # TODO if collection_name is null see ProcessQueryParameters
        self.collection_name = collection_name
        self.object_type = object_type
        self.nested_object_types = nested_object_types
        self.is_map_reduce = is_map_reduce
        self._with_statistics = with_statistics
        self.default_operator = default_operator
        self.metadata_only = metadata_only
        self.query_parameters = {}
        self.negate = False
        self.fields_to_fetch_token = {}
        self._select_tokens = []  # List[_Token]
        self._from_token = None
        self._group_by_tokens = []  # List[_Token]
        self._order_by_tokens = []  # List[_Token]
        self._where_tokens = []  # List[_Token]
        self.includes = set()
        self._current_clause_depth = 0
        self.is_intersect = False
        self.start = 0
        self.page_size = None
        self.cutoff_etag = None
        self.wait_for_non_stale_results = wait_for_non_stale_results
        self.timeout = timeout if timeout is not None else self.session.conventions.timeout
        self.fetch = None
        self._query = None

        return self

    def __iter__(self):
        return iter(self._execute_query())

    def __str__(self):
        return self._build_query()

    def _build_query(self):
        if self._query:
            return self._query

        if self._current_clause_depth != 0:
            raise InvalidOperationException(
                "A clause was not closed correctly within this query, current clause depth = {0}".format(
                    self._current_clause_depth))

        query_builder = []
        self.build_from(query_builder)
        self.build_group_by(query_builder)
        self.build_where(query_builder)
        self.build_order_by(query_builder)
        self.build_select(query_builder)
        self.build_include(query_builder)

        return "".join(query_builder)

    def build_from(self, query_builder):
        Query._add_space_if_needed(query_builder)
        if self.index_name == "dynamic":
            query_builder += "FROM "
            if Utils.contains_any(self.collection_name, [' ', '\t', '\r', '\n', '\v']):
                if '"' in self.collection_name:
                    raise ValueError("Collection name cannot contain a quote, but was: " + self.collection_name)
                    query_builder.append('"' + self.collection_name + '"')
            else:
                query_builder.append(self.collection_name)
        else:
            query_builder.append("FROM INDEX '" + self.index_name + "'")

    def build_group_by(self, query_builder):
        if len(self._group_by_tokens) > 0:
            query_builder.append(" GROUP BY ")
            index = 0
            for token in self._group_by_tokens:
                if index > 0:
                    query_builder.append(",")
                    query_builder.append(token.write)
                index += 1

    def build_where(self, query_builder):
        if len(self._where_tokens) > 0:
            query_builder.append(" WHERE ")
            if self.is_intersect:
                self.query_builder.append("intersect(")

            for token in self._where_tokens:
                Query._add_space_if_needed(query_builder)
                query_builder.append(token.write)

            if self.is_intersect:
                query_builder.append(") ")

    def build_order_by(self, query_builder):
        if len(self._order_by_tokens) > 0:
            query_builder.append(" ORDER BY ")
            first = True
            for token in self._order_by_tokens:
                if not first and self._order_by_tokens[-1] is not None:
                    query_builder.append(", ")
                first = False
                query_builder.append(token.write)
                if token.ordering != OrderingType.str:
                    query_builder.append(token.ordering)
                if token.descending:
                    query_builder.append("DESC")

    def build_select(self, query_builder):
        select_token_length = len(self._select_tokens)
        if select_token_length > 0:
            index = 0
            query_builder.append("SELECT ")
            if select_token_length == 1 and self._select_tokens[0].token == "distinct":
                query_builder.append("DISTINCT *")
                return

            for token in self._select_tokens:
                if index > 0 and self._select_tokens[index - 1].token != "distinct":
                    query_builder.append(",")
                Query._add_space_if_needed(query_builder)
                query_builder.append(token.write)
                index += 1

    def build_include(self, query_builder):
        if self.includes is not None and len(self.includes) > 0:
            query_builder.append(" INCLUDE ")
            first = True
            for include in self.includes:
                if not first:
                    query_builder.append(",")
                first = False
                required_quotes = False
                if not re.match("^[A-Za-z0-9_]+$", include):
                    required_quotes = True
                if required_quotes:
                    query_builder.append("'{0}'".format(include.replace("'", "\\'")))
                else:
                    query_builder.append(include)

    def assert_no_raw_query(self):
        if self._query:
            raise InvalidOperationException(
                "raw_query was called, cannot modify this query by calling on operations "
                "that would modify the query (such as where, select, order_by, group_by, etc)")

    @staticmethod
    def _get_rql_write_case(token):
        write = None
        if token.token == "in":
            write = " IN ($" + token.value + ")"
        elif token.token == "all_in":
            write = " ALL IN ($" + token.value + ")"
        elif token.token == "between":
            write = "".join(["BETWEEN $", str(token.value[0]), " AND $", str(token.value[1])])
        elif token.token == "equals":
            write = " = $" + token.value
        elif token.token == "not_equals":
            write = " != $" + token.value
        elif token.token == "greater_then":
            write = " > $" + token.value
        elif token.token == "greater_then_or_equal":
            write = " >= $" + token.value
        elif token.token == "less_than":
            write = " < $" + token.value
        elif token.token == "less_than_or_equal":
            write = " <= $" + token.value
        elif token.token == "search":
            write_builder = [", $", token.value]
            if hasattr(token, "search_operator"):
                write_builder.append(", AND")
            write_builder.append(")")
            write = "".join(write_builder)
        elif token.token == "lucene":
            write = ", $" + token.value + ")"
        elif token.token == "startsWith":
            write = ", $" + token.value + ")"
        elif token.token == "endsWith":
            write = ", $" + token.value + ")"
        elif token.token == "exists":
            write = ")"

        if write is None:
            raise AttributeError("token.token don't match any of the cases for rql builder")
        return write

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

        if token.token in ["search", "lucene", "startsWith", "endWith", "exists"]:
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

    @staticmethod
    def _add_space_if_needed(query_builder):
        if len(query_builder) > 0:
            if not query_builder[-1].endswith(("(", ")", ",", " ")):
                query_builder.append(" ")

    def negate_if_needed(self, field_name):
        if self.negate:
            self.negate = False

            if len(self._where_tokens) == 0:
                if field_name is not None:
                    self.where_exists(field_name)
                else:
                    self.where_true()

                self.and_also()
            self._where_tokens.append(_Token(write="NOT"))

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
            if hasattr(last_token, "search_operator"):
                # default to OR operator after search if AND was not specified explicitly
                query_operator = QueryOperator.OR

            if query_operator:
                self._where_tokens.append(_Token(write=str(query_operator)))

    def intersect(self):
        if len(self._where_tokens) > 0:
            last = self._where_tokens[-1]
            if last.token in Query.where_operators:
                self.is_intersect = True
                self._where_tokens.append(_Token(value=None, token="intersect", write=","))
                return self
            else:
                raise InvalidOperationException("Cannot add INTERSECT at this point.")

    def raw_query(self, query):
        """
        To get all the document that equal to the query

        @param str query: The rql query
        """
        if len(self._where_tokens) != 0 or len(self._select_tokens) != 0 or len(
                self._order_by_tokens) != 0 or len(self._group_by_tokens) != 0:
            raise InvalidOperationException(
                "You can only use raw_query on a new query, without applying any operations "
                "(such as where, select, order_by, group_by, etc)")
        self._query = query
        return self

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
            value = timedelta

        self._add_operator_if_needed()

        parameter_name = self.add_query_parameter(value)
        token = "equals"
        if self.negate:
            self.negate = False
            token = "not_equals"
        token = _Token(field_name=field_name, value=parameter_name, token=token, exact=exact)
        token.write = self.rql_where_write(token)
        self._where_tokens.append(token)

        return self

    def where_not_equal(self, field_name, value, exact=False):
        if not self.negate:
            self.negate = True
        return self.where_equals(field_name, value, exact)

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
                self.where_in(field_name, kwargs[field_name], exact)
            else:
                self.where_equals(field_name, kwargs[field_name], exact)
        return self

    # def search(self, field_name, search_terms, escape_query_options=EscapeQueryOptions.RawQuery, boost=1):
    #     """
    #     for more complex text searching
    #
    #     @param field_name:The field name in the index you want to query.
    #     :type str
    #     @param search_terms: The terms you want to query
    #     :type str
    #     @param escape_query_options: The way we should escape special characters
    #     :type EscapeQueryOptions
    #     @param boost: This feature gives user the ability to manually tune the relevance level of matching documents
    #     :type numeric
    #     """
    #     if boost < 0:
    #         raise ArgumentOutOfRangeException("boost", "boost factor must be a positive number")
    #
    #     search_terms = Utils.quote_key(str(search_terms))
    #     search_terms = self._lucene_builder(search_terms, "search", escape_query_options)
    #     self.query_builder += "{0}:{1}".format(field_name, search_terms)
    #     if boost != 1:
    #         self.query_builder += "^{0}".format(boost)
    #     return self

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

        @param str field_name:The field name in the index you want to query.
        @param str value: The value will be the fields value you want to query
        """
        if field_name is None:
            raise ValueError("None field_name is invalid")

        field_name = Utils.escape_if_needed(field_name)
        self._add_operator_if_needed()
        self.negate_if_needed(field_name)

        token = _Token(field_name=field_name, token="startsWith", value=self.add_query_parameter(value))
        token.write = self.rql_where_write(token)
        self._where_tokens.append(token)

        return self

    def where_in(self, field_name, values, exact=False):
        """
        Check that the field has one of the specified values

        @param str field_name: Name of the field
        @param str values: The values we wish to query
        @param bool exact: Getting the exact query (ex. case sensitive)
        """
        field_name = Utils.escape_if_needed(field_name)
        self._add_operator_if_needed()
        self.negate_if_needed(field_name)

        self._where_tokens.append(
            _Token(field_name=field_name, value=list(Utils.unpack_iterable(values)), token="in", exact=exact))
        return self

    def where_between(self, field_name, start, end, exact=False):
        field_name = Utils.escape_if_needed(field_name)

        self._add_operator_if_needed()
        self.negate_if_needed(field_name)

        if isinstance(start, timedelta):
            start = Utils.timedelta_tick(start)
        if isinstance(end, timedelta):
            end = Utils.timedelta_tick(end)

        from_parameter_name = self.add_query_parameter("*" if start is None else start)
        to_parameter_name = self.add_query_parameter("NULL" if end is None else end)

        token = _Token(field_name=field_name, token="between", value=(from_parameter_name, to_parameter_name),
                       exact=exact)
        token.write = self.rql_where_write(token)
        self._where_tokens.append(token)

        return self

    # def where_greater_than(self, field_name, value):
    #     return self.where_between(field_name, value, None)
    #
    # def where_greater_than_or_equal(self, field_name, value):
    #     return self.where_between_or_equal(field_name, value, None)
    #
    # def where_less_than(self, field_name, value):
    #     return self.where_between(field_name, None, value)
    #
    # def where_less_than_or_equal(self, field_name, value):
    #     return self.where_between_or_equal(field_name, None, value)

    def where_not_none(self, field_name):
        self.and_also().not_.where_equals(field_name, None)
        return self

    def add_order(self, field_name, descending, ordering):
        """
        @param str field_name: The field you want to order
        @param bool descending: In descending Order
        @param OrderingType ordering: The field_name type (str, long, float or alpha_numeric)
        :return:
        """
        return self.order_by(field_name, ordering, descending=descending)

    def order_by(self, field_name, ordering=OrderingType.str, descending=False):
        self.assert_no_raw_query()
        field_name = Utils.escape_if_needed(field_name)
        self._order_by_tokens.append(
            _Token(field_name=field_name, token="order_by", write=field_name, descending=descending, ordering=ordering))

        return self

    def order_by_descending(self, field_name, ordering=OrderingType.str):
        return self.order_by(field_name, ordering, descending=True)

    def order_by_score(self):
        return self.order_by("score()")

    def order_by_score_descending(self):
        return self.order_by("score()", descending=True)

    def random_ordering(self):
        return self.order_by("random()")

    def and_also(self):
        if len(self._where_tokens) > 0:
            last = self._where_tokens[-1]
            if last:
                if isinstance(self.query_parameters[last.value], QueryOperator):
                    raise InvalidOperationException("Cannot add AND, previous token was already an QueryOperator.")

                self._where_tokens.append(_Token(value=QueryOperator.AND, write="AND"))

        return self

    def or_else(self):
        if len(self.query_builder) > 0:
            self.query_builder += " OR"
        return self

    def boost(self, boost):
        if boost != 1:
            try:
                last = self._where_tokens[-1]
            except IndexError:
                raise InvalidOperationException("Missing where clause")
            if boost <= 0:
                raise ArgumentOutOfRangeException("boost", "Boost factor must be a positive number")

            setattr(last, "boost", boost)
            return self

    def take(self, count):
        self.page_size = count
        return self

    def skip(self, count):
        self.start = count
        return self

    def _negate_next(self):
        """
        Negate the next operation
        """

        self.negate = not self.negate

    @property
    def not_(self):
        self._negate_next()
        return self

    def _execute_query(self):
        self.session.increment_requests_count()
        conventions = self.session.conventions
        end_time = time.time() + self.timeout.seconds
        query = self._build_query()
        while True:
            index_query = IndexQuery(query=query, query_parameters=self.query_parameters, start=self.start,
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
                continue
            break

        results = []
        response_results = response.pop("Results")
        response_includes = response.pop("Includes")
        self.session.save_includes(response_includes)
        for result in response_results:
            entity, metadata, original_metadata = Utils.convert_to_entity(result, self.object_type, conventions,
                                                                          self.nested_object_types,
                                                                          fetch=False if not self.fetch else True)
            if not self.fetch and self.object_type != dict:
                self.session.save_entity(key=original_metadata.get("@id", None), entity=entity,
                                         original_metadata=original_metadata,
                                         metadata=metadata, document=result)
            results.append(entity)

        if self._with_statistics:
            return results, response
        return results
