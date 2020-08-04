from pyravendb.raven_operations.query_operation import QueryOperation
from pyravendb.custom_exceptions.exceptions import *
from pyravendb.commands.raven_commands import GetFacetsCommand
from pyravendb.data.query import IndexQuery, QueryOperator, OrderingType, FacetQuery
from pyravendb.tools.utils import Utils
from datetime import timedelta
import time
import re


class _Token:
    def __init__(self, field_name="", value=None, token=None, write=None, **kwargs):
        self.__dict__.update({"field_name": field_name, "value": value, "token": token, "write": write, **kwargs})


class Query(object):
    where_operators = {'equals': 'equals', 'greater_than': 'greater_than',
                       'greater_than_or_equal': 'greater_than_or_equal', 'less_than': 'less_than',
                       'less_than_or_equal': 'less_than_or_equal', 'in': 'in', 'all_in': 'all_in',
                       'between': 'between', 'search': 'search', 'lucene': 'lucene', 'startsWith': 'startsWith',
                       'endsWith': 'endsWith', 'exists': 'exists'}

    rql_keyword = ("AS", "SELECT", "WHERE", "LOAD", "GROUP", "ORDER", "INCLUDE")

    def __init__(self, session):
        """
        These argument will be initialized when class is called (see __call__)
        for be able to query with the same instance of the Query class
        """

        self.session = session

        self.negate = None
        self.fields_to_fetch = None
        self._select_tokens = None
        self._from_token = None
        self._group_by_tokens = None
        self._order_by_tokens = None
        self._where_tokens = None
        self.includes = None
        self._current_clause_depth = None
        self.is_intersect = None
        self.the_wait_for_non_stale_results = False
        self._query = None
        self.start = None
        self.page_size = None
        self.last_equality = None
        self.query_parameters = None
        self.is_distinct = None

    @property
    def not_(self):
        self._negate_next()
        return self

    def __call__(self, object_type=None, index_name=None, collection_name=None, is_map_reduce=False,
                 with_statistics=False, metadata_only=False, default_operator=None, wait_for_non_stale_results=False,
                 timeout=None, nested_object_types=None):
        """
        @param Type object_type: The type of the object we want to track the entity too.
        @param str index_name: The index name we want to apply.
        @param str collection_name: Name of the collection (mutually exclusive with indexName).
        @param bool is_map_reduce: Whether we are querying a map/reduce index(modify how we treat identifier properties)
        @param bool with_statistics: Make it True to get the query statistics as well.
        @param QueryOperator default_operator: The default query operator (OR or AND).
        @param bool wait_for_non_stale_results: Instructs the query to wait for non stale results.
        @param timedelta timeout: The time to wait for non stale result.
        @param nested_object_types: A dict of classes for nested object the key will be the name of the class and the.
        value will be the object we want to get for that attribute.
        """
        self.object_type = object_type
        self.index_name, self.collection_name = self._process_query_parameters(index_name, collection_name)
        self.nested_object_types = nested_object_types
        self.is_map_reduce = is_map_reduce
        self._with_statistics = with_statistics
        self.default_operator = default_operator
        self.metadata_only = metadata_only
        self.query_parameters = {}
        self.negate = False
        self.fields_to_fetch = None
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
        self._query = None
        self.last_equality = None
        self.is_distinct = False

        return self

    def __iter__(self):
        return iter(self._execute_query())

    def __str__(self):
        return self._build_query()

    def _process_query_parameters(self, index_name, collection_name):
        if index_name and collection_name:
            raise InvalidOperationException(
                "Parameters 'index_name' and 'collection_name' are mutually exclusive. "
                "Please specify only one of them.")

        if not index_name and not collection_name:
            if not self.object_type:
                collection_name = "@all_docs"
            else:
                collection_name = self.session.conventions.default_transform_plural(self.object_type.__name__)

        return "dynamic" if index_name is None else index_name, collection_name

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
        if not self.index_name and not self.collection_name:
            raise NotSupportedException("Either index_name or collection_name must be specified")

        if self.index_name == "dynamic":
            query_builder.append("FROM ")
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
                    query_builder.append(" DESC")

    def build_select(self, query_builder):
        select_token_length = len(self._select_tokens)
        if select_token_length > 0:
            index = 0
            query_builder.append(" SELECT ")
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
            write = "".join([" BETWEEN $", str(token.value[0]), " AND $", str(token.value[1])])
        elif token.token == "equals":
            write = " = $" + token.value
        elif token.token == "not_equals":
            write = " != $" + token.value
        elif token.token == "greater_than":
            write = " > $" + token.value
        elif token.token == "greater_than_or_equal":
            write = " >= $" + token.value
        elif token.token == "less_than":
            write = " < $" + token.value
        elif token.token == "less_than_or_equal":
            write = " <= $" + token.value
        elif token.token == "search":
            write_builder = [", $", token.value]
            search_operator = getattr(token, "search_operator", None)
            if search_operator and search_operator == QueryOperator.AND:
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
        elif token.token == "select":
            write_builder = []
            for value in token.value:
                if len(write_builder) > 0:
                    write_builder.append(", ")
                    if value.upper() in Query.rql_keyword:
                        value = "'{0}'".format(value)
                write_builder.append(value)
            write = "".join(write_builder)

        if write is None:
            raise AttributeError(f"{token.token} don't match any of the cases for rql builder")
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

        if token.token in ["search", "lucene", "startsWith", "endsWith", "exists"]:
            where_rql += token.token + "("

        where_rql += token.field_name + Query._get_rql_write_case(token)

        if exact:
            where_rql += ")"
        if proximity:
            where_rql += "," + proximity + ")"
        if fuzzy:
            where_rql += "," + fuzzy + ")"
        if boost:
            where_rql += "," + str(boost) + ")"

        return where_rql

    @staticmethod
    def _add_space_if_needed(query_builder):
        if len(query_builder) > 0:
            if not query_builder[-1].endswith(("(", ")", ",", " ")):
                query_builder.append(" ")

    @staticmethod
    def escape_if_needed(name):
        if name:
            escape = False
            first = True
            for c in name:
                if first:
                    if not c.isalpha() and c != '_' and c != '@':
                        escape = True
                        break
                    first = False
                else:
                    if (not c.isalpha() and not c.isdigit()) and c != '_' and c != '@' and c != '[' and c != ']':
                        escape = True
                        break

            if escape or name.upper() in Query.rql_keyword:
                return "'{0}'".format(name)

        return name

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
        self.assert_no_raw_query()

        if len(self._where_tokens) > 0:
            query_operator = None
            last_token = self._where_tokens[-1]
            if last_token is not None and last_token.token in Query.where_operators:
                query_operator = QueryOperator.AND if self.default_operator == QueryOperator.AND else QueryOperator.OR
            search_operator = getattr(last_token, "search_operator", None)
            if search_operator and search_operator != QueryOperator.OR:
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

    def raw_query(self, query, query_parameters=None):
        """
        To get all the document that equal to the query

        @param str query: The rql query
        @param dict query_parameters: Add query parameters to the query {key : value}
        """
        self.assert_no_raw_query()

        if len(self._where_tokens) != 0 or len(self._select_tokens) != 0 or len(
                self._order_by_tokens) != 0 or len(self._group_by_tokens) != 0:
            raise InvalidOperationException(
                "You can only use raw_query on a new query, without applying any operations "
                "(such as where, select, order_by, group_by, etc)")

        if query_parameters:
            self.query_parameters = query_parameters
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

        field_name = Query.escape_if_needed(field_name)
        self._add_operator_if_needed()

        token = "equals"
        if self.negate:
            self.negate = False
            token = "not_equals"

        self.last_equality = {field_name: value}
        token = _Token(field_name=field_name, value=self.add_query_parameter(value), token=token, exact=exact)
        token.write = self.rql_where_write(token)
        self._where_tokens.append(token)

        return self

    def where_not_equals(self, field_name, value, exact=False):
        if not self.negate:
            self.negate = True
        return self.where_equals(field_name, value, exact)

    def where_exists(self, field_name):
        if field_name is None:
            raise ValueError("None field_name is invalid")

        field_name = Query.escape_if_needed(field_name)

        self._add_operator_if_needed()
        self.negate_if_needed(field_name)
        self._where_tokens.append(_Token(field_name=field_name, value=None, token="exists", write="exists(" +
                                                                                                  field_name + ")"))
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

    def search(self, field_name, search_terms, operator=QueryOperator.OR):
        """
        For more complex text searching

        @param str field_name: The field name in the index you want to query.
        :type str
        @param str search_terms: The terms you want to query
        @param QueryOperator operator: OR or AND
        """

        if field_name is None:
            raise ValueError("None field_name is invalid")

        field_name = Query.escape_if_needed(field_name)
        self._add_operator_if_needed()
        self.negate_if_needed(field_name)

        self.last_equality = {field_name: "(" + search_terms + ")" if ' ' in search_terms else search_terms}
        token = _Token(field_name=field_name, token="search", value=self.add_query_parameter(search_terms),
                       search_operator=operator)
        token.write = self.rql_where_write(token)
        self._where_tokens.append(token)
        return self

    def where_ends_with(self, field_name, value):
        """
        To get all the document that ends with the value in the giving field_name

        @param str field_name:The field name in the index you want to query.
        @param str value: The value will be the fields value you want to query
        """
        if field_name is None:
            raise ValueError("None field_name is invalid")

        field_name = Query.escape_if_needed(field_name)
        self._add_operator_if_needed()
        self.negate_if_needed(field_name)

        self.last_equality = {field_name: value}
        token = _Token(field_name=field_name, token="endsWith", value=self.add_query_parameter(value))
        token.write = self.rql_where_write(token)
        self._where_tokens.append(token)

        return self

    def where_starts_with(self, field_name, value):
        """
        To get all the document that starts with the value in the giving field_name

        @param str field_name:The field name in the index you want to query.
        @param str value: The value will be the fields value you want to query
        """
        if field_name is None:
            raise ValueError("None field_name is invalid")

        field_name = Query.escape_if_needed(field_name)
        self._add_operator_if_needed()
        self.negate_if_needed(field_name)

        self.last_equality = {field_name: value}
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
        field_name = Query.escape_if_needed(field_name)
        self._add_operator_if_needed()
        self.negate_if_needed(field_name)

        token = _Token(field_name=field_name, value=self.add_query_parameter(list(Utils.unpack_iterable(values))),
                       token="in", exact=exact)
        token.write = self.rql_where_write(token)
        self._where_tokens.append(token)

        return self

    def where_between(self, field_name, start, end, exact=False):
        field_name = Query.escape_if_needed(field_name)

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

    def where_greater_than(self, field_name, value):
        field_name = Query.escape_if_needed(field_name)
        self._add_operator_if_needed()
        self.negate_if_needed(field_name)

        if isinstance(value, timedelta):
            value = Utils.timedelta_tick(value)

        token = _Token(field_name=field_name, token="greater_than", value=self.add_query_parameter(value))
        token.write = self.rql_where_write(token)
        self._where_tokens.append(token)

        return self

    def where_greater_than_or_equal(self, field_name, value):
        field_name = Query.escape_if_needed(field_name)
        self._add_operator_if_needed()
        self.negate_if_needed(field_name)

        if isinstance(value, timedelta):
            value = Utils.timedelta_tick(value)

        token = _Token(field_name=field_name, token="greater_than_or_equal", value=self.add_query_parameter(value))
        token.write = self.rql_where_write(token)
        self._where_tokens.append(token)

        return self

    def where_less_than(self, field_name, value):
        field_name = Query.escape_if_needed(field_name)
        self._add_operator_if_needed()
        self.negate_if_needed(field_name)

        if isinstance(value, timedelta):
            value = Utils.timedelta_tick(value)

        token = _Token(field_name=field_name, token="less_than", value=self.add_query_parameter(value))
        token.write = self.rql_where_write(token)
        self._where_tokens.append(token)

        return self

    def where_less_than_or_equal(self, field_name, value):
        field_name = Query.escape_if_needed(field_name)
        self._add_operator_if_needed()
        self.negate_if_needed(field_name)

        if isinstance(value, timedelta):
            value = Utils.timedelta_tick(value)

        token = _Token(field_name=field_name, token="less_than_or_equal", value=self.add_query_parameter(value))
        token.write = self.rql_where_write(token)
        self._where_tokens.append(token)

        return self

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
        field_name = Query.escape_if_needed(field_name)
        self._order_by_tokens.append(
            _Token(field_name=field_name, token="order_by", write=field_name, descending=descending,
                   ordering=ordering))

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

    def distinct(self):
        if self.is_distinct:
            raise InvalidOperationException("This is already a distinct query.")
        self.is_distinct = True
        self._select_tokens.insert(0, _Token(token="distinct"))
        return self

    def select(self, *args):
        """
        @param str args: The fields to fetch
        """

        if len(args) > 0:
            self.fields_to_fetch = args
            if len(self._select_tokens) == 0:
                token = _Token(token="select", value=args)
                token.write = self._get_rql_write_case(token)
                self._select_tokens.append(token)
            else:
                replaced = False
                for token in self._select_tokens:
                    if token.token == "select":
                        token.value = args
                        token.write = self._get_rql_write_case(token)
                        replaced = True
                        break
                if not replaced:
                    token = _Token(token="select", value=args)
                    token.write = self._get_rql_write_case(token)
                    self._select_tokens.append(token)
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
            last.write = self.rql_where_write(last)
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

    def include(self, path):
        self.includes.add(path)
        return self

    def get_last_equality_term(self):
        """
        The last term that we asked the query to use equals on
        """
        return self.last_equality

    def to_facets(self, facets, start=0, page_size=None):
        """
        Query the facets results for this query using the specified list of facets with the given start and pageSize

        @param List[Facet] facets: List of facets
        @param int start:  Start index for paging
        @param page_size: Paging PageSize. If set, overrides Facet.max_result
        """

        if len(facets) == 0:
            raise ValueError("Facets must contain at least one entry", "facets")
        str_query = self.__str__()
        facet_query = FacetQuery(str_query, None, facets, start, page_size, query_parameters=self.query_parameters,
                                 wait_for_non_stale_results=self.wait_for_non_stale_results,
                                 wait_for_non_stale_results_timeout=self.timeout, cutoff_etag=self.cutoff_etag)

        command = GetFacetsCommand(query=facet_query)
        return self.session.requests_executor.execute(command)

    def get_index_query(self):
        return IndexQuery(query=self.__str__(), query_parameters=self.query_parameters, start=self.start,
                          page_size=self.page_size, cutoff_etag=self.cutoff_etag,
                          wait_for_non_stale_results=self.wait_for_non_stale_results,
                          wait_for_non_stale_results_timeout=self.timeout)

    def _execute_query(self):
        conventions = self.session.conventions
        end_time = time.time() + self.timeout.seconds
        query = self._build_query()
        while True:
            index_query = IndexQuery(query=query, query_parameters=self.query_parameters, start=self.start,
                                     page_size=self.page_size, cutoff_etag=self.cutoff_etag,
                                     wait_for_non_stale_results=self.wait_for_non_stale_results,
                                     wait_for_non_stale_results_timeout=self.timeout)

            query_command = QueryOperation(session=self.session, index_name=self.index_name,
                                           index_query=index_query,
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
        response_includes = response.pop("Includes", None)
        self.session.save_includes(response_includes)
        for result in response_results:
            entity, metadata, original_metadata, original_document = Utils.convert_to_entity(result, self.object_type,
                                                                                             conventions,
                                                                                             self.nested_object_types)
            if self.object_type != dict and not self.fields_to_fetch:
                self.session.save_entity(key=original_metadata.get("@id", None), entity=entity,
                                         original_metadata=original_metadata,
                                         metadata=metadata, original_document=original_document)
            results.append(entity)

        if self._with_statistics:
            return results, response
        return results
