from enum import Enum
from pyravendb.custom_exceptions.exceptions import ErrorResponseException
from pyravendb.data.indexes import IndexQuery
from pyravendb.tools.utils import Utils
import time


class Query(object):
    def __init__(self, session):
        self.session = session
        self.query_builder = ""
        self.negate = False
        self._sort_hints = set()
        self._sort_fields = set()

    def __call__(self, object_type=None, index_name=None, using_default_operator=None,
                 wait_for_non_stale_results=False, includes=None, with_statistics=False):
        """
        @param index_name: The index name we want to apply
        :type index_name: str
        @param object_type: The type of the object we want to track the entity too
        :type Type
        @param using_default_operator: If None, by default will use OR operator for the query (can use for OR or AND)
        @param with_statistics: Make it True to get the query statistics as well
        :type bool
        """
        if not index_name:
            index_name = "dynamic"
            if object_type is not None:
                index_name += "/{0}".format(self.session.conventions.default_transform_plural(object_type.__name__))
        self.index_name = index_name
        self.object_type = object_type
        self.using_default_operator = using_default_operator
        self.wait_for_non_stale_results = wait_for_non_stale_results
        self.includes = includes
        if includes and not isinstance(self.includes, list):
            self.includes = [self.includes]
        self._with_statistics = with_statistics
        return self

    def __iter__(self):
        return self._execute_query().__iter__()

    def _lucene_builder(self, value, action=None):
        lucene_text = Utils.to_lucene(value, action=action)

        if len(self.query_builder) > 0 and not self.query_builder.endswith(' '):
            self.query_builder += ' '
        if self.negate:
            self.negate = False
            self.query_builder += '-'

        return lucene_text

    def where_equals(self, field_name, value):
        """
        To get all the document that equal to the value in the given field_name

        @param field_name:The field name in the index you want to query.
        :type str
        @param value: The value will be the fields value you want to query
        """
        if field_name is None:
            raise ValueError("None field_name is invalid")

        if value is not None and not isinstance(value, str) and field_name is not None:
            sort_hint = self.session.conventions.get_default_sort_option(type(value).__name__)
            if sort_hint:
                self._sort_hints.add("SortHint-{0}={1}".format(field_name, sort_hint))

        lucene_text = self._lucene_builder(value, action="equal")
        self.query_builder += "{0}:{1}".format(field_name, lucene_text)
        return self

    def where(self, **kwargs):
        """
        To get all the document that equal to the value within kwargs with the specific key

        @param kwargs: the keys of the kwargs will be the fields name in the index you want to query.
        The value will be the the fields value you want to query
        (if kwargs[field_name] is a list it will behave has the where_in method)
        """
        for field_name in kwargs:
            if isinstance(kwargs[field_name], list):
                self.where_in(field_name, kwargs[field_name])
            else:
                self.where_equals(field_name, kwargs[field_name])
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
            sort_hint = self.session.conventions.get_default_sort_option(type(value).__name__)
            if sort_hint:
                self._sort_hints.add("SortHint-{0}={1}".format(field_name, sort_hint))

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
            sort_hint = self.session.conventions.get_default_sort_option(type(value).__name__)
            if sort_hint:
                self._sort_hints.add("SortHint-{0}={1}".format(field_name, sort_hint))

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
        value = start or end
        if self.session.conventions.uses_range_type(value) and not field_name.endswith("_Range"):
            sort_hint = self.session.conventions.get_default_sort_option(type(value).__name__)
            if sort_hint:
                self._sort_hints.add("SortHint-{0}={1}".format(field_name, sort_hint))
            field_name = "{0}_Range".format(field_name)
        lucene_text = self._lucene_builder([start, end], action="between")
        self.query_builder += "{0}:{1}".format(field_name, lucene_text)
        return self

    def where_between_or_equal(self, field_name, start, end):
        value = start or end
        if self.session.conventions.uses_range_type(value) and not field_name.endswith("_Range"):
            sort_hint = self.session.conventions.get_default_sort_option(type(value).__name__)
            if sort_hint:
                self._sort_hints.add("SortHint-{0}={1}".format(field_name, sort_hint))
            field_name = "{0}_Range".format(field_name)
        lucene_text = self._lucene_builder([start, end], action="equal_between")
        self.query_builder += "{0}:{1}".format(field_name, lucene_text)
        return self

    def where_greater_than(self, field_name, value):
        return self.where_between(field_name, value, None)

    def where_greater_than_or_equal(self, field_name, value):
        return self.where_between_or_equal(field_name, value, None)

    def where_less_then(self, field_name, value):
        return self.where_between(field_name, None, value)

    def where_less_then_or_equal(self, field_name, value):
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
        if len(self.query_builder) > 0:
            self.query_builder += " AND"
        return self

    def or_else(self):
        if len(self.query_builder) > 0:
            self.query_builder += " OR"
        return self

    def add_not(self):
        self.negate = True
        return self

    def _execute_query(self):
        self.session.increment_requests_count()
        conventions = self.session.conventions
        start_time = time.time()
        end_time = start_time + conventions.timeout
        while True:
            response = self.session.database_commands. \
                query(self.index_name, IndexQuery(self.query_builder, default_operator=self.using_default_operator,
                                                  sort_hints=self._sort_hints, sort_fields=self._sort_fields,
                                                  wait_for_non_stale_results=self.wait_for_non_stale_results),
                      includes=self.includes)
            if response["IsStale"] and self.wait_for_non_stale_results:
                if start_time > end_time:
                    raise ErrorResponseException("The index is still stale after reached the timeout")
                    time.sleep(0.1)
                continue
            break

        results = []
        response_results = response.pop("Results")
        response_includes = response.pop("Includes")
        for result in response_results:
            entity, metadata, original_metadata = Utils.convert_to_entity(result, self.object_type, conventions)
            self.session.save_entity(key=original_metadata["@id"], entity=entity, original_metadata=original_metadata,
                                     metadata=metadata, document=result)
            results.append(entity)
        self.session.save_includes(response_includes)
        if self._with_statistics:
            return results, response
        return results


class QueryOperator(Enum):
    OR = "OR"
    AND = "AND"
