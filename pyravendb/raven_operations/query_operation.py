from pyravendb.commands.raven_commands import QueryCommand
from pyravendb.custom_exceptions.exceptions import *
import logging

logging.basicConfig(filename='query.log', level=logging.DEBUG)


class QueryOperation:
    def __init__(self, session, index_name, index_query, disable_entities_tracking=False, metadata_only=False,
                 index_entries_only=False):
        self._session = session
        self._index_name = index_name
        self._index_query = index_query
        self._disable_entities_tracking = disable_entities_tracking
        self._metadata_only = metadata_only
        self._index_entries_only = index_entries_only

        if self._session.conventions.raise_if_query_page_size_is_not_set and not self._index_query.page_size_set:
            raise InvalidOperationException("""Attempt to query without explicitly specifying a page size.
                                            You can use .take() methods to set maximum number of results.
                                            By default the page size is set to sys.maxsize and can cause
                                            severe performance degradation.""")

    @property
    def index_query(self):
        return self._index_query

    def create_request(self):
        self._session.increment_requests_count()
        logging.debug("Executing query '{0}' on index '{1}'".format(self._index_query.query, self._index_name))

        return QueryCommand(self._session.conventions, self._index_query, self._metadata_only, self._index_entries_only)
