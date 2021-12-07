import json
from typing import Optional, Dict
import requests
from pyravendb.custom_exceptions.exceptions import *
import logging

from pyravendb.http import RavenCommand, ServerNode

logging.basicConfig(filename="query.log", level=logging.DEBUG)


class QueryOperation:
    def __init__(
        self,
        session,
        index_name,
        index_query,
        disable_entities_tracking=False,
        metadata_only=False,
        index_entries_only=False,
    ):
        self._session = session
        self._index_name = index_name
        self._index_query = index_query
        self._disable_entities_tracking = disable_entities_tracking
        self._metadata_only = metadata_only
        self._index_entries_only = index_entries_only

        if self._session.conventions.raise_if_query_page_size_is_not_set and not self._index_query.page_size_set:
            raise InvalidOperationException(
                """Attempt to query without explicitly specifying a page size.
                                            You can use .take() methods to set maximum number of results.
                                            By default the page size is set to sys.maxsize and can cause
                                            severe performance degradation."""
            )

    @property
    def index_query(self):
        return self._index_query

    def create_request(self):
        self._session.increment_requests_count()
        logging.debug("Executing query '{0}' on index '{1}'".format(self._index_query.query, self._index_name))

        return QueryCommand(
            self._session.conventions,
            self._index_query,
            self._metadata_only,
            self._index_entries_only,
        )


class QueryCommand(RavenCommand[Dict]):
    def __init__(self, conventions, index_query, metadata_only=False, index_entries_only=False):
        """
        @param IndexQuery index_query: A query definition containing all information required to query a specified index.
        @param bool metadata_only: True if returned documents should include only metadata without a document body.
        @param bool index_entries_only: True if query results should contain only index entries.
        @return:json
        :rtype:dict
        """
        super(QueryCommand, self).__init__(dict)
        if index_query is None:
            raise ValueError("Invalid index_query")
        if conventions is None:
            raise ValueError("Invalid convention")
        self._conventions = conventions
        self._index_query = index_query
        self._metadata_only = metadata_only
        self._index_entries_only = index_entries_only

    def create_request(self, server_node: ServerNode) -> requests.Request:
        url = f"{server_node.url}/databases/{server_node.database}/queries?query-hash={self._index_query.get_query_hash()}"
        if self._metadata_only:
            url += "&metadataOnly=true"
        if self._index_entries_only:
            url += "&debug=entries"
        data = self._index_query.to_json()

        return requests.Request("POST", url, data=data)

    def is_read_request(self) -> bool:
        return True

    def set_response(self, response: str, from_cache: bool) -> None:
        self.result = json.loads(response)
