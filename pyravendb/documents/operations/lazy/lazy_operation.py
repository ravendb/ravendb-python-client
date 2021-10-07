from typing import Callable

import requests

from pyravendb.documents.queries.query import QueryResult


class LazyOperation:
    def __init__(
        self,
        create_request: requests.Request,
        result: object,
        query_result: QueryResult,
        is_requires_retry: bool,
        handle_response: Callable[[requests.Response], None] = lambda get_response: None,
    ):
        self.create_request = create_request
        self.result = result
        self.query_result = query_result
        self.is_requires_retry = is_requires_retry
        self.handle_response = handle_response
