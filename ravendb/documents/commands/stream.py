import requests
from typing import TypeVar, Generic, Iterator

from ravendb.documents.queries.index_query import IndexQuery
from ravendb.documents.conventions import DocumentConventions
from ravendb.extensions.json_extensions import JsonExtensions
from ravendb.http.http_cache import HttpCache
from ravendb.http.misc import ResponseDisposeHandling
from ravendb.http.server_node import ServerNode
from ravendb.http.raven_command import RavenCommand, RavenCommandResponseType
from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
from ravendb.util.request_utils import RequestUtils

_T = TypeVar("_T")


class StreamResult(Generic[_T]):
    def __init__(
        self, key: str = None, change_vector: str = None, metadata: MetadataAsDictionary = None, document: _T = None
    ):
        self.key = key
        self.change_vector = change_vector
        self.metadata = metadata
        self.document = document


class StreamResultResponse:
    def __init__(self, response: requests.Response, stream_generator: Iterator):
        self.response = response
        self.stream_iterator = stream_generator


class StreamCommand(RavenCommand[StreamResultResponse]):
    def __init__(self, url: str):
        super(StreamCommand, self).__init__(StreamResultResponse)
        if not url or url.isspace():
            raise ValueError("Url cannot be None or empty")

        self._url = url
        self._response_type = RavenCommandResponseType.EMPTY

    def create_request(self, node: ServerNode) -> requests.Request:
        return requests.Request("GET", f"{node.url}/databases/{node.database}/{self._url}")

    def process_response(self, cache: HttpCache, response: requests.Response, url) -> ResponseDisposeHandling:
        try:
            result = StreamResultResponse(response, response.iter_lines())
            self.result = result

            return ResponseDisposeHandling.MANUALLY
        except Exception as e:
            raise RuntimeError("Unable to process stream response", e)

    def send(self, session: requests.Session, request: requests.Request) -> requests.Response:
        prepared_request = session.prepare_request(request)
        RequestUtils.remove_zstd_encoding(prepared_request)
        return session.send(prepared_request, cert=session.cert, stream=True)

    def is_read_request(self) -> bool:
        return True


class QueryStreamCommand(RavenCommand[StreamResultResponse]):
    def __init__(self, conventions: DocumentConventions, query: IndexQuery):
        super(QueryStreamCommand, self).__init__(StreamResultResponse)

        if conventions is None:
            raise ValueError("Conventions cannot be None")

        if query is None:
            raise ValueError("Query cannot be None")

        self._conventions = conventions
        self._index_query = query

        self._response_type = RavenCommandResponseType.EMPTY

    def create_request(self, node: ServerNode) -> requests.Request:
        request = requests.Request("POST")
        request.data = JsonExtensions.write_index_query(self._conventions, self._index_query)
        request.url = f"{node.url}/databases/{node.database}/streams/queries?format=jsonl"
        return request

    def process_response(self, cache: HttpCache, response: requests.Response, url) -> ResponseDisposeHandling:
        try:
            stream_response = StreamResultResponse(response, response.iter_lines())
            self.result = stream_response

            return ResponseDisposeHandling.MANUALLY
        except Exception as e:
            raise RuntimeError("Unable to process stream response: " + e.args[0], e)

    def send(self, session: requests.Session, request: requests.Request) -> requests.Response:
        prepared_request = session.prepare_request(request)
        RequestUtils.remove_zstd_encoding(prepared_request)
        return session.send(prepared_request, cert=session.cert, stream=True)

    def is_read_request(self) -> bool:
        return True
