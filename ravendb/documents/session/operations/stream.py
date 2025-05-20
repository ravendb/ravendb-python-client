import typing
from typing import Optional, Dict, Iterator

import requests

from ravendb.documents.commands.stream import QueryStreamCommand, StreamResultResponse, StreamCommand
from ravendb.documents.queries.index_query import IndexQuery
from ravendb.documents.session.stream_statistics import StreamQueryStatistics
from ravendb.exceptions.exceptions import IndexDoesNotExistException
from ravendb.tools.parsers import JSONLRavenStreamParser
from ravendb.tools.utils import Utils
from ravendb.primitives import constants


if typing.TYPE_CHECKING:
    from ravendb.documents.session.document_session import InMemoryDocumentSessionOperations


class StreamOperation:
    def __init__(
        self, session: "InMemoryDocumentSessionOperations", statistics: Optional[StreamQueryStatistics] = None
    ):
        self._session = session
        self._statistics = statistics
        self._is_query_stream: Optional[bool] = None

    def create_request(self, query: IndexQuery) -> QueryStreamCommand:
        self._is_query_stream = True

        if query.wait_for_non_stale_results:
            raise RuntimeError(
                "Since stream() does not wait for indexing (by design), "
                "streaming query with wait_for_non_stale_results is not supported"
            )

        self._session.increment_requests_count()

        return QueryStreamCommand(self._session.conventions, query)

    def create_request_for_starts_with(
        self, starts_with: str, matches: str, start: int, page_size: int, exclude: Optional[str], start_after: str
    ) -> StreamCommand:
        sb = ["streams/docs?"]

        if starts_with is not None:
            sb.append("startsWith=")
            sb.append(Utils.escape(starts_with))
            sb.append("&")
        if matches is not None:
            sb.append("matches=")
            sb.append(Utils.escape(matches))
            sb.append("&")
        if exclude is not None:
            sb.append("exclude=")
            sb.append(Utils.escape(exclude))
            sb.append("&")
        if start_after is not None:
            sb.append("startAfter=")
            sb.append(Utils.escape(start_after))
            sb.append("&")
        if start != 0:
            sb.append("start")
            sb.append(start)
            sb.append("&")
        if page_size != constants.int_max:
            sb.append("pageSize=")
            sb.append(starts_with)
            sb.append("&")

        sb.append("format=jsonl")
        return StreamCommand("".join(sb))

    def set_result(self, response: StreamResultResponse) -> Iterator[Dict]:
        if response is None or response.stream_iterator is None:
            raise IndexDoesNotExistException("The index does not exists, failed to stream results")

        parser = JSONLRavenStreamParser(response.stream_iterator)

        if self._is_query_stream:
            self._handle_stream_query_stats(parser, self._statistics)

        return StreamOperation.YieldStreamResults(response, parser)

    @staticmethod
    def _handle_stream_query_stats(
        parser: JSONLRavenStreamParser, stream_query_statistics: StreamQueryStatistics
    ) -> None:
        stats_json = parser.next_query_statistics()

        if "ResultEtag" not in stats_json:
            raise RuntimeError("Expected ResultEtag field")

        result_etag = stats_json["ResultEtag"]

        if "IsStale" not in stats_json:
            raise RuntimeError("Expected IsStale field")

        is_stale = stats_json["IsStale"]

        if "IndexName" not in stats_json:
            raise RuntimeError("Expected IndexName field")

        index_name = stats_json["IndexName"]

        if "TotalResults" not in stats_json:
            raise RuntimeError("Expected TotalResults field")

        total_results = stats_json["TotalResults"]

        if "IndexTimestamp" not in stats_json:
            raise RuntimeError("Expected IndexTimestamp field")

        index_timestamp_str = stats_json["IndexTimestamp"]

        if stream_query_statistics is None:
            return

        stream_query_statistics.index_name = index_name
        stream_query_statistics.is_stale = is_stale
        stream_query_statistics.total_results = total_results
        stream_query_statistics.result_etag = result_etag
        stream_query_statistics.index_timestamp = Utils.string_to_datetime(index_timestamp_str)

    class YieldStreamResults(Iterator[Dict]):
        def __init__(self, response: StreamResultResponse, parser: JSONLRavenStreamParser):  # todo: parser
            self._response = response
            self._parser = parser

        def __iter__(self):
            return self

        def __next__(self) -> Dict:
            try:
                json_dict = self._parser.next_item()
            except Exception as e:
                if isinstance(e, requests.exceptions.StreamConsumedError):
                    raise StopIteration()
                raise e
            return json_dict

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self._response.response.close()
