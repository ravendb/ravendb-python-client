from __future__ import annotations
import logging
from typing import Optional, List, TYPE_CHECKING, Type, TypeVar

from pyravendb.commands.commands_results import GetDocumentsResult
from pyravendb.data.timeseries import TimeSeriesRange
from pyravendb.documents.commands import GetDocumentsCommand
from pyravendb.documents.session.document_info import DocumentInfo
from pyravendb.tools.utils import CaseInsensitiveSet, CaseInsensitiveDict, Utils

if TYPE_CHECKING:
    from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations

_T = TypeVar("_T")


class LoadOperation:

    logger = logging.getLogger("load_operation")

    def __init__(
        self,
        session: InMemoryDocumentSessionOperations,
        keys: List[str] = None,
        includes: Optional[List[str]] = None,
        counters_to_include: Optional[List[str]] = None,
        compare_exchange_values_to_include: Optional[List[str]] = None,
        include_all_counters: Optional[bool] = None,
        time_series_to_include: Optional[List[TimeSeriesRange]] = None,
    ):
        self._session = session
        self._keys = keys
        self._includes = includes if includes is not None else []
        self._counters_to_include = counters_to_include if counters_to_include is not None else []
        self._compare_exchange_values_to_include = (
            compare_exchange_values_to_include if compare_exchange_values_to_include is not None else []
        )
        self._time_series_to_include = time_series_to_include if time_series_to_include is not None else []
        self._include_all_counters = include_all_counters
        self._time_series_to_include = time_series_to_include if time_series_to_include is not None else []
        self._results_set = False
        self._results = []

    def create_request(self) -> GetDocumentsCommand:
        if self._session.check_if_id_already_included(
            self._keys, list(self._includes) if self._includes is not None else None
        ):
            return None

        self._session.increment_requests_count()

        self.logger.info(f"Requesting the following ids {','.join(self._keys)} from {self._session.store_identifier}")

        if self._include_all_counters:
            return GetDocumentsCommand(
                GetDocumentsCommand.GetDocumentsByIdsCommandOptions(
                    self._keys,
                    self._includes,
                    False,
                    self._time_series_to_include,
                    self._compare_exchange_values_to_include,
                    [],
                    True,
                    self._session.conventions,
                )
            )

        return GetDocumentsCommand(
            GetDocumentsCommand.GetDocumentsByIdsCommandOptions(
                self._keys,
                self._includes,
                False,
                self._time_series_to_include,
                self._compare_exchange_values_to_include,
                self._counters_to_include,
                False,
                self._session.conventions,
            )
        )

    def by_key(self, key: str):
        if not key:
            return self
        if self._keys is None:
            self._keys = [key]
        return self

    def with_includes(self, includes: List[str]):
        self._includes = includes
        return self

    def with_compare_exchange(self, cmpxch: List[str]):
        self._compare_exchange_values_to_include = cmpxch
        return self

    def with_counters(self, counters: List[str]):
        if counters:
            self._counters_to_include = counters
        return self

    def with_all_counters(self):
        self._include_all_counters = True
        return self

    def with_time_series(self, time_series: List[TimeSeriesRange]):
        if time_series:
            self._time_series_to_include = time_series
        return self

    def by_keys(self, keys: List[str]):
        distinct = CaseInsensitiveSet(filter(lambda key: key and key.strip(), keys))
        self._keys = list(distinct)
        return self

    def get_document(self, object_type: Type[_T]) -> _T:
        if self._session.no_tracking:
            if not self._results_set and len(self._keys) > 0:
                raise RuntimeError("Cannot execute get_document before operation execution.")

            if (not self._results) or not self._results["Results"]:
                return None

            document = self._results["Results"][0]
            if not document:
                return None

            document_info = DocumentInfo.get_new_document_info(document)
            return self._session.track_entity(object_type, document_found=document_info)
        return self.__get_document(object_type, self._keys[0])

    def __get_document(self, object_type: Type[_T], key: str) -> _T:
        if key is None:
            # todo: fix these ugly protected calls below
            return Utils.get_default_value(object_type)

        if self._session.is_deleted(key):
            return Utils.get_default_value(object_type)

        doc = self._session.documents_by_id.get(key)
        if doc is not None:
            return self._session.track_entity(object_type, document_found=doc)

        doc = self._session.included_documents_by_id.get(key)
        if doc is not None:
            return self._session.track_entity(object_type, document_found=doc)

        return Utils.get_default_value(object_type)

    def get_documents(self, object_type: Type[_T]) -> CaseInsensitiveDict[str, _T]:
        final_results = CaseInsensitiveDict()
        if self._session.no_tracking:
            if (not self._results_set) and len(self._keys) > 0:
                raise ValueError("Cannot execute 'get_documents before operation execution.")

            for key in self._keys:
                if not key:
                    continue
                final_results[key] = None

            if (not self._results) or not self._results["Results"]:
                return final_results

            for document in self._results["Results"]:
                if not document:
                    continue
                new_document_info = DocumentInfo.get_new_document_info(document)
                final_results[new_document_info.key] = self._session.track_entity(
                    entity_type=object_type, document_found=new_document_info
                )

            return final_results

        for key in self._keys:
            if not key:
                continue
            final_results[key] = self.__get_document(object_type, key)

        return final_results

    def set_result(self, result: GetDocumentsResult) -> None:
        self._results_set = True
        if self._session.no_tracking:
            self._results = result
            return

        if not result:
            self._session.register_missing(*self._keys)
            return

        self._session.register_includes(result.includes)

        if self._include_all_counters or self._counters_to_include:
            self._session.register_counters(
                result.counter_includes, self._keys, self._counters_to_include, self._include_all_counters
            )

        if self._time_series_to_include:
            self._session.register_time_series(result.time_series_includes)

        if self._compare_exchange_values_to_include:
            self._session.get_cluster_session().register_compare_exchange_values(result.compare_exchange_includes)

        for document in result.results:
            if document is None:
                continue

            new_document_info = DocumentInfo.get_new_document_info(document)
            self._session.documents_by_id.add(new_document_info)

        for key in self._keys:
            value = self._session.documents_by_id.get(key, None)
            if value is None:
                self._session.register_missing(key)

        self._session.register_missing_includes(result.results, result.includes, self._includes)
