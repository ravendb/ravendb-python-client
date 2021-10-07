from typing import Optional, List

from pyravendb.commands.commands_results import GetDocumentsResult
from pyravendb.data.timeseries import TimeSeriesRange
from pyravendb.documents.session.document_info import DocumentInfo
from pyravendb.store.document_session import DocumentSession
from pyravendb.tools.utils import CaseInsensitiveSet, Utils, CaseInsensitiveDict


class LoadOperation:
    def __init__(
        self,
        session: DocumentSession,
        keys: List[str] = None,
        includes: Optional[List[str]] = None,
        counters_to_include: Optional[List[str]] = None,
        compare_exchange_values_to_include: Optional[List[str]] = None,
        include_all_counters: Optional[bool] = None,
        time_series_to_include: Optional[List[TimeSeriesRange]] = None,
    ):
        self._session = session
        self._keys = keys
        self._includes = includes
        self._counters_to_include = counters_to_include
        self._compare_exchange_values_to_include = compare_exchange_values_to_include
        self._time_series_to_include = time_series_to_include
        self._include_all_counters = include_all_counters
        self._time_series_to_include = time_series_to_include
        self._results_set = False
        self._results = None

    def create_request(self):
        if self._session.check_if_already_included(
            self._keys, list(self._includes) if self._includes is not None else None
        ):
            return None

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
        distinct = CaseInsensitiveSet()
        self._keys = list([distinct.add(key) for key in keys if key and key.strip()])
        return self

    def get_document(self, object_type: type, key: str = None):
        if self._session.no_tracking:
            if not self._results_set and len(self._keys) > 0:
                raise RuntimeError("Cannot execute get_document before operation execution.")

            if (not self._results) or not self._results["Results"]:
                return None

            document = self._results["Results"][0]
            if not document:
                return None

            document_info = DocumentInfo.get_new_document_info(document)
            return self._session.track_entity(object_type, document_info=document_info)

        if not key:
            key = self._keys[0]

        doc = self._session.advanced.get_document_by_id(key)
        if doc:
            return self._session.track_entity(object_type, doc)

        doc = self._session.included_documents_by_id.get(key)
        if doc:
            return self._session.track_entity(object_type, doc)

        return None

    def get_documents(self, object_type: type):
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
                    entity_type=object_type, document_info=new_document_info
                )

            return final_results

        for key in self._keys:
            if not key:
                continue
            final_results[key] = self.get_document(object_type, key)

        return final_results

    def set_result(self, result: GetDocumentsResult) -> None:
        self._results_set = True
        if self._session.no_tracking:
            self._results = result
            return

        if not result:
            self._session.register_missing(self._keys)
            return

        self._session.register_includes(result.includes)

        if self._include_all_counters or self._counters_to_include:
            self._session.register_counters(
                result.counter_includes, self._keys, self._counters_to_include, self._include_all_counters
            )

        if self._time_series_to_include:
            self._session.register_time_series(result.time_series_includes)
