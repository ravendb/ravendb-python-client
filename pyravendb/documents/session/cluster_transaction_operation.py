from typing import Union, Tuple, Optional

from pyravendb.documents.operations.compare_exchange.compare_exchange import (
    CompareExchangeSessionValue,
    CompareExchangeValue,
    CompareExchangeValueState,
)
from pyravendb.documents.operations.compare_exchange.compare_exchange_value_result_parser import (
    CompareExchangeValueResultParser,
)
from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
from pyravendb.documents.session.transaction_mode import TransactionMode
from pyravendb.store.document_session import DocumentSession
from pyravendb.tools.utils import CaseInsensitiveDict, CaseInsensitiveSet


class ClusterTransactionOperationsBase:
    def __init__(self, session: DocumentSession):
        if session.transaction_mode != TransactionMode.CLUSTER_WIDE:
            raise RuntimeError(
                "This function is part of cluster transaction session, "
                "in order to use it you have to open the session with ClusterWide option"
            )
        self._session = session
        self._state = CaseInsensitiveDict()

    @property
    def session(self) -> DocumentSession:
        return self._session

    @property
    def number_of_tracked_compare_exchange_values(self) -> int:
        return len(self._state)

    def is_tracked(self, key: str) -> bool:
        return self.try_get_compare_exchange_value_from_session(key)[0]

    def create_compare_exchange_value(self, key: str, item) -> CompareExchangeValue:
        if key is None:
            raise ValueError("Key cannot be None")

        value_is_not_none, value = self.try_get_compare_exchange_value_from_session(key)
        if not value_is_not_none:
            value = CompareExchangeSessionValue(key, 0, CompareExchangeValueState.NONE)
            self._state[key] = value
        return value.create(item)

    def delete_compare_exchange_value(self, item_or_key: Union[CompareExchangeValue, str], index: int = None) -> None:
        if not item_or_key or not isinstance(item_or_key, (str, CompareExchangeValue)):
            raise ValueError("Item or key cannot be None and it must be either str or CompareExchangeValue")
        if isinstance(item_or_key, CompareExchangeValue):
            if index:
                raise ValueError(
                    "Unexpected argument: passing index is mutually exclusive with passing CompareExchangeValue"
                )
            key = item_or_key.key
        else:
            key = item_or_key

        value_is_not_none, value = self.try_get_compare_exchange_value_from_session(key)
        if not value_is_not_none:
            value = CompareExchangeSessionValue(key, 0, CompareExchangeValueState.NONE)
            self._state[key] = value
        value.delete(index if index else item_or_key.index)

    def clear(self) -> None:
        self._state.clear()

    def _get_compare_exchange_value_internal(
        self, object_type: type, keys: [Union[list[str], str]]
    ) -> Union[CompareExchangeValue, dict[str, CompareExchangeValue]]:
        if isinstance(keys, str):
            keys = [keys]

    def _get_compare_exchange_value_internal_page(
        self, object_type: type, starts_with: str, start: int, page_size: int
    ) -> dict[str, CompareExchangeValue]:
        pass

    def get_compare_exchange_value_from_session_internal(
        self, object_type: type, key_or_keys: Union[str, list[str]], not_tracked: Union[bool, CaseInsensitiveSet[str]]
    ) -> Union[CompareExchangeValue, dict[str, CompareExchangeValue]]:
        if not object_type:
            raise ValueError("Object_type cannot be None")

        if isinstance(key_or_keys, str):
            key_or_keys = [key_or_keys]
            if not isinstance(not_tracked, bool):
                raise TypeError(f"Expected bool if passing single string, got {type(not_tracked)}")
            not_tracked = CaseInsensitiveSet([key_or_keys]) if not_tracked else CaseInsensitiveSet()

        if isinstance(key_or_keys, list):
            if not isinstance(not_tracked, set):
                raise ValueError(f"Expected set of strings if passing multiple keys, got {type(not_tracked)}")

        results = CaseInsensitiveDict()
        for key in key_or_keys:
            value_is_not_none, value = self.try_get_compare_exchange_value_from_session(key)
            if value_is_not_none:
                results[key] = value.value.get_value(object_type, self.session.conventions)
                continue

            if not not_tracked:
                not_tracked = CaseInsensitiveSet()

            not_tracked.add(key)

        return results

    def register_missing_compare_exchange_value(self, key: str) -> CompareExchangeSessionValue:
        value = CompareExchangeSessionValue(key, -1, CompareExchangeValueState.MISSING)
        if self.session.no_tracking:
            return value

        self._state[key] = value
        return value

    def register_compare_exchange_values(self, values: dict) -> None:
        if self.session.no_tracking:
            return

        if values:
            for key, value in values:
                self.register_compare_exchange_values(
                    CompareExchangeValueResultParser.get_single_value(dict, value, False, self.session.conventions)
                )

    def register_compare_exchange_value(self, value: CompareExchangeValue) -> CompareExchangeSessionValue:
        if self.session.no_tracking:
            return CompareExchangeSessionValue(value=value)
        session_value: CompareExchangeSessionValue = self._state.get(value.key)
        if not session_value:
            session_value = CompareExchangeSessionValue(value=value)
            self._state[value.key] = session_value
            return session_value

        session_value.update_value(value)
        return session_value

    # todo: ensure its ok - mutable list as argument - is there Java Reference class equivalent in Python?
    def try_get_compare_exchange_value_from_session(self, key: str) -> Tuple[bool, CompareExchangeSessionValue]:
        value = self._state.get(key)
        return value is not None, value

    def prepare_compare_exchange_entities(self, result: InMemoryDocumentSessionOperations.SaveChangesData) -> None:
        if len(self._state) == 0:
            return

        for key, value in self._state.items():
            command = value.command(self.session.conventions)
            if command is None:
                continue
            result.session_commands.append(command)

    def update_state(self, key: str, index: int) -> None:
        got_value, value = self.try_get_compare_exchange_value_from_session(key)
        if not got_value:
            return
        value.update_state(index)


class ClusterTransactionOperations(ClusterTransactionOperationsBase):
    def __init__(self, session: DocumentSession):
        super().__init__(session)

    def lazily(self):
        raise NotImplementedError()

    def get_compare_exchange_value(self, object_type: type, *keys: str) -> CompareExchangeValue:
        return super()._get_compare_exchange_value_internal(object_type, keys)

    def get_compare_exchange_value_page(self, object_type: type, starts_with: str, start: int = 0, page_size: int = 25):
        return super()._get_compare_exchange_value_internal_page(object_type, starts_with, start, page_size)
