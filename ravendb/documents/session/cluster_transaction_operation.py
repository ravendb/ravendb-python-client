from __future__ import annotations

import abc
from typing import Union, Tuple, TYPE_CHECKING, List, Dict, TypeVar, Type, Optional, Callable

from ravendb.documents.operations.compare_exchange.compare_exchange import (
    CompareExchangeSessionValue,
    CompareExchangeValue,
    CompareExchangeValueState,
)
from ravendb.documents.operations.compare_exchange.operations import (
    GetCompareExchangeValuesOperation,
    GetCompareExchangeValueOperation,
)
from ravendb.documents.session.misc import TransactionMode
from ravendb.documents.session.operations.lazy import (
    LazyGetCompareExchangeValueOperation,
    LazyGetCompareExchangeValuesOperation,
)

from ravendb.tools.utils import CaseInsensitiveDict, CaseInsensitiveSet
from ravendb.documents.operations.compare_exchange.compare_exchange_value_result_parser import (
    CompareExchangeValueResultParser,
)
from ravendb.util.util import StartingWithOptions

if TYPE_CHECKING:
    from ravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
    from ravendb.documents.session.document_session import DocumentSession
    from ravendb import Lazy

_T = TypeVar("_T")


class IClusterTransactionOperationsBase(abc.ABC):
    @abc.abstractmethod
    def delete_compare_exchange_value(self, item_or_key: Union[CompareExchangeValue, str], index: int = None) -> None:
        pass

    @abc.abstractmethod
    def create_compare_exchange_value(self, key: str, item: _T) -> CompareExchangeValue[_T]:
        pass


class IClusterTransactionOperations(IClusterTransactionOperationsBase):
    @property
    @abc.abstractmethod
    def lazily(self) -> ILazyClusterTransactionOperations:
        pass

    @abc.abstractmethod
    def get_compare_exchange_value(self, key: str, object_type: Type[_T] = None) -> Optional[CompareExchangeValue[_T]]:
        pass

    @abc.abstractmethod
    def get_compare_exchange_values(
        self, keys: List[str], object_type: Type[_T]
    ) -> Dict[str, CompareExchangeValue[_T]]:
        pass

    @abc.abstractmethod
    def get_compare_exchange_values_starting_with(
        self,
        starts_with: str,
        start: Optional[int] = None,
        page_size: Optional[int] = None,
        object_type: Optional[Type[_T]] = None,
    ):
        pass


class ClusterTransactionOperationsBase(IClusterTransactionOperationsBase):
    def __init__(self, session: "DocumentSession"):
        if session.transaction_mode != TransactionMode.CLUSTER_WIDE:
            raise RuntimeError(
                "This function is part of cluster transaction session, "
                "in order to use it you have to open the session with ClusterWide option"
            )
        self._session = session
        self._state = CaseInsensitiveDict()

    @property
    def session(self) -> "DocumentSession":
        return self._session

    @property
    def number_of_tracked_compare_exchange_values(self) -> int:
        return len(self._state)

    def is_tracked(self, key: str) -> bool:
        return self.try_get_compare_exchange_value_from_session(key)[0]

    def create_compare_exchange_value(self, key: str, item: _T) -> CompareExchangeValue[_T]:
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
        self, key: str, object_type: Optional[Type[_T]] = None
    ) -> Union[None, CompareExchangeValue[_T]]:
        v, not_tracked = self.get_compare_exchange_value_from_session_internal(key, object_type)
        if not not_tracked:
            return v

        self.session.increment_requests_count()

        value = self.session.operations.send(
            GetCompareExchangeValueOperation(key, dict, False), self.session.session_info
        )
        if value is None:
            self.register_missing_compare_exchange_value(key)
            return None

        session_value = self.register_compare_exchange_value(value)
        if session_value is not None:
            return session_value.get_value(object_type, self.session.conventions)

        return None

    def _get_compare_exchange_values_internal(
        self, keys_or_start_with_options: Union[List[str], StartingWithOptions], object_type: Optional[Type[_T]]
    ) -> Dict[str, Optional[CompareExchangeValue[_T]]]:
        start_with = isinstance(keys_or_start_with_options, StartingWithOptions)
        if start_with:
            self._session.increment_requests_count()
            values = self._session.operations.send(
                GetCompareExchangeValuesOperation(keys_or_start_with_options, dict), self.session.session_info
            )

            results = {}

            for key, value in values.items():
                if value is None:
                    self.register_missing_compare_exchange_value(key)
                    results[key] = None
                    continue

                session_value = self.register_compare_exchange_value(value)
                results[key] = session_value.get_value(object_type, self.session.conventions)

            return results

        results, not_tracked_keys = self.get_compare_exchange_values_from_session_internal(
            keys_or_start_with_options, object_type
        )

        if not not_tracked_keys:
            return results

        self.session.increment_requests_count()
        keys_array = not_tracked_keys
        values: Dict[str, CompareExchangeValue[dict]] = self.session.operations.send(
            GetCompareExchangeValuesOperation(keys_array, dict), self.session.session_info
        )

        for key in keys_array:
            value = values.get(key)
            if value is None:
                self.register_missing_compare_exchange_value(key)
                results[key] = None
                continue

            session_value = self.register_compare_exchange_value(value)
            results[value.key] = session_value.get_value(object_type, self.session.conventions)

        return results

    def get_compare_exchange_value_from_session_internal(
        self, key: str, object_type: Optional[Type[_T]] = None
    ) -> Tuple[Union[None, CompareExchangeValue[_T]], bool]:
        session_value_result = self.try_get_compare_exchange_value_from_session(key)
        if session_value_result[0]:
            not_tracked = False
            return session_value_result[1].get_value(object_type, self.session.conventions), not_tracked

        not_tracked = True
        return None, not_tracked

    def get_compare_exchange_values_from_session_internal(
        self, keys: List[str], object_type: Optional[Type[_T]]
    ) -> Tuple[CaseInsensitiveDict[str, CompareExchangeValue[_T]], Optional[CaseInsensitiveSet]]:
        not_tracked_keys = None
        results = CaseInsensitiveDict()

        if not keys:
            return results, None

        for key in keys:
            success, session_value = self.try_get_compare_exchange_value_from_session(key)
            if success:
                results[key] = session_value.get_value(object_type, self.session.conventions)
                continue

            if not_tracked_keys is None:
                not_tracked_keys = CaseInsensitiveSet()

            not_tracked_keys.add(key)

        return results, not_tracked_keys

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
            for key, value in values.items():
                self.register_compare_exchange_value(
                    CompareExchangeValueResultParser.get_single_value(dict, value, False, self.session.conventions)
                )

    def register_compare_exchange_value(self, value: CompareExchangeValue[_T]) -> CompareExchangeSessionValue[_T]:
        if self.session.no_tracking:
            return CompareExchangeSessionValue(value=value)
        session_value: CompareExchangeSessionValue = self._state.get(value.key)
        if session_value is None:
            session_value = CompareExchangeSessionValue(value=value)
            self._state[value.key] = session_value
            return session_value

        session_value.update_value(value)
        return session_value

    # todo: ensure its ok - mutable list as argument - is there Java Reference class equivalent in Python?
    def try_get_compare_exchange_value_from_session(self, key: str) -> Tuple[bool, CompareExchangeSessionValue]:
        value = self._state.get(key)
        return value is not None, value

    def prepare_compare_exchange_entities(self, result: "InMemoryDocumentSessionOperations.SaveChangesData") -> None:
        if len(self._state) == 0:
            return

        for key, value in self._state.items():
            command = value.get_command(self.session.conventions)
            if command is None:
                continue
            result.session_commands.append(command)

    def update_state(self, key: str, index: int) -> None:
        got_value, value = self.try_get_compare_exchange_value_from_session(key)
        if not got_value:
            return
        value.update_state(index)


class ClusterTransactionOperations(ClusterTransactionOperationsBase, IClusterTransactionOperations):
    def __init__(self, session: "DocumentSession"):
        super().__init__(session)

    @property
    def lazily(self) -> ILazyClusterTransactionOperations:
        return LazyClusterTransactionOperations(self.session)

    def get_compare_exchange_value(self, key: str, object_type: Type[_T] = None) -> Optional[CompareExchangeValue[_T]]:
        return self._get_compare_exchange_value_internal(key, object_type)

    def get_compare_exchange_values(
        self, keys: List[str], object_type: Type[_T]
    ) -> Dict[str, CompareExchangeValue[_T]]:
        return super()._get_compare_exchange_values_internal(keys, object_type)

    def get_compare_exchange_values_starting_with(
        self,
        starts_with: str,
        start: Optional[int] = None,
        page_size: Optional[int] = None,
        object_type: Optional[Type[_T]] = None,
    ):
        return self._get_compare_exchange_values_internal(
            StartingWithOptions(starts_with, start, page_size), object_type
        )


# this class helps to expose better typehints without tons of methods and fields from ClusterTransactionOperationsBase
class ILazyClusterTransactionOperations(abc.ABC):
    @abc.abstractmethod
    def get_compare_exchange_value(
        self,
        key: str,
        object_type: Optional[Type[_T]] = None,
        on_eval: Optional[Callable[[CompareExchangeValue[_T]], None]] = None,
    ) -> Lazy[CompareExchangeValue[_T]]:
        pass

    @abc.abstractmethod
    def get_compare_exchange_values(
        self,
        keys: List[str],
        object_type: Type[_T] = None,
        on_eval: Callable[[Dict[str, CompareExchangeValue[_T]]], None] = None,
    ) -> Optional[Lazy[Dict[str, CompareExchangeValue[_T]]]]:
        pass


class LazyClusterTransactionOperations(ClusterTransactionOperations, ILazyClusterTransactionOperations):
    def get_compare_exchange_value(
        self,
        key: str,
        object_type: Optional[Type[_T]] = None,
        on_eval: Optional[Callable[[CompareExchangeValue[_T]], None]] = None,
    ) -> Lazy[CompareExchangeValue[_T]]:
        return self.session.add_lazy_operation(
            CompareExchangeValue,
            LazyGetCompareExchangeValueOperation(self, key, self.session.conventions, object_type),
            on_eval,
        )

    def get_compare_exchange_values(
        self,
        keys: List[str],
        object_type: Type[_T] = None,
        on_eval: Callable[[Dict[str, CompareExchangeValue[_T]]], None] = None,
    ) -> Optional[Lazy[Dict[str, CompareExchangeValue[_T]]]]:
        return self.session.add_lazy_operation(
            dict,
            LazyGetCompareExchangeValuesOperation(self, keys, self.session.conventions, object_type),
            on_eval,
        )
