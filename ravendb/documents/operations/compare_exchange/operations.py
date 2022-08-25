from __future__ import annotations
import json
from typing import Union, Optional, Generic, TypeVar, Dict, TYPE_CHECKING, Type, Collection

import requests

from ravendb import constants
from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.operations.compare_exchange.compare_exchange import (
    CompareExchangeValue,
    CompareExchangeSessionValue,
)
from ravendb.documents.operations.compare_exchange.compare_exchange_value_result_parser import (
    CompareExchangeValueResultParser,
)
from ravendb.documents.operations.definitions import IOperation
from ravendb.http.http_cache import HttpCache
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
from ravendb.documents.session.entity_to_json import EntityToJson
from ravendb.tools.utils import Utils
from ravendb.util.util import RaftIdGenerator, StartingWithOptions

_T = TypeVar("_T")

if TYPE_CHECKING:
    from ravendb.documents.store import DocumentStore


class CompareExchangeResult(Generic[_T]):
    def __init__(self, value: _T, index: int, successful: bool):
        self.value = value
        self.index = index
        self.successful = successful

    @classmethod
    def parse_from_string(
        cls, object_type: type, response_string: str, conventions: DocumentConventions
    ) -> CompareExchangeResult:
        response: dict = json.loads(response_string)

        index = response.get("Index", None)
        if index is None:
            raise RuntimeError("Response is invalid. Index is missing.")

        successful = response.get("Successful")
        raw = response.get("Value")

        val = None

        if raw:
            val = raw.get(constants.CompareExchange.OBJECT_FIELD_NAME)

        if val is None:
            return cls(Utils.json_default(object_type), index, successful)

        result = (
            Utils.convert_json_dict_to_object(val, object_type) if not isinstance(val, (str, int, float, bool)) else val
        )

        return cls(result, index, successful)


class PutCompareExchangeValueOperation(IOperation[CompareExchangeResult], Generic[_T]):
    def __init__(self, key: str, value: _T, index: int, metadata: MetadataAsDictionary = None):
        self.__key = key
        self.__value = value
        self.__index = index
        self.__metadata = metadata

    def get_command(self, store: DocumentStore, conventions: DocumentConventions, cache: HttpCache) -> RavenCommand[_T]:
        return self.__PutCompareExchangeValueCommand(
            self.__key, self.__value, self.__index, self.__metadata, conventions
        )

    class __PutCompareExchangeValueCommand(RavenCommand[_T], RaftCommand, Generic[_T]):
        def __init__(
            self, key: str, value: _T, index: int, metadata: MetadataAsDictionary, conventions: DocumentConventions
        ):
            if not key:
                raise ValueError("The key argument must have value")

            if index < 0:
                raise ValueError("Index must be a non-negative number")

            super().__init__(CompareExchangeResult[_T])

            self.__key = key
            self.__value = value
            self.__index = index
            self.__metadata = metadata
            self.__conventions = conventions or DocumentConventions()

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/cmpxchg?key={Utils.quote_key(self.__key)}&index={self.__index}"
            tuple = {constants.CompareExchange.OBJECT_FIELD_NAME: self.__value}
            json_dict = EntityToJson.convert_entity_to_json_internal_static(tuple, self.__conventions, None, False)
            if self.__metadata:
                metadata = CompareExchangeSessionValue.prepare_metadata_for_put(
                    self.__key, self.__metadata, self.__conventions
                )
                json_dict[constants.Documents.Metadata.KEY] = metadata

            return requests.Request("PUT", url, data=json_dict)

        def set_response(self, response: str, from_cache: bool) -> None:
            self.result = CompareExchangeResult.parse_from_string(type(self.__value), response, self.__conventions)

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class GetCompareExchangeValueOperation(IOperation[CompareExchangeValue[_T]], Generic[_T]):
    def __init__(self, key: str, object_type: Type[_T], materialize_metadata: bool = True):
        self.__key = key
        self.__object_type = object_type
        self.__materialize_metadata = materialize_metadata

    def get_command(self, store: DocumentStore, conventions: DocumentConventions, cache: HttpCache) -> RavenCommand[_T]:
        return self.GetCompareExchangeValueCommand(
            self.__key, self.__object_type, self.__materialize_metadata, conventions
        )

    class GetCompareExchangeValueCommand(RavenCommand[CompareExchangeValue[_T]]):
        def __init__(
            self, key: str, object_type: Type[_T], materialize_metadata: bool, conventions: DocumentConventions
        ):
            if not key:
                raise ValueError("The key argument must have value")
            super().__init__(CompareExchangeValue[_T])
            self.__key = key
            self.__object_type = object_type
            self.__materialize_metadata = materialize_metadata
            self.__conventions = conventions

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/cmpxchg?key={Utils.quote_key(self.__key)}"
            return requests.Request("GET", url)

        def set_response(self, response: str, from_cache: bool) -> None:
            self.result = CompareExchangeValueResultParser.get_value(
                self.__object_type, response, self.__materialize_metadata, self.__conventions
            )


class DeleteCompareExchangeValueOperation(IOperation[CompareExchangeResult[_T]], Generic[_T]):
    def __init__(self, object_type: type, key: str, index: int):
        self.__key = key
        self.__object_type = object_type
        self.__index = index

    def get_command(self, store: DocumentStore, conventions: DocumentConventions, cache: HttpCache) -> RavenCommand[_T]:
        return self.RemoveCompareExchangeCommand[_T](self.__object_type, self.__key, self.__index, conventions)

    class RemoveCompareExchangeCommand(RavenCommand[CompareExchangeResult[_T]], RaftCommand, Generic[_T]):
        def __init__(self, object_type: type, key: str, index: int, conventions: DocumentConventions):
            if not key:
                raise ValueError("The key must have value")

            super().__init__(CompareExchangeResult[_T])
            self.__object_type = object_type
            self.__key = key
            self.__index = index
            self.__conventions = conventions

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request(
                "DELETE",
                f"{node.url}/databases/{node.database}/cmpxchg?key={Utils.quote_key(self.__key)}&index={self.__index}",
            )

        def set_response(self, response: str, from_cache: bool) -> None:
            self.result = CompareExchangeResult.parse_from_string(self.__object_type, response, self.__conventions)

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class GetCompareExchangeValuesOperation(IOperation[Dict[str, CompareExchangeValue[_T]]], Generic[_T]):
    def __init__(
        self,
        keys_or_start_with: Union[Collection[str], StartingWithOptions],
        object_type: Optional[Type[_T]],
        materialize_metadata: Optional[bool] = True,
    ):  # todo: starting with
        start_with = isinstance(keys_or_start_with, StartingWithOptions)
        if not materialize_metadata and start_with:
            raise ValueError(
                f"Cannot set materialize_metadata to False while "
                f"collecting cmpxchg values starting with '{keys_or_start_with.starts_with}'"
            )

        self._object_type = object_type

        if start_with:
            self._keys = None
            self._materialize_metadata = True  # todo: documentation - tell the user that it'll be set to True anyway
            self._start = keys_or_start_with.start
            self._page_size = keys_or_start_with.page_size
            self._start_with = keys_or_start_with.starts_with
        else:
            self._materialize_metadata = materialize_metadata
            self._keys = keys_or_start_with
            self._start = None
            self._page_size = None
            self._start_with = None

    def get_command(self, store: DocumentStore, conventions: DocumentConventions, cache: HttpCache) -> RavenCommand[_T]:
        return self.GetCompareExchangeValuesCommand(self, self._materialize_metadata, conventions)

    class GetCompareExchangeValuesCommand(RavenCommand[Dict[str, CompareExchangeValue[_T]]], Generic[_T]):
        def __init__(
            self,
            operation: GetCompareExchangeValuesOperation[_T],
            materialize_metadata: bool,
            conventions: DocumentConventions,
        ):
            super().__init__(dict)
            self.__operation = operation
            self.__materialize_metadata = materialize_metadata
            self.__conventions = conventions

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            path_builder = [node.url, "/databases/", node.database, "/cmpxchg?"]

            if self.__operation._keys:
                for key in self.__operation._keys:
                    path_builder.append("&key=")
                    path_builder.append(Utils.quote_key(key))
            else:
                if self.__operation._start_with:
                    path_builder.append("&startsWith=")
                    path_builder.append(Utils.quote_key(self.__operation._start_with))
                if self.__operation._start:
                    path_builder.append("&start=")
                    path_builder.append(Utils.quote_key(self.__operation._start))
                if self.__operation._page_size:
                    path_builder.append("&pageSize=")
                    path_builder.append(Utils.quote_key(self.__operation._page_size))

            return requests.Request("GET", "".join(path_builder))

        def set_response(self, response: str, from_cache: bool) -> None:
            self.result = CompareExchangeValueResultParser.get_values(
                self.__operation._object_type, response, self.__materialize_metadata, self.__conventions
            )
