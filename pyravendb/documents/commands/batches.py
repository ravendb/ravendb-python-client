from __future__ import annotations

import datetime
import json
from abc import abstractmethod
from enum import Enum
from typing import Callable, Union, Optional

import requests

from pyravendb.data.document_conventions import DocumentConventions
from pyravendb.documents.operations.operations import PatchRequest
from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
from pyravendb.documents.session.session import ForceRevisionStrategy
from pyravendb.documents.session.transaction_mode import TransactionMode
from pyravendb.http.raven_command import RavenCommand
from pyravendb.http.server_node import ServerNode
from pyravendb.json.result import BatchCommandResult
from pyravendb.store.entity_to_json import EntityToJson
from pyravendb.tools.utils import CaseInsensitiveSet, Utils
from pyravendb.util.util import RaftIdGenerator


class CommandType(Enum):
    NONE = "None"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    ATTACHMENT_PUT = "AttachmentPUT"
    ATTACHMENT_DELETE = "AttachmentDELETE"
    ATTACHMENT_MOVE = "AttachmentMOVE"
    ATTACHMENT_COPY = "AttachmentCOPY"
    COMPARE_EXCHANGE_PUT = "CompareExchangePUT"
    COMPARE_EXCHANGE_DELETE = "CompareExchangeDELETE"

    FORCE_REVISION_CREATION = "ForceRevisionCreation"

    COUNTERS = "Counters"
    TIME_SERIES = "TimeSeries"
    TIME_SERIES_BULK_INSERT = "TIME_SERIES_BULK_INSERT"
    TIME_SERIES_COPY = "TIME_SERIES_COPY"
    BATCH_PATCH = "BatchPATCH"
    CLIENT_ANY_COMMAND = "CLIENT_ANY_COMMAND"
    CLIENT_MODIFY_DOCUMENT_COMMAND = "CLIENT_MODIFY_DOCUMENT_COMMAND"

    def __str__(self):
        return self.value

    @staticmethod
    def parse_csharp_value(input: str) -> CommandType:
        if input == "None":
            return CommandType.NONE
        elif input == "PUT":
            return CommandType.PUT
        elif input == "PATCH":
            return CommandType.PATCH
        elif input == "DELETE":
            return CommandType.DELETE
        elif input == "AttachmentPUT":
            return CommandType.ATTACHMENT_PUT
        elif input == "AttachmentDELETE":
            return CommandType.ATTACHMENT_DELETE
        elif input == "AttachmentMOVE":
            return CommandType.ATTACHMENT_MOVE
        elif input == "AttachmentCOPY":
            return CommandType.ATTACHMENT_COPY
        elif input == "CompareExchangePUT":
            return CommandType.COMPARE_EXCHANGE_PUT
        elif input == "CompareExchangeDELETE":
            return CommandType.COMPARE_EXCHANGE_DELETE
        elif input == "Counters":
            return CommandType.COUNTERS
        elif input == "BatchPATCH":
            return CommandType.BATCH_PATCH
        elif input == "ForceRevisionCreation":
            return CommandType.FORCE_REVISION_CREATION
        elif input == "TimeSeries":
            return CommandType.TIME_SERIES
        else:
            raise ValueError(f"Unable to parse: {input}")


# --------------COMMAND--------------
class SingleNodeBatchCommand(RavenCommand):
    def __init__(
        self,
        conventions: DocumentConventions,
        commands: list[CommandData],
        options: BatchOptions = None,
        mode: TransactionMode = TransactionMode.SINGLE_NODE,
    ):
        super().__init__(result_class=BatchCommandResult)
        self.__conventions = conventions
        self.__commands = commands
        self.__options = options
        self.__mode = mode
        self.__attachment_streams: set[bytes] = set()
        if not conventions:
            raise ValueError("Conventions cannot be None")
        if commands is None:
            raise ValueError("Commands cannot be None")

        for command in commands:
            # todo: attachments stuff
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def is_read_request(self):
        return False

    def create_request(self, node: ServerNode) -> (requests.Request, str):
        request = requests.Request(method="POST")
        request.data = {"Commands": [command.serialize(self.__conventions) for command in self.__commands]}
        if self.__mode == TransactionMode.CLUSTER_WIDE:
            request.data["TransactionMode"] = "ClusterWide"
        # todo: attachments stuff
        sb = [f"{node.url}/databases/{node.database}/bulk_docs"]
        self.__append_options(sb)
        return request, sb

    def __append_options(self, sb: list[str]) -> None:
        if self.__options is None:
            return
        sb.append("?")
        replication_options = self.__options.replication_options
        if replication_options:
            sb.append(
                f"&waitForReplicasTimeout=" f"{Utils.timedelta_to_str(replication_options.wait_for_replicas_timeout)}"
            )
            sb.append(
                f"&throwOnTimeoutInWaitForReplicas="
                f"{'true' if replication_options.throw_on_timeout_in_wait_for_replicas else 'false'}"
            )
            sb.append(
                f"&numberOfReplicasToWaitFor="
                f"{'majority' if replication_options.majority else replication_options.number_of_replicas_to_wait_for}"
            )

        index_options = self.__options.index_options
        if index_options:
            sb.append(f"&waitForIndexesTimeout={Utils.timedelta_to_str(index_options.wait_for_indexes_timeout)}")
            sb.append(f"&waitForIndexThrow={'true' if index_options.throw_on_timeout_in_wait_for_indexes else 'false'}")
            if index_options.wait_for_specific_indexes:
                for specific_index in index_options.wait_for_specific_indexes:
                    sb.append(f"&waitForSpecificIndex={specific_index.encode('utf-8')}")

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is None:
            raise ValueError(
                "Got None response from the server after doing a batch, something is very wrong."
                " Probably a garbled response."
            )

        self.result = EntityToJson.convert_to_entity_static(
            json.loads(response), BatchCommandResult, self.__conventions, None, None
        )


class ClusterWideBatchCommand(SingleNodeBatchCommand):
    def __init__(
        self, conventions: DocumentConventions, commands: list[CommandData], options: Optional[BatchOptions] = None
    ):
        super().__init__(conventions, commands, options, TransactionMode.CLUSTER_WIDE)

    def get_raft_unique_request_id(self) -> str:
        return RaftIdGenerator.new_id()


# -------------- DATA ---------------


class CommandData:
    def __init__(
        self,
        key: str = None,
        name: str = None,
        change_vector: str = None,
        command_type: CommandType = None,
        on_before_save_changes: Callable = None,
    ):
        self.__key = key
        self.__name = name
        self.__change_vector = change_vector
        self.__command_type = command_type
        self.__on_before_save_changes = on_before_save_changes

    @property
    def key(self) -> str:
        return self.__key

    @property
    def name(self) -> str:
        return self.__name

    @property
    def change_vector(self) -> str:
        return self.__change_vector

    @property
    def command_type(self) -> CommandType:
        return self.__command_type

    @property
    def on_before_save_changes(
        self,
    ) -> Callable[[InMemoryDocumentSessionOperations], None]:
        return self.__on_before_save_changes

    @abstractmethod
    def serialize(self, conventions: DocumentConventions) -> dict:
        pass


class DeleteCommandData(CommandData):
    def __init__(self, key: str, change_vector: str):
        super(DeleteCommandData, self).__init__(key=key, command_type=CommandType.DELETE)

    def serialize(self, conventions: DocumentConventions) -> dict:
        return {"Id": self.key, "ChangeVector": self.change_vector, "Type": CommandType.DELETE}


class PutCommandDataBase(CommandData):
    def __init__(
        self,
        key: str,
        change_vector: str,
        document: object,
        strategy: ForceRevisionStrategy = ForceRevisionStrategy.NONE,
    ):
        super(PutCommandDataBase, self).__init__(
            key=key, name=None, change_vector=change_vector, command_type=CommandType.PUT
        )
        self.__document = document
        self.__force_revisions_creation_strategy = strategy

    @property
    def document(self) -> object:
        return self.__document

    @property
    def force_revision_creation_strategy(self) -> ForceRevisionStrategy:
        return self.__force_revisions_creation_strategy

    def serialize(self, conventions: DocumentConventions) -> dict:
        result = {
            "Id": self.__key,
            "ChangeVector": self.__change_vector,
            "Document": self.__document,
            "Type": CommandType.PUT,
        }
        if self.force_revision_creation_strategy != ForceRevisionStrategy.NONE:
            result["ForceRevisionCreationStrategy"] = self.__force_revisions_creation_strategy
        return result


class PutCommandDataWithJson(PutCommandDataBase):
    def __init__(self, key, change_vector, document, strategy):
        super(PutCommandDataWithJson, self).__init__(key, change_vector, document, strategy)


class ForceRevisionCommandData(CommandData):
    def __init__(self, key: str):
        super(ForceRevisionCommandData, self).__init__(key=key, command_type=CommandType.FORCE_REVISION_CREATION)

    def serialize(self, conventions: DocumentConventions) -> dict:
        return {"Id": super().key, "Type": super().command_type}


class BatchPatchCommandData(CommandData):
    class IdAndChangeVector:
        def __init__(self, key: str, change_vector: str):
            self.key = key
            self.change_vector = change_vector

        @staticmethod
        def create(key: str, change_vector: str) -> BatchPatchCommandData.IdAndChangeVector:
            return BatchPatchCommandData.IdAndChangeVector(key, change_vector)

    def __init__(self, patch: PatchRequest, patch_if_missing: PatchRequest, *ids: Union[str, IdAndChangeVector]):
        if patch is None:
            raise ValueError("Patch cannot be None")

        super(BatchPatchCommandData, self).__init__(
            command_type=CommandType.BATCH_PATCH, name=None, change_vector=None, key=None
        )

        self.__seen_ids: set[str] = CaseInsensitiveSet()
        self.__ids: list[BatchPatchCommandData.IdAndChangeVector] = []
        self.__patch = patch
        self.__patch_if_missing = patch_if_missing
        self.on_before_save_changes: Callable[[InMemoryDocumentSessionOperations], None] = None

        if not ids:
            raise ValueError("Ids cannot be None or empty collection")
        for key in ids:
            if isinstance(key, BatchPatchCommandData.IdAndChangeVector):
                self.__add(key.key, key.change_vector)
            elif isinstance(key, str):
                self.__add(key)
            raise TypeError(f"Unexpected type: {type(key)} of key")

    def __add(self, key: str, change_vector: Optional[str] = None):
        if not key:
            raise ValueError("Value cannot be None or whitespace")
        if not self.__seen_ids.add(key):
            raise ValueError(f"Could not add ID '{key}' because item with the same ID was already added")
        self.__ids.append(BatchPatchCommandData.IdAndChangeVector.create(key, change_vector))

    @property
    def ids(self):
        return self.__ids

    @property
    def patch(self):
        return self.__patch

    @property
    def patch_if_missing(self):
        return self.__patch_if_missing

    def serialize(self, conventions: DocumentConventions) -> dict:
        arr = []
        for kvp in self.__ids:
            if kvp.change_vector is not None:
                arr.append({"Id": kvp.key, "ChangeVector": kvp.change_vector})
            arr.append({"Id": kvp.key})
        if self.patch_if_missing is None:
            return {"Ids": arr, "Patch": self.patch.serialize(), "Type": "BatchPATCH"}
        return {
            "Ids": arr,
            "Patch": self.patch.serialize(),
            "Type": "BatchPATCH",
            "PatchIfMissing": self.patch_if_missing.serialize(),
        }


class PatchCommandData(CommandData):
    def __init__(
        self, key: str, change_vector: str, patch: PatchRequest, patch_if_missing: Optional[PatchRequest] = None
    ):
        super().__init__(key, None, change_vector, CommandType.PATCH)
        if not key:
            raise ValueError("Key cannot be None")

        if not patch:
            raise ValueError("Patch cannot be None")

        self.__key = key
        self.create_if_missing: Union[None, dict] = None
        self.__change_vector = change_vector
        self.__patch = patch
        self.__patch_if_missing = patch_if_missing
        self.return_document: Union[None, bool] = None

        def __consumer(session: InMemoryDocumentSessionOperations) -> None:
            self.return_document = session.is_loaded(key)

        self.__on_before_save_changes = __consumer

    @property
    def patch(self):
        return self.__patch

    @property
    def patch_if_missing(self):
        return self.__patch_if_missing

    @property
    def on_before_save_changes(self):
        return self.__on_before_save_changes

    def serialize(self, conventions: DocumentConventions) -> dict:
        data = {"Id": self.key, "ChangeVector": self.change_vector, "Patch": self.patch.serialize(), "Type": "PATCH"}
        if self.patch_if_missing:
            data.update({"PatchIfMissing": self.patch_if_missing.serialize()})
        if self.create_if_missing:
            data.update({"CreateIfMissing": self.create_if_missing})
        if self.return_document:
            data.update({"ReturnDocument": self.return_document})
        return data


class PutCompareExchangeCommandData(CommandData):
    def __init__(self, key: str, value: dict, index: int):
        super(PutCompareExchangeCommandData, self).__init__(key=key, command_type=CommandType.COMPARE_EXCHANGE_PUT)
        self.document = value
        self.index = index

    def serialize(self, conventions: DocumentConventions) -> dict:
        return {
            "Id": self.key,
            "Document": self.document,
            "Index": self.index,
            "Type": CommandType.COMPARE_EXCHANGE_PUT,
        }


class DeleteCompareExchangeCommandData(CommandData):
    def __init__(self, key: str, index: int):
        super(DeleteCompareExchangeCommandData, self).__init__(
            key=key, command_type=CommandType.COMPARE_EXCHANGE_DELETE
        )
        self.key = key
        self.index = index

    def serialize(self, conventions: DocumentConventions) -> dict:
        return {"Id": self.key, "Index": self.index, "Type": CommandType.COMPARE_EXCHANGE_DELETE}

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, value):
        self._key = value


# ------------ OPTIONS ------------


class ReplicationBatchOptions:
    def __init__(
        self,
        wait_for_replicas: bool = None,
        number_of_replicas_to_wait_for: int = None,
        wait_for_replicas_timeout: datetime.timedelta = None,
        majority: bool = None,
        throw_on_timeout_in_wait_for_replicas: bool = True,
    ):
        self.wait_for_replicas = wait_for_replicas
        self.number_of_replicas_to_wait_for = number_of_replicas_to_wait_for
        self.wait_for_replicas_timeout = wait_for_replicas_timeout
        self.majority = majority
        self.throw_on_timeout_in_wait_for_replicas = throw_on_timeout_in_wait_for_replicas


class IndexBatchOptions:
    def __init__(
        self,
        wait_for_indexes: bool = None,
        wait_for_indexes_timeout: datetime.timedelta = None,
        throw_on_timeout_in_wait_for_indexes: bool = None,
        wait_for_specific_indexes: list[str] = None,
    ):
        self.wait_for_indexes = wait_for_indexes
        self.wait_for_indexes_timeout = wait_for_indexes_timeout
        self.throw_on_timeout_in_wait_for_indexes = throw_on_timeout_in_wait_for_indexes
        self.wait_for_specific_indexes = wait_for_specific_indexes


class BatchOptions:
    def __init__(
        self,
        replication_options: Optional[ReplicationBatchOptions] = None,
        index_options: Optional[IndexBatchOptions] = None,
    ):
        self.replication_options = replication_options
        self.index_options = index_options
