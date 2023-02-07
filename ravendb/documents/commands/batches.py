from __future__ import annotations

import datetime
import json
from abc import abstractmethod
from enum import Enum
from typing import Callable, Union, Optional, TYPE_CHECKING, List, Set, Dict

import requests

from ravendb.documents.operations.counters.operation import (
    CounterOperation,
    DocumentCountersOperation,
    CounterOperationType,
)
from ravendb.documents.session.misc import TransactionMode, ForceRevisionStrategy
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.json.result import BatchCommandResult
from ravendb.tools.utils import CaseInsensitiveSet, Utils
from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions
    from ravendb.documents.operations.patch import PatchRequest
    from ravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations


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

    @classmethod
    def from_csharp_value_str(cls, value: str) -> CommandType:
        if value == "None":
            return cls.NONE
        elif value == "PUT":
            return cls.PUT
        elif value == "PATCH":
            return cls.PATCH
        elif value == "DELETE":
            return cls.DELETE
        elif value == "AttachmentPUT":
            return cls.ATTACHMENT_PUT
        elif value == "AttachmentDELETE":
            return cls.ATTACHMENT_DELETE
        elif value == "AttachmentMOVE":
            return cls.ATTACHMENT_MOVE
        elif value == "AttachmentCOPY":
            return cls.ATTACHMENT_COPY
        elif value == "CompareExchangePUT":
            return cls.COMPARE_EXCHANGE_PUT
        elif value == "CompareExchangeDELETE":
            return cls.COMPARE_EXCHANGE_DELETE
        elif value == "Counters":
            return cls.COUNTERS
        elif value == "BatchPATCH":
            return cls.BATCH_PATCH
        elif value == "ForceRevisionCreation":
            return cls.FORCE_REVISION_CREATION
        elif value == "TimeSeries":
            return cls.TIME_SERIES
        else:
            raise ValueError(f"Unable to parse: {value}")


# --------------COMMAND--------------
class SingleNodeBatchCommand(RavenCommand):
    def __init__(
        self,
        conventions: DocumentConventions,
        commands: List[CommandData],
        options: BatchOptions = None,
        mode: TransactionMode = TransactionMode.SINGLE_NODE,
    ):
        if not conventions:
            raise ValueError("Conventions cannot be None")
        if commands is None:
            raise ValueError("Commands cannot be None")
        for command in commands:
            if command is None:
                raise ValueError("Command cannot be None")

        super().__init__(result_class=BatchCommandResult)
        self.__conventions = conventions
        self.__commands = commands
        self.__options = options
        self.__mode = mode
        self.__attachment_streams: List[bytes] = list()

        for command in commands:
            if isinstance(command, PutAttachmentCommandData):
                if self.__attachment_streams is None:
                    self.__attachment_streams = []
                stream = command.stream
                if stream in self.__attachment_streams:
                    raise RuntimeError(
                        "It is forbidden to re-use the same stream for more than one attachment. "
                        "Use a unique stream per put attachment command."
                    )
                self.__attachment_streams.append(stream)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def is_read_request(self):
        return False

    def create_request(self, node: ServerNode) -> requests.Request:
        request = requests.Request(method="POST")
        files = {"main": None}
        for command in self.__commands:
            if command.command_type == CommandType.ATTACHMENT_PUT:
                command: PutAttachmentCommandData
                files[command.name] = (
                    command.name,
                    command.stream,
                    command.content_type,
                    {"Command-Type": "AttachmentStream"},
                )
            if not request.data:
                request.data = {"Commands": []}
            request.data["Commands"].append(command.serialize(self.__conventions))

        if self.__mode == TransactionMode.CLUSTER_WIDE:
            request.data["TransactionMode"] = "ClusterWide"
            if files:
                request.use_stream = True

        if len(files) > 1:
            files["main"] = json.dumps(request.data, default=self.__conventions.json_default_method)
            request.files = files
            request.data = None

        sb = [f"{node.url}/databases/{node.database}/bulk_docs?"]
        self._append_options(sb)

        request.url = "".join(sb)

        return request

    def _append_options(self, sb: List[str]) -> None:
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
        self.result = Utils.initialize_object(json.loads(response), self._result_class, True)


class ClusterWideBatchCommand(SingleNodeBatchCommand):
    def __init__(
        self,
        conventions: DocumentConventions,
        commands: List[CommandData],
        options: Optional[BatchOptions] = None,
        disable_atomic_documents_writes: Optional[bool] = None,
    ):
        super().__init__(conventions, commands, options, TransactionMode.CLUSTER_WIDE)

        self.__disable_atomic_document_writes = disable_atomic_documents_writes

    def get_raft_unique_request_id(self) -> str:
        return RaftIdGenerator.new_id()

    def _append_options(self, sb: List[str]) -> None:
        super()._append_options(sb)

        if self.__disable_atomic_document_writes is None:
            return

        sb.append("&disableAtomicDocumentWrites=")
        sb.append("true" if self.__disable_atomic_document_writes else "false")


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
        self._key = key
        self._name = name
        self._change_vector = change_vector
        self._command_type = command_type
        self._on_before_save_changes = on_before_save_changes

    @property
    def key(self) -> str:
        return self._key

    @property
    def name(self) -> str:
        return self._name

    @property
    def change_vector(self) -> str:
        return self._change_vector

    @property
    def command_type(self) -> CommandType:
        return self._command_type

    @property
    def on_before_save_changes(
        self,
    ) -> Callable[[InMemoryDocumentSessionOperations], None]:
        return self._on_before_save_changes

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
            "Id": self._key,
            "ChangeVector": self._change_vector,
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

        @classmethod
        def create(cls, key: str, change_vector: str) -> BatchPatchCommandData.IdAndChangeVector:
            return cls(key, change_vector)

    def __init__(self, patch: PatchRequest, patch_if_missing: PatchRequest, *ids: Union[str, IdAndChangeVector]):
        if patch is None:
            raise ValueError("Patch cannot be None")

        super(BatchPatchCommandData, self).__init__(
            command_type=CommandType.BATCH_PATCH, name=None, change_vector=None, key=None
        )

        self.__seen_ids: Set[str] = CaseInsensitiveSet()
        self.__ids: List[BatchPatchCommandData.IdAndChangeVector] = []
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
        self,
        key: str,
        change_vector: Union[None, str],
        patch: PatchRequest,
        patch_if_missing: Optional[PatchRequest] = None,
    ):
        super().__init__(key, None, change_vector, CommandType.PATCH)
        if not key:
            raise ValueError("Key cannot be None")

        if not patch:
            raise ValueError("Patch cannot be None")

        self.create_if_missing: Union[None, dict] = None
        self.__patch = patch
        self.__patch_if_missing = patch_if_missing
        self.return_document: Union[None, bool] = None

        def __consumer(session: InMemoryDocumentSessionOperations) -> None:
            self.return_document = session.advanced.is_loaded(key)

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


class CountersBatchCommandData(CommandData):
    def __init__(
        self,
        document_id: str,
        counter_operations: Union[List[CounterOperation], CounterOperation],
        from_etl: Optional[bool] = None,
        change_vector=None,
    ):
        if not document_id:
            raise ValueError("None or empty document id is Invalid")
        if not counter_operations:
            raise ValueError("invalid counter_operations")
        if not isinstance(counter_operations, list):
            counter_operations = [counter_operations]

        super().__init__(key=document_id, command_type=CommandType.COUNTERS, change_vector=change_vector)
        self._from_etl = from_etl
        self._counters = DocumentCountersOperation(self.key, *counter_operations)

    def has_increment(self, counter_name):
        self.has_operation_of_type(CounterOperationType.INCREMENT, counter_name)

    def has_delete(self, counter_name):
        self.has_operation_of_type(CounterOperationType.DELETE, counter_name)

    def has_operation_of_type(self, operation_type: CounterOperationType, counter_name: str):
        for op in self.counters.operations:
            if op.counter_name != counter_name:
                continue

            if op.counter_operation_type == operation_type:
                return True
        return False

    @property
    def counters(self):
        return self._counters

    def serialize(self, conventions: DocumentConventions) -> Dict:
        json_dict = {
            "Id": self.key,
            "Counters": self.counters.to_json(),
            "Type": self.type,
        }
        if self._from_etl:
            json_dict["FromEtl"] = self._from_etl
        return json_dict


class PutAttachmentCommandData(CommandData):
    def __init__(self, document_id: str, name: str, stream: bytes, content_type: str, change_vector: str):
        if not document_id:
            raise ValueError(document_id)
        if not name:
            raise ValueError(name)

        super(PutAttachmentCommandData, self).__init__(document_id, name, change_vector, CommandType.ATTACHMENT_PUT)
        self.__stream = stream
        self.__content_type = content_type

    @property
    def stream(self):
        return self.__stream

    @property
    def content_type(self):
        return self.__content_type

    def serialize(self, conventions: DocumentConventions) -> dict:
        return {
            "Id": self._key,
            "Name": self._name,
            "ContentType": self.__content_type,
            "ChangeVector": self._change_vector,
            "Type": str(self._command_type),
        }


class CopyAttachmentCommandData(CommandData):
    def __init__(
        self,
        source_document_id: str,
        source_name: str,
        destination_document_id: str,
        destination_name: str,
        change_vector: str,
    ):
        if source_document_id.isspace():
            raise ValueError("source_document_id is required")
        if source_name.isspace():
            raise ValueError("source_name is required")
        if destination_document_id.isspace():
            raise ValueError("destination_document_id is required")
        if destination_name.isspace():
            raise ValueError("destination_name is required")
        super().__init__(source_document_id, source_name, change_vector, CommandType.ATTACHMENT_COPY)
        self.destination_id = destination_document_id
        self.destination_name = destination_name

    def serialize(self, conventions: DocumentConventions) -> dict:
        return {
            "Id": self.key,
            "Name": self.name,
            "DestinationId": self.destination_id,
            "DestinationName": self.destination_name,
            "ChangeVector": self.change_vector,
            "Type": "AttachmentCOPY",
        }


class MoveAttachmentCommandData(CommandData):
    def __init__(
        self,
        key: str,
        name: str,
        destination_id: str,
        destination_name: str,
        change_vector: str,
    ):
        if key.isspace():
            raise ValueError("source_document_id is required")
        if name.isspace():
            raise ValueError("source_name is required")
        if destination_id.isspace():
            raise ValueError("destination_document_id is required")
        if destination_name.isspace():
            raise ValueError("destination_name is required")
        super().__init__(key, name, change_vector, CommandType.ATTACHMENT_MOVE)
        self.destination_id = destination_id
        self.destination_name = destination_name

    def serialize(self, conventions: DocumentConventions) -> dict:
        return {
            "Id": self.key,
            "Name": self.name,
            "DestinationId": self.destination_id,
            "DestinationName": self.destination_name,
            "ChangeVector": self.change_vector,
            "Type": "AttachmentMOVE",
        }


class DeleteAttachmentCommandData(CommandData):
    def __init__(self, document_id: str, name: str, change_vector: str):
        if not document_id:
            raise ValueError(document_id)
        if not name:
            raise ValueError(name)
        super().__init__(document_id, name, change_vector, CommandType.ATTACHMENT_DELETE)

    def serialize(self, conventions: DocumentConventions) -> dict:
        return {"Id": self._key, "Name": self.name, "ChangeVector": self.change_vector, "Type": self.command_type}


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
        wait_for_specific_indexes: List[str] = None,
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
