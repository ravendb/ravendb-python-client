from __future__ import annotations

import datetime
import json
from datetime import timedelta
from typing import Dict, Any, Generic, TypeVar, List, Optional, Type, TYPE_CHECKING

import requests

from ravendb.documents.commands.revisions import GetRevisionsCommand
from ravendb.documents.operations.definitions import (
    IOperation,
    MaintenanceOperation,
    OperationIdResult,
    VoidOperation,
)
from ravendb.http.raven_command import RavenCommand, VoidRavenCommand
from ravendb.util.util import RaftIdGenerator
from ravendb.http.topology import RaftCommand
from ravendb.documents.session.entity_to_json import EntityToJsonStatic
from ravendb.documents.conventions import DocumentConventions
from ravendb.tools.utils import Utils

if TYPE_CHECKING:
    from ravendb.http.http_cache import HttpCache
    from ravendb import DocumentStore, ServerNode


_T = TypeVar("_T")


class RevisionsCollectionConfiguration:
    def __init__(
        self,
        minimum_revisions_to_keep: int = None,
        minimum_revisions_age_to_keep: timedelta = None,
        disabled: bool = False,
        purge_on_delete: bool = False,
        maximum_revisions_to_delete_upon_document_creation: int = None,
    ):
        self.minimum_revisions_to_keep = minimum_revisions_to_keep
        self.minimum_revisions_age_to_keep = minimum_revisions_age_to_keep
        self.disabled = disabled
        self.purge_on_delete = purge_on_delete
        self.maximum_revisions_to_delete_upon_document_creation = maximum_revisions_to_delete_upon_document_creation

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> RevisionsCollectionConfiguration:
        return cls(
            json_dict["MinimumRevisionsToKeep"],
            json_dict["MinimumRevisionAgeToKeep"],
            json_dict["Disabled"],
            json_dict["PurgeOnDelete"],
            json_dict["MaximumRevisionsToDeleteUponDocumentUpdate"],
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "MinimumRevisionsToKeep": self.minimum_revisions_to_keep,
            "MinimumRevisionAgeToKeep": self.minimum_revisions_age_to_keep,
            "Disabled": self.disabled,
            "PurgeOnDelete": self.purge_on_delete,
            "MaximumRevisionsToDeleteUponDocumentUpdate": self.maximum_revisions_to_delete_upon_document_creation,
        }


class RevisionsConfiguration:
    def __init__(
        self,
        default_config: RevisionsCollectionConfiguration = None,
        collections: Dict[str, RevisionsCollectionConfiguration] = None,
    ):
        self.default_config = default_config
        self.collections = collections

    def to_json(self) -> Dict[str, Any]:
        return {
            "Default": self.default_config.to_json() if self.default_config else None,
            "Collections": (
                {key: value.to_json() for key, value in self.collections.items()} if self.collections else None
            ),
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> RevisionsConfiguration:
        return cls(
            RevisionsCollectionConfiguration.from_json(json_dict["Default"]),
            {key: RevisionsCollectionConfiguration.from_json(value) for key, value in json_dict["Collections"].items()},
        )


class RevisionsResult(Generic[_T]):
    def __init__(self, results: List[_T] = None, total_results: int = None):
        self.results = results
        self.total_results = total_results


class RevisionIncludeResult:
    def __init__(
        self,
        Id: str = None,
        change_vector: str = None,
        before: datetime.datetime = None,
        revision: Dict[str, Any] = None,
    ):
        self.Id = Id
        self.change_vector = change_vector
        self.before = before
        self.revision = revision


class GetRevisionsOperation(Generic[_T], IOperation[RevisionsResult[_T]]):
    class Parameters:
        def __init__(self, id_: str = None, start: int = None, page_size: int = None):
            self.id_ = id_
            self.start = start
            self.page_size = page_size

        def validate(self):
            if not self.id_:
                raise ValueError("Id cannot be None")

    def __init__(
        self, id_: str = None, object_type: Optional[Type[_T]] = dict, start: int = None, page_size: int = None
    ):
        parameters = self.Parameters(id_, start, page_size)
        self._object_type = object_type
        self._parameters = parameters

    @classmethod
    def from_parameters(cls, parameters: Parameters, object_type: Type[_T] = None) -> GetRevisionsOperation[_T]:
        if parameters is None:
            raise ValueError("Parameters cannot be None")

        parameters.validate()

        operation = cls()
        operation._object_type = object_type
        operation._parameters = parameters
        return operation

    def get_command(self, store: DocumentStore, conventions: DocumentConventions, cache: HttpCache) -> RavenCommand[_T]:
        return self.GetRevisionsResultCommand(
            self._object_type, self._parameters.id_, self._parameters.start, self._parameters.page_size
        )

    class GetRevisionsResultCommand(RavenCommand[RevisionsResult[_T]]):
        def __init__(self, object_type: Optional[Type[_T]], id_: str = None, start: int = None, page_size: int = None):
            super().__init__(RevisionsResult[_T])
            self._object_type = object_type
            self._cmd = GetRevisionsCommand(id_, start, page_size)

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            return self._cmd.create_request(node)

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                return

            response_dict = json.loads(response)
            if "Results" not in response_dict:
                return

            revisions = response_dict["Results"]
            total = response_dict["TotalResults"]

            results = []
            for revision in revisions:
                if not revision:
                    continue

                entity = EntityToJsonStatic.convert_to_entity(
                    revision, self._object_type, DocumentConventions.default_conventions()
                )
                results.append(entity)

            result = RevisionsResult(results, total)
            self.result = result


class ConfigureRevisionsOperationResult:
    def __init__(self, raft_command_index: int = None):
        self.raft_command_index = raft_command_index

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> ConfigureRevisionsOperationResult:
        return cls(json_dict["RaftCommandIndex"])


class ConfigureRevisionsOperation(MaintenanceOperation[ConfigureRevisionsOperationResult]):
    def __init__(self, configuration: RevisionsConfiguration):
        if configuration is None:
            raise ValueError("Configuration cannot be None")
        self._configuration = configuration

    def get_command(self, conventions: "DocumentConventions") -> "RavenCommand[ConfigureRevisionsOperationResult]":
        return self.ConfigureRevisionsCommand(self._configuration)

    class ConfigureRevisionsCommand(RavenCommand[ConfigureRevisionsOperationResult], RaftCommand):
        def __init__(self, configuration: RevisionsConfiguration):
            super().__init__(ConfigureRevisionsOperationResult)
            self._configuration = configuration

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/revisions/config"

            request = requests.Request("POST", url)
            request.data = self._configuration.to_json()
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = ConfigureRevisionsOperationResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator().new_id()


class RevisionsOperationContinuationParameters:
    """State for resuming an interrupted revisions operation (etags are node-local)."""

    def __init__(
        self,
        start_from_etags: Dict[str, int] = None,
        etag_barriers: Dict[str, int] = None,
        node_tags: Dict[str, str] = None,
    ):
        self.start_from_etags = start_from_etags
        self.etag_barriers = etag_barriers
        self.node_tags = node_tags

    def to_json(self) -> Dict[str, Any]:
        return {
            "StartFromEtags": self.start_from_etags,
            "EtagBarriers": self.etag_barriers,
            "NodeTags": self.node_tags,
        }


class RevisionsOperationParameters:
    """Base parameters shared by enforce-configuration and adopt-orphaned operations."""

    def __init__(
        self,
        collections: List[str] = None,
        continuation_parameters: RevisionsOperationContinuationParameters = None,
    ):
        self.collections = collections
        self.continuation_parameters = continuation_parameters

    def to_json(self) -> Dict[str, Any]:
        return {
            "Collections": self.collections,
            "ContinuationParameters": (
                self.continuation_parameters.to_json() if self.continuation_parameters else None
            ),
        }


class EnforceRevisionsConfigurationOperation(IOperation[OperationIdResult]):
    """Applies the current revisions configuration to all existing revisions (long-running:
    send via ``store.operations.send_async`` and await completion)."""

    class Parameters(RevisionsOperationParameters):
        def __init__(
            self,
            include_force_created: bool = False,
            max_ops_per_second: int = None,
            collections: List[str] = None,
            continuation_parameters: RevisionsOperationContinuationParameters = None,
        ):
            super().__init__(collections, continuation_parameters)
            if max_ops_per_second is not None and max_ops_per_second <= 0:
                raise ValueError("max_ops_per_second must be greater than 0")
            self.include_force_created = include_force_created
            self.max_ops_per_second = max_ops_per_second

        def to_json(self) -> Dict[str, Any]:
            json_dict = super().to_json()
            json_dict["IncludeForceCreated"] = self.include_force_created
            json_dict["MaxOpsPerSecond"] = self.max_ops_per_second
            return json_dict

    def __init__(self, parameters: Optional["EnforceRevisionsConfigurationOperation.Parameters"] = None):
        self._parameters = parameters if parameters is not None else EnforceRevisionsConfigurationOperation.Parameters()

    def get_command(
        self, store: DocumentStore, conventions: DocumentConventions, cache: HttpCache
    ) -> RavenCommand[OperationIdResult]:
        return self.EnforceRevisionsConfigurationCommand(self._parameters)

    class EnforceRevisionsConfigurationCommand(RavenCommand[OperationIdResult]):
        def __init__(self, parameters: "EnforceRevisionsConfigurationOperation.Parameters"):
            super().__init__(OperationIdResult)
            self._parameters = parameters

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/revisions/config/enforce"
            request = requests.Request("POST", url)
            request.data = self._parameters.to_json()
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = OperationIdResult.from_json(json.loads(response))


class AdoptOrphanedRevisionsOperation(IOperation[OperationIdResult]):
    """Re-attaches orphaned revisions to their documents (long-running: send via
    ``store.operations.send_async`` and await completion)."""

    class Parameters(RevisionsOperationParameters):
        pass

    def __init__(self, parameters: Optional["AdoptOrphanedRevisionsOperation.Parameters"] = None):
        self._parameters = parameters if parameters is not None else AdoptOrphanedRevisionsOperation.Parameters()

    def get_command(
        self, store: DocumentStore, conventions: DocumentConventions, cache: HttpCache
    ) -> RavenCommand[OperationIdResult]:
        return self.AdoptOrphanedRevisionsCommand(self._parameters)

    class AdoptOrphanedRevisionsCommand(RavenCommand[OperationIdResult]):
        def __init__(self, parameters: "AdoptOrphanedRevisionsOperation.Parameters"):
            super().__init__(OperationIdResult)
            self._parameters = parameters

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/revisions/orphaned/adopt"
            request = requests.Request("POST", url)
            request.data = self._parameters.to_json()
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = OperationIdResult.from_json(json.loads(response))


class DeleteRevisionsOperation(MaintenanceOperation["DeleteRevisionsOperation.Result"]):
    """Explicitly deletes revisions for one or more documents - by document id(s), by a
    date range, or by specific revision change-vectors."""

    class Result:
        def __init__(self, total_deletes: int = None):
            self.total_deletes = total_deletes

        @classmethod
        def from_json(cls, json_dict: Dict[str, Any]) -> DeleteRevisionsOperation.Result:
            return cls(json_dict["TotalDeletes"])

    class Parameters:
        def __init__(
            self,
            document_ids: List[str] = None,
            remove_force_created_revisions: bool = False,
            revisions_change_vectors: List[str] = None,
            from_date: datetime.datetime = None,
            to_date: datetime.datetime = None,
        ):
            self.document_ids = document_ids
            self.remove_force_created_revisions = remove_force_created_revisions
            self.revisions_change_vectors = revisions_change_vectors
            self.from_date = from_date
            self.to_date = to_date

        def validate(self) -> None:
            if not self.document_ids:
                raise ValueError("Document ids cannot be None or empty")

            for document_id in self.document_ids:
                if not document_id or document_id.isspace():
                    raise ValueError("Document id cannot be None or whitespace")

            if self.revisions_change_vectors:
                if len(self.document_ids) != 1:
                    raise ValueError("The number of document ids must be 1 when using revisions change vectors")
                if self.from_date is not None or self.to_date is not None:
                    raise ValueError("Can't use revisions change vectors and date range in the same request")
            elif self.from_date is not None and self.to_date is not None and self.to_date <= self.from_date:
                raise ValueError("To date must be greater than From date")

        def to_json(self) -> Dict[str, Any]:
            return {
                "DocumentIds": self.document_ids,
                "RevisionsChangeVectors": self.revisions_change_vectors,
                "From": Utils.datetime_to_string(self.from_date) if self.from_date is not None else None,
                "To": Utils.datetime_to_string(self.to_date) if self.to_date is not None else None,
                "RemoveForceCreatedRevisions": self.remove_force_created_revisions,
            }

    def __init__(
        self,
        document_id: str = None,
        revisions_change_vectors: List[str] = None,
        from_date: datetime.datetime = None,
        to_date: datetime.datetime = None,
        remove_force_created_revisions: bool = False,
        document_ids: List[str] = None,
        parameters: Optional["DeleteRevisionsOperation.Parameters"] = None,
    ):
        if parameters is None:
            if document_ids is None and document_id is not None:
                document_ids = [document_id]
            parameters = DeleteRevisionsOperation.Parameters(
                document_ids,
                remove_force_created_revisions,
                revisions_change_vectors,
                from_date,
                to_date,
            )
        parameters.validate()
        self._parameters = parameters

    def get_command(self, conventions: DocumentConventions) -> RavenCommand[DeleteRevisionsOperation.Result]:
        return self.DeleteRevisionsCommand(self._parameters)

    class DeleteRevisionsCommand(RavenCommand["DeleteRevisionsOperation.Result"], RaftCommand):
        def __init__(self, parameters: "DeleteRevisionsOperation.Parameters"):
            super().__init__(DeleteRevisionsOperation.Result)
            self._parameters = parameters

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/revisions"
            request = requests.Request("DELETE", url)
            request.data = self._parameters.to_json()
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = DeleteRevisionsOperation.Result.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator().new_id()


class RevertRevisionsByIdOperation(VoidOperation):
    """Reverts one or more documents to a specific revision identified by its change-vector."""

    def __init__(self, id_to_change_vector: Dict[str, str] = None, id_: str = None, change_vector: str = None):
        if id_to_change_vector is None:
            if not id_:
                raise ValueError("Id cannot be None or empty")
            if not change_vector:
                raise ValueError("Change vector cannot be None or empty")
            id_to_change_vector = {id_: change_vector}

        if not id_to_change_vector:
            raise ValueError("id_to_change_vector cannot be None or empty")

        self._id_to_change_vector = id_to_change_vector

    def get_command(self, store: DocumentStore, conventions: DocumentConventions, cache: HttpCache) -> VoidRavenCommand:
        return self.RevertRevisionsByIdCommand(self._id_to_change_vector)

    class RevertRevisionsByIdCommand(VoidRavenCommand):
        def __init__(self, id_to_change_vector: Dict[str, str]):
            super().__init__()
            self._id_to_change_vector = id_to_change_vector

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/revisions/revert/docs"
            request = requests.Request("POST", url)
            request.data = {"IdToChangeVector": self._id_to_change_vector}
            return request


class RevisionsBinConfiguration:
    """Configuration for the automatic revisions-bin cleaner."""

    def __init__(
        self,
        disabled: bool = False,
        minimum_entries_age_to_keep_in_min: int = 43200,
        cleaner_frequency_in_sec: int = 300,
    ):
        self.disabled = disabled
        self.minimum_entries_age_to_keep_in_min = minimum_entries_age_to_keep_in_min
        self.cleaner_frequency_in_sec = cleaner_frequency_in_sec

    def to_json(self) -> Dict[str, Any]:
        return {
            "Disabled": self.disabled,
            "MinimumEntriesAgeToKeepInMin": self.minimum_entries_age_to_keep_in_min,
            "CleanerFrequencyInSec": self.cleaner_frequency_in_sec,
        }


class ConfigureRevisionsBinCleanerOperationResult:
    def __init__(self, raft_command_index: int = None):
        self.raft_command_index = raft_command_index

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> ConfigureRevisionsBinCleanerOperationResult:
        return cls(json_dict["RaftCommandIndex"])


class ConfigureRevisionsBinCleanerOperation(MaintenanceOperation[ConfigureRevisionsBinCleanerOperationResult]):
    """Enables / configures the automatic revisions-bin cleaner for the database."""

    def __init__(self, configuration: RevisionsBinConfiguration):
        if configuration is None:
            raise ValueError("Configuration cannot be None")
        self._configuration = configuration

    def get_command(
        self, conventions: DocumentConventions
    ) -> RavenCommand[ConfigureRevisionsBinCleanerOperationResult]:
        return self.ConfigureRevisionsBinCleanerCommand(self._configuration)

    class ConfigureRevisionsBinCleanerCommand(RavenCommand[ConfigureRevisionsBinCleanerOperationResult], RaftCommand):
        def __init__(self, configuration: RevisionsBinConfiguration):
            super().__init__(ConfigureRevisionsBinCleanerOperationResult)
            self._configuration = configuration

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/revisions/bin-cleaner/config"
            request = requests.Request("POST", url)
            request.data = self._configuration.to_json()
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = ConfigureRevisionsBinCleanerOperationResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator().new_id()
