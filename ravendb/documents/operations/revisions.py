from __future__ import annotations

import datetime
import json
from datetime import timedelta
from typing import Dict, Any, Generic, TypeVar, List, Optional, Type, TYPE_CHECKING

import requests

from ravendb.documents.commands.revisions import GetRevisionsCommand
from ravendb.documents.operations.definitions import IOperation, MaintenanceOperation
from ravendb.http.raven_command import RavenCommand
from ravendb.util.util import RaftIdGenerator
from ravendb.http.topology import RaftCommand
from ravendb.documents.session.entity_to_json import EntityToJson
from ravendb.documents.conventions import DocumentConventions


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

                entity = EntityToJson.convert_to_entity_static(
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
