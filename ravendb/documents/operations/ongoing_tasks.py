from __future__ import annotations
import json
from enum import Enum
from typing import Optional, TYPE_CHECKING, Union

import requests

from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.serverwide.operations.common import ModifyOngoingTaskResult
from ravendb.tools.utils import Utils
from ravendb.util.util import RaftIdGenerator
from ravendb.http.raven_command import RavenCommand
from ravendb.documents.operations.replication.definitions import PullReplicationMode

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions
    from ravendb.documents.operations.ai.gen_ai_configuration import GenAiConfiguration
    from ravendb.documents.operations.ai.embeddings_generation_configuration import EmbeddingsGenerationConfiguration


class OngoingTaskType(Enum):
    REPLICATION = "Replication"
    RAVEN_ETL = "RavenEtl"
    SQL_ETL = "SqlEtl"
    OLAP_ETL = "OlapEtl"
    ELASTIC_SEARCH_ETL = "ElasticSearchEtl"
    QUEUE_ETL = "QueueEtl"
    SNOWFLAKE_ETL = "SnowflakeEtl"
    BACKUP = "Backup"
    SUBSCRIPTION = "Subscription"
    PULL_REPLICATION_AS_HUB = "PullReplicationAsHub"
    PULL_REPLICATION_AS_SINK = "PullReplicationAsSink"
    QUEUE_SINK = "QueueSink"
    EMBEDDINGS_GENERATION = "EmbeddingsGeneration"
    GEN_AI = "GenAi"


class OngoingTaskState(Enum):
    NONE = "None"
    ENABLED = "Enabled"
    DISABLED = "Disabled"
    PARTIALLY_ENABLED = "PartiallyEnabled"


class OngoingTaskConnectionStatus(Enum):
    NONE = "None"
    ACTIVE = "Active"
    NOT_ACTIVE = "NotActive"
    RECONNECT = "Reconnect"
    NOT_ON_THIS_NODE = "NotOnThisNode"


class NodeId:
    """Represents a node identifier in the cluster."""

    def __init__(
        self,
        node_tag: Optional[str] = None,
        node_url: Optional[str] = None,
        responsible_node: Optional[str] = None,
    ):
        self.node_tag = node_tag
        self.node_url = node_url
        self.responsible_node = responsible_node

    def to_json(self) -> dict:
        return {
            "NodeTag": self.node_tag,
            "NodeUrl": self.node_url,
            "ResponsibleNode": self.responsible_node,
        }

    @classmethod
    def from_json(cls, json_dict: dict) -> "NodeId":
        if json_dict is None:
            return None
        return cls(
            node_tag=json_dict.get("NodeTag"),
            node_url=json_dict.get("NodeUrl"),
            responsible_node=json_dict.get("ResponsibleNode"),
        )


class OngoingTask:
    """Base class for ongoing task information."""

    def __init__(
        self,
        task_id: Optional[int] = None,
        task_type: Optional[OngoingTaskType] = None,
        responsible_node: Optional[NodeId] = None,
        task_state: Optional[OngoingTaskState] = None,
        task_connection_status: Optional[OngoingTaskConnectionStatus] = None,
        task_name: Optional[str] = None,
        error: Optional[str] = None,
        mentor_node: Optional[str] = None,
        pin_to_mentor_node: Optional[bool] = None,
    ):
        self.task_id = task_id
        self.task_type = task_type
        self.responsible_node = responsible_node
        self.task_state = task_state
        self.task_connection_status = task_connection_status
        self.task_name = task_name
        self.error = error
        self.mentor_node = mentor_node
        self.pin_to_mentor_node = pin_to_mentor_node

    def to_json(self) -> dict:
        return {
            "TaskId": self.task_id,
            "TaskType": self.task_type.value if self.task_type else None,
            "ResponsibleNode": self.responsible_node.to_json() if self.responsible_node else None,
            "TaskState": self.task_state.value if self.task_state else None,
            "TaskConnectionStatus": self.task_connection_status.value if self.task_connection_status else None,
            "TaskName": self.task_name,
            "Error": self.error,
            "MentorNode": self.mentor_node,
            "PinToMentorNode": self.pin_to_mentor_node,
        }

    @classmethod
    def from_json(cls, json_dict: dict) -> "OngoingTask":
        if json_dict is None:
            return None
        task_type_str = json_dict.get("TaskType")
        task_state_str = json_dict.get("TaskState")
        task_connection_status_str = json_dict.get("TaskConnectionStatus")

        return cls(
            task_id=json_dict.get("TaskId"),
            task_type=OngoingTaskType(task_type_str) if task_type_str else None,
            responsible_node=NodeId.from_json(json_dict.get("ResponsibleNode")),
            task_state=OngoingTaskState(task_state_str) if task_state_str else None,
            task_connection_status=(
                OngoingTaskConnectionStatus(task_connection_status_str) if task_connection_status_str else None
            ),
            task_name=json_dict.get("TaskName"),
            error=json_dict.get("Error"),
            mentor_node=json_dict.get("MentorNode"),
            pin_to_mentor_node=json_dict.get("PinToMentorNode"),
        )


class OngoingTaskGenAi(OngoingTask):
    """Ongoing task information for GenAI tasks."""

    def __init__(
        self,
        task_id: Optional[int] = None,
        responsible_node: Optional[NodeId] = None,
        task_state: Optional[OngoingTaskState] = None,
        task_connection_status: Optional[OngoingTaskConnectionStatus] = None,
        task_name: Optional[str] = None,
        error: Optional[str] = None,
        mentor_node: Optional[str] = None,
        pin_to_mentor_node: Optional[bool] = None,
        connection_string_name: Optional[str] = None,
        configuration: Optional["GenAiConfiguration"] = None,
        change_vector: Optional[str] = None,
    ):
        super().__init__(
            task_id=task_id,
            task_type=OngoingTaskType.GEN_AI,
            responsible_node=responsible_node,
            task_state=task_state,
            task_connection_status=task_connection_status,
            task_name=task_name,
            error=error,
            mentor_node=mentor_node,
            pin_to_mentor_node=pin_to_mentor_node,
        )
        self.connection_string_name = connection_string_name
        self.configuration = configuration
        self.change_vector = change_vector

    def to_json(self) -> dict:
        result = super().to_json()
        result["ConnectionStringName"] = self.connection_string_name
        result["Configuration"] = self.configuration.to_json() if self.configuration else None
        result["ChangeVector"] = self.change_vector
        return result

    @classmethod
    def from_json(cls, json_dict: dict) -> "OngoingTaskGenAi":
        from ravendb.documents.operations.ai.gen_ai_configuration import GenAiConfiguration

        if json_dict is None:
            return None

        task_state_str = json_dict.get("TaskState")
        task_connection_status_str = json_dict.get("TaskConnectionStatus")
        config_dict = json_dict.get("Configuration")

        return cls(
            task_id=json_dict.get("TaskId"),
            responsible_node=NodeId.from_json(json_dict.get("ResponsibleNode")),
            task_state=OngoingTaskState(task_state_str) if task_state_str else None,
            task_connection_status=(
                OngoingTaskConnectionStatus(task_connection_status_str) if task_connection_status_str else None
            ),
            task_name=json_dict.get("TaskName"),
            error=json_dict.get("Error"),
            mentor_node=json_dict.get("MentorNode"),
            pin_to_mentor_node=json_dict.get("PinToMentorNode"),
            connection_string_name=json_dict.get("ConnectionStringName"),
            configuration=GenAiConfiguration.from_json(config_dict) if config_dict else None,
            change_vector=json_dict.get("ChangeVector"),
        )


class OngoingTaskEmbeddingsGeneration(OngoingTask):
    """Ongoing task information for Embeddings Generation tasks."""

    def __init__(
        self,
        task_id: Optional[int] = None,
        responsible_node: Optional[NodeId] = None,
        task_state: Optional[OngoingTaskState] = None,
        task_connection_status: Optional[OngoingTaskConnectionStatus] = None,
        task_name: Optional[str] = None,
        error: Optional[str] = None,
        mentor_node: Optional[str] = None,
        pin_to_mentor_node: Optional[bool] = None,
        connection_string_name: Optional[str] = None,
        configuration: Optional["EmbeddingsGenerationConfiguration"] = None,
        change_vector: Optional[str] = None,
    ):
        super().__init__(
            task_id=task_id,
            task_type=OngoingTaskType.EMBEDDINGS_GENERATION,
            responsible_node=responsible_node,
            task_state=task_state,
            task_connection_status=task_connection_status,
            task_name=task_name,
            error=error,
            mentor_node=mentor_node,
            pin_to_mentor_node=pin_to_mentor_node,
        )
        self.connection_string_name = connection_string_name
        self.configuration = configuration
        self.change_vector = change_vector

    def to_json(self) -> dict:
        result = super().to_json()
        result["ConnectionStringName"] = self.connection_string_name
        result["Configuration"] = self.configuration.to_json() if self.configuration else None
        result["ChangeVector"] = self.change_vector
        return result

    @classmethod
    def from_json(cls, json_dict: dict) -> "OngoingTaskEmbeddingsGeneration":
        from ravendb.documents.operations.ai.embeddings_generation_configuration import (
            EmbeddingsGenerationConfiguration,
        )

        if json_dict is None:
            return None

        task_state_str = json_dict.get("TaskState")
        task_connection_status_str = json_dict.get("TaskConnectionStatus")
        config_dict = json_dict.get("Configuration")

        return cls(
            task_id=json_dict.get("TaskId"),
            responsible_node=NodeId.from_json(json_dict.get("ResponsibleNode")),
            task_state=OngoingTaskState(task_state_str) if task_state_str else None,
            task_connection_status=(
                OngoingTaskConnectionStatus(task_connection_status_str) if task_connection_status_str else None
            ),
            task_name=json_dict.get("TaskName"),
            error=json_dict.get("Error"),
            mentor_node=json_dict.get("MentorNode"),
            pin_to_mentor_node=json_dict.get("PinToMentorNode"),
            connection_string_name=json_dict.get("ConnectionStringName"),
            configuration=EmbeddingsGenerationConfiguration.from_json(config_dict) if config_dict else None,
            change_vector=json_dict.get("ChangeVector"),
        )


class OngoingTaskPullReplicationAsHub(OngoingTask):
    """Ongoing task information for a single pull-replication hub connection."""

    def __init__(
        self,
        task_id: Optional[int] = None,
        responsible_node: Optional[NodeId] = None,
        task_state: Optional[OngoingTaskState] = None,
        task_connection_status: Optional[OngoingTaskConnectionStatus] = None,
        task_name: Optional[str] = None,
        error: Optional[str] = None,
        mentor_node: Optional[str] = None,
        pin_to_mentor_node: Optional[bool] = None,
        from_to_string: Optional[str] = None,
        destination_url: Optional[str] = None,
        destination_database: Optional[str] = None,
        delay_replication_for=None,
        handler_id: Optional[str] = None,
        last_accepted_change_vector_from_destination: Optional[str] = None,
        source_database_change_vector: Optional[str] = None,
        last_sent_etag: Optional[int] = None,
        last_database_etag: Optional[int] = None,
    ):
        super().__init__(
            task_id=task_id,
            task_type=OngoingTaskType.PULL_REPLICATION_AS_HUB,
            responsible_node=responsible_node,
            task_state=task_state,
            task_connection_status=task_connection_status,
            task_name=task_name,
            error=error,
            mentor_node=mentor_node,
            pin_to_mentor_node=pin_to_mentor_node,
        )
        self.from_to_string = from_to_string
        self.destination_url = destination_url
        self.destination_database = destination_database
        self.delay_replication_for = delay_replication_for
        self.handler_id = handler_id
        self.last_accepted_change_vector_from_destination = last_accepted_change_vector_from_destination
        self.source_database_change_vector = source_database_change_vector
        self.last_sent_etag = last_sent_etag
        self.last_database_etag = last_database_etag

    def to_json(self) -> dict:
        result = super().to_json()
        result["FromToString"] = self.from_to_string
        result["DestinationUrl"] = self.destination_url
        result["DestinationDatabase"] = self.destination_database
        result["DelayReplicationFor"] = (
            Utils.timedelta_to_str(self.delay_replication_for) if self.delay_replication_for is not None else None
        )
        result["HandlerId"] = self.handler_id
        result["LastAcceptedChangeVectorFromDestination"] = self.last_accepted_change_vector_from_destination
        result["SourceDatabaseChangeVector"] = self.source_database_change_vector
        result["LastSentEtag"] = self.last_sent_etag
        result["LastDatabaseEtag"] = self.last_database_etag
        return result

    @classmethod
    def from_json(cls, json_dict: dict) -> Optional["OngoingTaskPullReplicationAsHub"]:
        if json_dict is None:
            return None
        task_state_str = json_dict.get("TaskState")
        task_connection_status_str = json_dict.get("TaskConnectionStatus")
        delay = json_dict.get("DelayReplicationFor")
        return cls(
            task_id=json_dict.get("TaskId"),
            responsible_node=NodeId.from_json(json_dict.get("ResponsibleNode")),
            task_state=OngoingTaskState(task_state_str) if task_state_str else None,
            task_connection_status=(
                OngoingTaskConnectionStatus(task_connection_status_str) if task_connection_status_str else None
            ),
            task_name=json_dict.get("TaskName"),
            error=json_dict.get("Error"),
            mentor_node=json_dict.get("MentorNode"),
            pin_to_mentor_node=json_dict.get("PinToMentorNode"),
            from_to_string=json_dict.get("FromToString"),
            destination_url=json_dict.get("DestinationUrl"),
            destination_database=json_dict.get("DestinationDatabase"),
            delay_replication_for=Utils.string_to_timedelta(delay) if delay else None,
            handler_id=json_dict.get("HandlerId"),
            last_accepted_change_vector_from_destination=json_dict.get("LastAcceptedChangeVectorFromDestination"),
            source_database_change_vector=json_dict.get("SourceDatabaseChangeVector"),
            last_sent_etag=json_dict.get("LastSentEtag"),
            last_database_etag=json_dict.get("LastDatabaseEtag"),
        )


class OngoingTaskPullReplicationAsSink(OngoingTask):
    """Ongoing task information for a pull-replication sink task."""

    def __init__(
        self,
        task_id: Optional[int] = None,
        responsible_node: Optional[NodeId] = None,
        task_state: Optional[OngoingTaskState] = None,
        task_connection_status: Optional[OngoingTaskConnectionStatus] = None,
        task_name: Optional[str] = None,
        error: Optional[str] = None,
        mentor_node: Optional[str] = None,
        pin_to_mentor_node: Optional[bool] = None,
        hub_name: Optional[str] = None,
        mode: Optional[PullReplicationMode] = None,
        destination_url: Optional[str] = None,
        topology_discovery_urls: Optional[list] = None,
        destination_database: Optional[str] = None,
        connection_string_name: Optional[str] = None,
        certificate_public_key: Optional[str] = None,
        access_name: Optional[str] = None,
        allowed_hub_to_sink_paths: Optional[list] = None,
        allowed_sink_to_hub_paths: Optional[list] = None,
    ):
        super().__init__(
            task_id=task_id,
            task_type=OngoingTaskType.PULL_REPLICATION_AS_SINK,
            responsible_node=responsible_node,
            task_state=task_state,
            task_connection_status=task_connection_status,
            task_name=task_name,
            error=error,
            mentor_node=mentor_node,
            pin_to_mentor_node=pin_to_mentor_node,
        )
        self.hub_name = hub_name
        self.mode = mode
        self.destination_url = destination_url
        self.topology_discovery_urls = topology_discovery_urls
        self.destination_database = destination_database
        self.connection_string_name = connection_string_name
        self.certificate_public_key = certificate_public_key
        self.access_name = access_name
        self.allowed_hub_to_sink_paths = allowed_hub_to_sink_paths
        self.allowed_sink_to_hub_paths = allowed_sink_to_hub_paths

    def to_json(self) -> dict:
        result = super().to_json()
        result["HubName"] = self.hub_name
        result["Mode"] = self.mode.value if self.mode else None
        result["DestinationUrl"] = self.destination_url
        result["TopologyDiscoveryUrls"] = self.topology_discovery_urls
        result["DestinationDatabase"] = self.destination_database
        result["ConnectionStringName"] = self.connection_string_name
        result["CertificatePublicKey"] = self.certificate_public_key
        result["AccessName"] = self.access_name
        result["AllowedHubToSinkPaths"] = self.allowed_hub_to_sink_paths
        result["AllowedSinkToHubPaths"] = self.allowed_sink_to_hub_paths
        return result

    @classmethod
    def from_json(cls, json_dict: dict) -> Optional["OngoingTaskPullReplicationAsSink"]:
        if json_dict is None:
            return None
        task_state_str = json_dict.get("TaskState")
        task_connection_status_str = json_dict.get("TaskConnectionStatus")
        mode_str = json_dict.get("Mode")
        return cls(
            task_id=json_dict.get("TaskId"),
            responsible_node=NodeId.from_json(json_dict.get("ResponsibleNode")),
            task_state=OngoingTaskState(task_state_str) if task_state_str else None,
            task_connection_status=(
                OngoingTaskConnectionStatus(task_connection_status_str) if task_connection_status_str else None
            ),
            task_name=json_dict.get("TaskName"),
            error=json_dict.get("Error"),
            mentor_node=json_dict.get("MentorNode"),
            pin_to_mentor_node=json_dict.get("PinToMentorNode"),
            hub_name=json_dict.get("HubName"),
            mode=PullReplicationMode(mode_str) if mode_str else None,
            destination_url=json_dict.get("DestinationUrl"),
            topology_discovery_urls=json_dict.get("TopologyDiscoveryUrls"),
            destination_database=json_dict.get("DestinationDatabase"),
            connection_string_name=json_dict.get("ConnectionStringName"),
            certificate_public_key=json_dict.get("CertificatePublicKey"),
            access_name=json_dict.get("AccessName"),
            allowed_hub_to_sink_paths=json_dict.get("AllowedHubToSinkPaths"),
            allowed_sink_to_hub_paths=json_dict.get("AllowedSinkToHubPaths"),
        )


class ToggleOngoingTaskStateOperation(MaintenanceOperation[ModifyOngoingTaskResult]):
    def __init__(
        self, task_name_or_id: Union[int, str], type_of_task: Optional[OngoingTaskType], disable: Optional[bool]
    ):
        if isinstance(task_name_or_id, str):
            task_name = task_name_or_id
            if not task_name or task_name.isspace():
                raise RuntimeError("Task name id must have a non empty value")

            self._task_name = task_name
            self._task_id = 0
        elif isinstance(task_name_or_id, int):
            task_id = task_name_or_id
            self._task_name = None
            self._task_id = task_id
        else:
            raise TypeError("Unexpected type of the 'task_name_or_id'.")

        self._type_of_task = type_of_task
        self._disable = disable

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[ModifyOngoingTaskResult]:
        return ToggleOngoingTaskStateOperation._ToggleTaskStateCommand(
            self._task_id, self._task_name, self._type_of_task, self._disable
        )

    class _ToggleTaskStateCommand(RavenCommand[ModifyOngoingTaskResult], RaftCommand):
        def __init__(self, task_id: int, task_name: str, type_of_task: OngoingTaskType, disable: bool):
            super(ToggleOngoingTaskStateOperation._ToggleTaskStateCommand, self).__init__(ModifyOngoingTaskResult)
            self._task_id = task_id
            self._task_name = task_name
            self._type_of_task = type_of_task
            self._disable = disable

        def create_request(self, node: ServerNode) -> requests.Request:
            url = (
                f"{node.url}/databases/{node.database}/admin/tasks/state"
                f"?key={self._task_id}"
                f"&type={self._type_of_task.value}"
                f"&disable={'true' if self._disable else 'false'}"
            )

            if self._task_name is not None:
                url += f"&taskName={Utils.quote_key(self._task_name)}"

            return requests.Request("POST", url)

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is not None:
                self.result = ModifyOngoingTaskResult.from_json(json.loads(response))

        def is_read_request(self) -> bool:
            return False

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class DeleteOngoingTaskOperation(MaintenanceOperation[ModifyOngoingTaskResult]):
    """Operation to delete an ongoing task."""

    def __init__(self, task_id: int, task_type: OngoingTaskType):
        """
        Initialize the delete operation.

        Args:
            task_id: The unique identifier of the ongoing task to be deleted.
            task_type: The type of the ongoing task.
        """
        self._task_id = task_id
        self._task_type = task_type

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[ModifyOngoingTaskResult]:
        return DeleteOngoingTaskOperation._DeleteOngoingTaskCommand(self._task_id, self._task_type)

    class _DeleteOngoingTaskCommand(RavenCommand[ModifyOngoingTaskResult], RaftCommand):
        def __init__(self, task_id: int, task_type: OngoingTaskType):
            super().__init__(ModifyOngoingTaskResult)
            self._task_id = task_id
            self._task_type = task_type

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/tasks?id={self._task_id}&type={self._task_type.value}"
            return requests.Request("DELETE", url)

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is not None:
                self.result = ModifyOngoingTaskResult.from_json(json.loads(response))

        def is_read_request(self) -> bool:
            return False

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class GetOngoingTaskInfoOperation(
    MaintenanceOperation[Union[OngoingTask, OngoingTaskGenAi, OngoingTaskEmbeddingsGeneration]]
):
    """
    Operation to retrieve detailed information about a specific ongoing task.
    Ongoing tasks include various types of tasks such as replication, ETL, backup, and subscriptions.
    """

    def __init__(self, task_id_or_name: Union[int, str], task_type: OngoingTaskType):
        """
        Initialize the get operation.

        Args:
            task_id_or_name: The unique identifier or name of the ongoing task to retrieve information for.
            task_type: The type of the ongoing task, such as replication, ETL, or backup.

        Raises:
            ValueError: If the specified task type is PullReplicationAsHub, which is not supported.
        """
        if task_type == OngoingTaskType.PULL_REPLICATION_AS_HUB:
            raise ValueError(
                "PullReplicationAsHub type is not supported. Please use GetPullReplicationTasksInfoOperation instead."
            )

        if isinstance(task_id_or_name, str):
            if not task_id_or_name or task_id_or_name.isspace():
                raise ValueError("Task name cannot be null or whitespace.")
            self._task_name = task_id_or_name
            self._task_id = None
        else:
            self._task_id = task_id_or_name
            self._task_name = None

        self._task_type = task_type

    def get_command(
        self, conventions: "DocumentConventions"
    ) -> RavenCommand[OngoingTask | OngoingTaskGenAi | OngoingTaskEmbeddingsGeneration]:
        if self._task_name is not None:
            return GetOngoingTaskInfoOperation._GetOngoingTaskInfoCommand(
                task_name=self._task_name, task_type=self._task_type
            )
        return GetOngoingTaskInfoOperation._GetOngoingTaskInfoCommand(task_id=self._task_id, task_type=self._task_type)

    class _GetOngoingTaskInfoCommand(RavenCommand[OngoingTask | OngoingTaskGenAi | OngoingTaskEmbeddingsGeneration]):
        def __init__(
            self,
            task_type: OngoingTaskType,
            task_id: Optional[int] = None,
            task_name: Optional[str] = None,
        ):
            super().__init__(OngoingTask)
            self._task_id = task_id
            self._task_name = task_name
            self._task_type = task_type

        def create_request(self, node: ServerNode) -> requests.Request:
            if self._task_name is not None:
                url = (
                    f"{node.url}/databases/{node.database}/task"
                    f"?taskName={Utils.quote_key(self._task_name)}&type={self._task_type.value}"
                )
            else:
                url = f"{node.url}/databases/{node.database}/task?key={self._task_id}&type={self._task_type.value}"
            return requests.Request("GET", url)

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is not None:
                json_dict = json.loads(response)
                self.result = self._deserialize_task(json_dict)

        def _deserialize_task(
            self, json_dict: dict
        ) -> OngoingTask | OngoingTaskGenAi | OngoingTaskEmbeddingsGeneration:
            """Deserialize the task based on its type."""
            if self._task_type == OngoingTaskType.GEN_AI:
                return OngoingTaskGenAi.from_json(json_dict)
            elif self._task_type == OngoingTaskType.EMBEDDINGS_GENERATION:
                return OngoingTaskEmbeddingsGeneration.from_json(json_dict)
            elif self._task_type == OngoingTaskType.PULL_REPLICATION_AS_SINK:
                return OngoingTaskPullReplicationAsSink.from_json(json_dict)
            else:
                # todo: handle more types of tasks
                return OngoingTask.from_json(json_dict)

        def is_read_request(self) -> bool:
            return False
