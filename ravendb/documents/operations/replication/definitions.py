from __future__ import annotations

import enum
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from ravendb.tools.utils import Utils


class PullReplicationMode(enum.Enum):
    """Direction of data flow between a pull-replication hub and sink (the C# ``[Flags]``
    combination serializes as the comma-separated string the server expects/returns)."""

    NONE = "None"
    HUB_TO_SINK = "HubToSink"
    SINK_TO_HUB = "SinkToHub"
    HUB_TO_SINK_AND_SINK_TO_HUB = "HubToSink, SinkToHub"

    def __str__(self) -> str:
        return self.value


class PreventDeletionsMode(enum.Enum):
    NONE = "None"
    PREVENT_SINK_TO_HUB_DELETIONS = "PreventSinkToHubDeletions"

    def __str__(self) -> str:
        return self.value


class ReplicationType(enum.Enum):
    EXTERNAL = "External"
    PULL_AS_SINK = "PullAsSink"
    PULL_AS_HUB = "PullAsHub"
    INTERNAL = "Internal"
    MIGRATION = "Migration"

    def __str__(self) -> str:
        return self.value


class ReplicationNode:
    def __init__(self, url: str = None, database: str = None, disabled: bool = False):
        self.url = url.rstrip("/") if url else url
        self.database = database
        self.disabled = disabled

    def get_replication_type(self) -> Optional[ReplicationType]:
        return None

    def to_json(self) -> Dict[str, Any]:
        replication_type = self.get_replication_type()
        return {
            "Database": self.database,
            "Url": self.url,
            "Disabled": self.disabled,
            "Type": replication_type.value if replication_type is not None else None,
        }


class ExternalReplicationBase(ReplicationNode):
    def __init__(
        self,
        database: str = None,
        connection_string_name: str = None,
        name: str = None,
        mentor_node: str = None,
        pin_to_mentor_node: bool = False,
        task_id: int = 0,
        url: str = None,
        disabled: bool = False,
    ):
        super().__init__(url, database, disabled)
        self.task_id = task_id
        self.name = name
        self.connection_string_name = connection_string_name
        self.mentor_node = mentor_node
        self.pin_to_mentor_node = pin_to_mentor_node

    def to_json(self) -> Dict[str, Any]:
        json_dict = super().to_json()
        json_dict.update(
            {
                "TaskId": self.task_id,
                "Name": self.name,
                "MentorNode": self.mentor_node,
                "PinToMentorNode": self.pin_to_mentor_node,
                "ConnectionStringName": self.connection_string_name,
            }
        )
        return json_dict

    def _fill_from_json(self, json_dict: Dict[str, Any]) -> None:
        self.url = json_dict.get("Url")
        self.database = json_dict.get("Database")
        self.disabled = json_dict.get("Disabled", False)
        self.task_id = json_dict.get("TaskId", 0)
        self.name = json_dict.get("Name")
        self.connection_string_name = json_dict.get("ConnectionStringName")
        self.mentor_node = json_dict.get("MentorNode")
        self.pin_to_mentor_node = json_dict.get("PinToMentorNode", False)


class ExternalReplication(ExternalReplicationBase):
    def __init__(
        self,
        database: str = None,
        connection_string_name: str = None,
        delay_replication_for: timedelta = None,
        name: str = None,
        mentor_node: str = None,
        pin_to_mentor_node: bool = False,
        task_id: int = 0,
    ):
        super().__init__(
            database=database,
            connection_string_name=connection_string_name,
            name=name,
            mentor_node=mentor_node,
            pin_to_mentor_node=pin_to_mentor_node,
            task_id=task_id,
        )
        self.delay_replication_for = delay_replication_for

    def get_replication_type(self) -> ReplicationType:
        return ReplicationType.EXTERNAL

    def to_json(self) -> Dict[str, Any]:
        json_dict = super().to_json()
        json_dict["DelayReplicationFor"] = (
            Utils.timedelta_to_str(self.delay_replication_for) if self.delay_replication_for is not None else None
        )
        return json_dict


class PullReplicationDefinition:
    """A pull-replication hub definition (the source side of a pull-replication task)."""

    def __init__(
        self,
        name: str = None,
        delay_replication_for: timedelta = None,
        mentor_node: str = None,
        disabled: bool = False,
        mode: PullReplicationMode = None,
        task_id: int = 0,
        with_filtering: bool = False,
        prevent_deletions_mode: PreventDeletionsMode = None,
        pin_to_mentor_node: bool = False,
    ):
        self.name = name
        self.delay_replication_for = delay_replication_for
        self.mentor_node = mentor_node
        self.disabled = disabled
        self.mode = mode if mode is not None else PullReplicationMode.HUB_TO_SINK
        self.task_id = task_id
        self.with_filtering = with_filtering
        self.prevent_deletions_mode = prevent_deletions_mode
        self.pin_to_mentor_node = pin_to_mentor_node

    def to_json(self) -> Dict[str, Any]:
        return {
            "Name": self.name,
            "DelayReplicationFor": (
                Utils.timedelta_to_str(self.delay_replication_for) if self.delay_replication_for is not None else None
            ),
            "MentorNode": self.mentor_node,
            "PinToMentorNode": self.pin_to_mentor_node,
            "Disabled": self.disabled,
            "Mode": self.mode.value if self.mode else None,
            "TaskId": self.task_id,
            "WithFiltering": self.with_filtering,
            "PreventDeletionsMode": self.prevent_deletions_mode.value if self.prevent_deletions_mode else None,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> Optional[PullReplicationDefinition]:
        if json_dict is None:
            return None
        delay = json_dict.get("DelayReplicationFor")
        mode = json_dict.get("Mode")
        prevent = json_dict.get("PreventDeletionsMode")
        return cls(
            name=json_dict.get("Name"),
            delay_replication_for=Utils.string_to_timedelta(delay) if delay else None,
            mentor_node=json_dict.get("MentorNode"),
            disabled=json_dict.get("Disabled", False),
            mode=PullReplicationMode(mode) if mode else None,
            task_id=json_dict.get("TaskId", 0),
            with_filtering=json_dict.get("WithFiltering", False),
            prevent_deletions_mode=PreventDeletionsMode(prevent) if prevent else None,
            pin_to_mentor_node=json_dict.get("PinToMentorNode", False),
        )


class PullReplicationAsSink(ExternalReplicationBase):
    """A pull-replication sink definition (the destination side of a pull-replication task)."""

    def __init__(
        self,
        database: str = None,
        connection_string_name: str = None,
        hub_name: str = None,
        mode: PullReplicationMode = None,
        allowed_hub_to_sink_paths: List[str] = None,
        allowed_sink_to_hub_paths: List[str] = None,
        certificate_with_private_key: str = None,
        certificate_password: str = None,
        access_name: str = None,
        name: str = None,
        mentor_node: str = None,
        pin_to_mentor_node: bool = False,
        task_id: int = 0,
    ):
        super().__init__(
            database=database,
            connection_string_name=connection_string_name,
            name=name,
            mentor_node=mentor_node,
            pin_to_mentor_node=pin_to_mentor_node,
            task_id=task_id,
        )
        self.hub_name = hub_name
        self.mode = mode if mode is not None else PullReplicationMode.HUB_TO_SINK
        self.allowed_hub_to_sink_paths = allowed_hub_to_sink_paths
        self.allowed_sink_to_hub_paths = allowed_sink_to_hub_paths
        self.certificate_with_private_key = certificate_with_private_key
        self.certificate_password = certificate_password
        self.access_name = access_name

    def get_replication_type(self) -> ReplicationType:
        return ReplicationType.PULL_AS_SINK

    def to_json(self) -> Dict[str, Any]:
        json_dict = super().to_json()
        json_dict.update(
            {
                "Mode": self.mode.value if self.mode else None,
                "HubName": self.hub_name,
                "AllowedHubToSinkPaths": self.allowed_hub_to_sink_paths,
                "AllowedSinkToHubPaths": self.allowed_sink_to_hub_paths,
                "CertificateWithPrivateKey": self.certificate_with_private_key,
                "CertificatePassword": self.certificate_password,
                "AccessName": self.access_name,
            }
        )
        return json_dict

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> Optional[PullReplicationAsSink]:
        if json_dict is None:
            return None
        obj = cls()
        obj._fill_from_json(json_dict)
        mode = json_dict.get("Mode")
        obj.mode = PullReplicationMode(mode) if mode else None
        obj.hub_name = json_dict.get("HubName")
        obj.allowed_hub_to_sink_paths = json_dict.get("AllowedHubToSinkPaths")
        obj.allowed_sink_to_hub_paths = json_dict.get("AllowedSinkToHubPaths")
        obj.certificate_with_private_key = json_dict.get("CertificateWithPrivateKey")
        obj.certificate_password = json_dict.get("CertificatePassword")
        obj.access_name = json_dict.get("AccessName")
        return obj


class ReplicationHubAccess:
    """Grants a sink (identified by a certificate) access to a replication hub."""

    def __init__(
        self,
        name: str = None,
        certificate_base64: str = None,
        allowed_hub_to_sink_paths: List[str] = None,
        allowed_sink_to_hub_paths: List[str] = None,
    ):
        self.name = name
        self.certificate_base64 = certificate_base64
        self.allowed_hub_to_sink_paths = allowed_hub_to_sink_paths
        self.allowed_sink_to_hub_paths = allowed_sink_to_hub_paths

    def to_json(self) -> Dict[str, Any]:
        return {
            "Name": self.name,
            "CertificateBase64": self.certificate_base64,
            "AllowedHubToSinkPaths": self.allowed_hub_to_sink_paths,
            "AllowedSinkToHubPaths": self.allowed_sink_to_hub_paths,
        }


class DetailedReplicationHubAccess:
    def __init__(
        self,
        name: Optional[str] = None,
        thumbprint: Optional[str] = None,
        certificate: Optional[str] = None,
        not_before: Optional[datetime] = None,
        not_after: Optional[datetime] = None,
        subject: Optional[str] = None,
        issuer: Optional[str] = None,
        allowed_hub_to_sink_paths: Optional[List[str]] = None,
        allowed_sink_to_hub_paths: Optional[List[str]] = None,
    ):
        self.name = name
        self.thumbprint = thumbprint
        self.certificate = certificate
        self.not_before = not_before
        self.not_after = not_after
        self.subject = subject
        self.issuer = issuer
        self.allowed_hub_to_sink_paths = allowed_hub_to_sink_paths
        self.allowed_sink_to_hub_paths = allowed_sink_to_hub_paths

    def to_json(self) -> Dict:
        return {
            "Name": self.name,
            "Thumbprint": self.thumbprint,
            "Certificate": self.certificate,
            "NotBefore": Utils.datetime_to_string(self.not_before),
            "NotAfter": Utils.datetime_to_string(self.not_after),
            "Subject": self.subject,
            "Issuer": self.issuer,
            "AllowedHubToSinkPaths": self.allowed_hub_to_sink_paths,
            "AllowedSinkToHubPaths": self.allowed_sink_to_hub_paths,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> Optional[DetailedReplicationHubAccess]:
        if json_dict is None:
            return None
        not_before = json_dict.get("NotBefore")
        not_after = json_dict.get("NotAfter")
        return cls(
            name=json_dict.get("Name"),
            thumbprint=json_dict.get("Thumbprint"),
            certificate=json_dict.get("Certificate"),
            not_before=Utils.string_to_datetime(not_before) if not_before else None,
            not_after=Utils.string_to_datetime(not_after) if not_after else None,
            subject=json_dict.get("Subject"),
            issuer=json_dict.get("Issuer"),
            allowed_hub_to_sink_paths=json_dict.get("AllowedHubToSinkPaths"),
            allowed_sink_to_hub_paths=json_dict.get("AllowedSinkToHubPaths"),
        )


class ReplicationHubAccessResult:
    def __init__(self, results: List[DetailedReplicationHubAccess] = None):
        self.results = results

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> ReplicationHubAccessResult:
        results = json_dict.get("Results") or []
        return cls([DetailedReplicationHubAccess.from_json(item) for item in results])


class PullReplicationDefinitionAndCurrentConnections:
    def __init__(self, definition: PullReplicationDefinition = None, ongoing_tasks: List = None):
        self.definition = definition
        self.ongoing_tasks = ongoing_tasks

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> Optional[PullReplicationDefinitionAndCurrentConnections]:
        if json_dict is None:
            return None
        from ravendb.documents.operations.ongoing_tasks import OngoingTaskPullReplicationAsHub

        definition = json_dict.get("Definition")
        ongoing = json_dict.get("OngoingTasks") or []
        return cls(
            definition=PullReplicationDefinition.from_json(definition) if definition else None,
            ongoing_tasks=[OngoingTaskPullReplicationAsHub.from_json(item) for item in ongoing],
        )
