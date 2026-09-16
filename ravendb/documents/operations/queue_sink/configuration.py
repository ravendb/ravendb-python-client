from __future__ import annotations

from typing import Any, Dict, List, Optional

from ravendb.documents.operations.etl.queue.connection import QueueBrokerType


class QueueSinkScript:
    """
    A user-defined script that consumes messages from one or more queues and decides
    how they are stored in RavenDB.
    """

    def __init__(
        self,
        name: str = None,
        queues: List[str] = None,
        script: str = None,
        disabled: bool = False,
    ):
        self.name = name
        # Broker-specific source entries. For Azure Service Bus these are built with
        # AzureServiceBusSinkSource.queue() / .subscription().
        self.queues = queues or []
        self.script = script
        self.disabled = disabled

    def to_json(self) -> Dict[str, Any]:
        return {
            "Name": self.name,
            "Script": self.script,
            "Queues": self.queues,
            "Disabled": self.disabled,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> QueueSinkScript:
        return cls(
            name=json_dict.get("Name"),
            queues=json_dict.get("Queues"),
            script=json_dict.get("Script"),
            disabled=json_dict.get("Disabled", False),
        )


class QueueSinkConfiguration:
    """
    A queue sink task: RavenDB consumes messages from an external broker (Kafka,
    RabbitMQ, Azure Queue Storage, Amazon SQS, Azure Service Bus) and stores them.
    """

    def __init__(
        self,
        name: str = None,
        broker_type: QueueBrokerType = None,
        connection_string_name: str = None,
        scripts: List[QueueSinkScript] = None,
        task_id: int = 0,
        disabled: bool = False,
        mentor_node: str = None,
        pin_to_mentor_node: bool = False,
    ):
        self.name = name
        self.broker_type = broker_type
        self.connection_string_name = connection_string_name
        self.scripts = scripts or []
        self.task_id = task_id
        self.disabled = disabled
        self.mentor_node = mentor_node
        self.pin_to_mentor_node = pin_to_mentor_node

    def to_json(self) -> Dict[str, Any]:
        return {
            "Name": self.name,
            "TaskId": self.task_id,
            "Disabled": self.disabled,
            "ConnectionStringName": self.connection_string_name,
            "MentorNode": self.mentor_node,
            "PinToMentorNode": self.pin_to_mentor_node,
            "Scripts": [script.to_json() for script in self.scripts],
            "BrokerType": self.broker_type.value if self.broker_type else None,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> QueueSinkConfiguration:
        broker_type = json_dict.get("BrokerType")
        return cls(
            name=json_dict.get("Name"),
            broker_type=QueueBrokerType(broker_type) if broker_type else None,
            connection_string_name=json_dict.get("ConnectionStringName"),
            scripts=[QueueSinkScript.from_json(script) for script in json_dict.get("Scripts") or []],
            task_id=json_dict.get("TaskId", 0),
            disabled=json_dict.get("Disabled", False),
            mentor_node=json_dict.get("MentorNode"),
            pin_to_mentor_node=json_dict.get("PinToMentorNode", False),
        )


class QueueSinkProcessState:
    """Which node a queue sink script last ran on."""

    def __init__(self, node_tag: str = None, configuration_name: str = None, script_name: str = None):
        self.node_tag = node_tag
        self.configuration_name = configuration_name
        self.script_name = script_name

    def to_json(self) -> Dict[str, Any]:
        return {
            "ConfigurationName": self.configuration_name,
            "ScriptName": self.script_name,
            "NodeTag": self.node_tag,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> QueueSinkProcessState:
        return cls(
            node_tag=json_dict.get("NodeTag"),
            configuration_name=json_dict.get("ConfigurationName"),
            script_name=json_dict.get("ScriptName"),
        )

    @staticmethod
    def generate_item_name(database_name: str, configuration_name: str, transformation_name: str) -> str:
        return f"values/{database_name}/queuesink/{configuration_name.lower()}/{transformation_name.lower()}"


class AddQueueSinkOperationResult:
    def __init__(self, raft_command_index: Optional[int] = None, task_id: Optional[int] = None):
        self.raft_command_index = raft_command_index
        self.task_id = task_id

    def to_json(self) -> Dict[str, Any]:
        return {"RaftCommandIndex": self.raft_command_index, "TaskId": self.task_id}

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AddQueueSinkOperationResult:
        return cls(
            raft_command_index=json_dict.get("RaftCommandIndex"),
            task_id=json_dict.get("TaskId"),
        )


class UpdateQueueSinkOperationResult(AddQueueSinkOperationResult):
    # Same shape as the add result; kept as its own name so callers read what they got back.
    pass
