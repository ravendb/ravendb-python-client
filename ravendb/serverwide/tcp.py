from __future__ import annotations

import json
import logging
import socket
from enum import Enum
from typing import Optional, Tuple, Dict, Callable

from ravendb.documents.operations.replication.definitions import DetailedReplicationHubAccess


class TcpConnectionHeaderMessage:
    class OperationTypes(Enum):
        NONE = "None"
        DROP = "Drop"
        SUBSCRIPTION = "Subscription"
        REPLICATION = "REplication"
        CLUSTER = "Cluster"
        HEARTBEATS = "Heartbeats"
        PING = "Ping"
        TEST_CONNECTION = "TestConnection"

    def __init__(
        self,
        database_name: Optional[str] = None,
        source_node_tag: Optional[str] = None,
        operation: Optional[OperationTypes] = None,
        operation_version: Optional[int] = None,
        info: Optional[str] = None,
        authorize_info: Optional[AuthorizationInfo] = None,
        replication_hub_access: Optional[DetailedReplicationHubAccess] = None,
    ):
        self.database_name = database_name
        self.source_node_tag = source_node_tag
        self.operation = operation
        self.operation_version = operation_version
        self.info = info
        self.authorize_info = authorize_info
        self.replication_hub_access = replication_hub_access

    def to_json(self) -> Dict:
        return {
            "DatabaseName": self.database_name,
            "SourceNodeTag": self.source_node_tag,
            "Operation": self.operation,
            "OperationVersion": self.operation_version,
            "Info": self.info,
            "AuthorizeInfo": self.authorize_info.to_json(),
            "ReplicationHubAccess": self.replication_hub_access.to_json(),
        }

    NUMBER_OR_RETRIES_FOR_SENDING_TCP_HEADER = 2
    PING_BASE_LINE = -1
    NONE_BASE_LINE = -1
    DROP_BASE_LINE = -2
    HEARTBEATS_BASE_LINE = 20
    HEARTBEATS_41200 = 41_200
    HEARTBEATS_42000 = 42_000
    SUBSCRIPTION_BASE_LINE = 40
    SUBSCRIPTION_INCLUDES = 41_400
    SUBSCRIPTION_COUNTER_INCLUDES = 50_000
    SUBSCRIPTION_TIME_SERIES_INCLUDES = 51_000
    TEST_CONNECTION_BASE_LINE = 50

    HEARTBEATS_TCP_VERSION = HEARTBEATS_42000
    SUBSCRIPTION_TCP_VERSION = SUBSCRIPTION_TIME_SERIES_INCLUDES
    TEST_CONNECTION_TCP_VERSION = TEST_CONNECTION_BASE_LINE

    class SupportedFeatures:
        class PingFeatures:
            base_line = True

        class NoneFeatures:
            base_line = True

        class DropFeatures:
            base_line = True

        class SubscriptionFeatures:
            base_line = True

            def __init__(
                self,
                includes: Optional[bool] = None,
                counter_includes: Optional[bool] = None,
                time_series_includes: Optional[bool] = None,
            ):
                self.includes = includes
                self.counter_includes = counter_includes
                self.time_series_includes = time_series_includes

        class HeartbeatsFeatures:
            base_line = True

            def __init__(self, send_changes_only: Optional[bool] = None, include_server_info: Optional[bool] = None):
                self.send_changes_only = send_changes_only
                self.include_server_info = include_server_info

        class TestConnectionFeatures:
            base_line = True

        def __init__(
            self,
            version: int,
            ping: Optional[PingFeatures] = None,
            none: Optional[NoneFeatures] = None,
            drop: Optional[DropFeatures] = None,
            subscription: Optional[SubscriptionFeatures] = None,
            heartbeats: Optional[HeartbeatsFeatures] = None,
            test_connection: Optional[TestConnectionFeatures] = None,
        ):
            self.protocol_version = version
            self.ping = ping
            self.none = none
            self.drop = drop
            self.subscription = subscription
            self.heartbeats = heartbeats
            self.test_connection = test_connection

    operations_to_supported_protocol_versions = {
        OperationTypes.PING: [PING_BASE_LINE],
        OperationTypes.NONE: [NONE_BASE_LINE],
        OperationTypes.DROP: [DROP_BASE_LINE],
        OperationTypes.SUBSCRIPTION: [
            SUBSCRIPTION_TIME_SERIES_INCLUDES,
            SUBSCRIPTION_COUNTER_INCLUDES,
            SUBSCRIPTION_INCLUDES,
            SUBSCRIPTION_BASE_LINE,
        ],
        OperationTypes.HEARTBEATS: [HEARTBEATS_42000, HEARTBEATS_41200, HEARTBEATS_BASE_LINE],
        OperationTypes.TEST_CONNECTION: [TEST_CONNECTION_BASE_LINE],
    }

    supported_features_by_protocol = {
        OperationTypes.PING: {PING_BASE_LINE: SupportedFeatures(PING_BASE_LINE, SupportedFeatures.PingFeatures())},
        OperationTypes.NONE: {NONE_BASE_LINE: SupportedFeatures(NONE_BASE_LINE, none=SupportedFeatures.NoneFeatures())},
        OperationTypes.DROP: {DROP_BASE_LINE: SupportedFeatures(DROP_BASE_LINE, drop=SupportedFeatures.DropFeatures())},
        OperationTypes.SUBSCRIPTION: {
            SUBSCRIPTION_BASE_LINE: SupportedFeatures(
                SUBSCRIPTION_BASE_LINE, subscription=SupportedFeatures.SubscriptionFeatures()
            ),
            SUBSCRIPTION_INCLUDES: SupportedFeatures(
                SUBSCRIPTION_INCLUDES, subscription=SupportedFeatures.SubscriptionFeatures(True)
            ),
            SUBSCRIPTION_COUNTER_INCLUDES: SupportedFeatures(
                SUBSCRIPTION_COUNTER_INCLUDES, subscription=SupportedFeatures.SubscriptionFeatures(True, True)
            ),
            SUBSCRIPTION_TIME_SERIES_INCLUDES: SupportedFeatures(
                SUBSCRIPTION_TIME_SERIES_INCLUDES, subscription=SupportedFeatures.SubscriptionFeatures(True, True, True)
            ),
        },
        OperationTypes.HEARTBEATS: {
            HEARTBEATS_BASE_LINE: SupportedFeatures(
                HEARTBEATS_BASE_LINE, heartbeats=SupportedFeatures.HeartbeatsFeatures()
            ),
            HEARTBEATS_41200: SupportedFeatures(
                HEARTBEATS_41200, heartbeats=SupportedFeatures.HeartbeatsFeatures(True)
            ),
            HEARTBEATS_42000: SupportedFeatures(
                HEARTBEATS_42000, heartbeats=SupportedFeatures.HeartbeatsFeatures(True, True)
            ),
        },
        OperationTypes.TEST_CONNECTION: {
            TEST_CONNECTION_BASE_LINE: SupportedFeatures(
                TEST_CONNECTION_BASE_LINE, test_connection=SupportedFeatures.TestConnectionFeatures()
            )
        },
    }

    operations = [
        OperationTypes.CLUSTER,
        OperationTypes.DROP,
        OperationTypes.HEARTBEATS,
        OperationTypes.NONE,
        OperationTypes.PING,
        OperationTypes.REPLICATION,
        OperationTypes.SUBSCRIPTION,
        OperationTypes.TEST_CONNECTION,
    ]

    class SupportedStatus(Enum):
        OUT_OF_RANGE = "OutOfRange"
        NOT_SUPPORTED = "NotSupported"
        SUPPORTED = "Supported"

    @staticmethod
    def operation_version_supported(operation_type: OperationTypes, version: int) -> Tuple[SupportedStatus, int]:
        current = -1
        supported_protocols = TcpConnectionHeaderMessage.operations_to_supported_protocol_versions.get(operation_type)
        if supported_protocols is None:
            raise RuntimeError(
                f"This is a bug. Probably you forgot to add '{operation_type}' operation to the operations_to_supported_protocol_versions dict"
            )

        for ver in supported_protocols:
            current = ver
            if ver == version:
                return TcpConnectionHeaderMessage.SupportedStatus.SUPPORTED, current

            if ver < version:
                return TcpConnectionHeaderMessage.SupportedStatus.NOT_SUPPORTED, current

        return TcpConnectionHeaderMessage.SupportedStatus.OUT_OF_RANGE, current

    @staticmethod
    def get_operation_tcp_version(operation_type: OperationTypes, index: int) -> int:
        # we don't check the if the index go out of range, since this is expected and means that we don't have
        if operation_type in [
            TcpConnectionHeaderMessage.OperationTypes.PING,
            TcpConnectionHeaderMessage.OperationTypes.NONE,
        ]:
            return -1
        if operation_type == TcpConnectionHeaderMessage.OperationTypes.DROP:
            return -2
        if operation_type in [
            TcpConnectionHeaderMessage.OperationTypes.SUBSCRIPTION,
            TcpConnectionHeaderMessage.OperationTypes.REPLICATION,
            TcpConnectionHeaderMessage.OperationTypes.CLUSTER,
            TcpConnectionHeaderMessage.OperationTypes.HEARTBEATS,
            TcpConnectionHeaderMessage.OperationTypes.TEST_CONNECTION,
        ]:
            return TcpConnectionHeaderMessage.operations_to_supported_protocol_versions.get(operation_type)[index]
        raise ValueError("operation_type")

    @staticmethod
    def get_supported_features_for(op_type: OperationTypes, protocol_version: int) -> SupportedFeatures:
        features = TcpConnectionHeaderMessage.supported_features_by_protocol.get(op_type).get(protocol_version)
        if features is None:
            raise ValueError(f"{op_type} in protocol {protocol_version} was not found in the features set")
        return features

    class AuthorizationInfo:
        class AuthorizeMethod(Enum):
            SERVER = "Server"
            PULL_REPLICATION = "PullReplication"
            PUSH_REPLICATION = "PushReplication"

        def __init__(self, authorize_as: Optional[AuthorizeMethod] = None, authorization_for: Optional[str] = None):
            self.authorize_as = authorize_as
            self.authorization_for = authorization_for

        def to_json(self) -> Dict:
            return {"AuthorizeAs": self.authorize_as.value, "AuthorizationFor": self.authorization_for}


class TcpNegotiateParameters:
    def __init__(
        self,
        operation: Optional[TcpConnectionHeaderMessage.OperationTypes] = None,
        authorize_info: Optional[TcpConnectionHeaderMessage.AuthorizationInfo] = None,
        version: Optional[int] = None,
        database: Optional[str] = None,
        source_node_tag: Optional[str] = None,
        destination_node_tag: Optional[str] = None,
        destination_url: Optional[str] = None,
        read_response_and_get_version_callback: Optional[Callable[[str], int]] = None,
    ):
        self.operation = operation
        self.authorize_info = authorize_info
        self.version = version
        self.database = database
        self.source_node_tag = source_node_tag
        self.destination_node_tag = destination_node_tag
        self.destination_url = destination_url
        self.read_response_and_get_version_callback = read_response_and_get_version_callback


class TcpNegotiation:
    logger = logging.getLogger("TcpNegotiation")
    OUT_OF_RANGE_STATUS = -1
    DROP_STATUS = -2

    @classmethod
    def negotiate_protocol_version(
        cls, sock: socket.socket, parameters: TcpNegotiateParameters
    ) -> TcpConnectionHeaderMessage.SupportedFeatures:
        cls.logger.info(
            f"Start of negotiation for {parameters.operation} operation with {parameters.destination_node_tag or parameters.destination_url}"
        )
        current = parameters.version
        while True:
            cls._send_tcp_version_info(sock, parameters, current)
            version = parameters.read_response_and_get_version_callback(parameters.destination_url)

            cls.logger.info(
                f"Read response from {parameters.source_node_tag or parameters.destination_url} "
                f"for {parameters.operation}, received version is '{version}'"
            )

            if version == current:
                break

            if version == cls.DROP_STATUS:
                return TcpConnectionHeaderMessage.get_supported_features_for(
                    TcpConnectionHeaderMessage.OperationTypes.DROP, TcpConnectionHeaderMessage.DROP_BASE_LINE
                )

            status, current = TcpConnectionHeaderMessage.operation_version_supported(parameters.operation, version)
            if status == TcpConnectionHeaderMessage.SupportedStatus.OUT_OF_RANGE:
                cls._send_tcp_version_info(sock, parameters, cls.OUT_OF_RANGE_STATUS)
                raise ValueError(
                    f"The {parameters.operation} version {parameters.version} is out of range, out lowest version is {current}"
                )

            cls.logger.info(
                f"The version {version} is {status}, will try to agree on '{current}' for {parameters.operation} with {parameters.destination_node_tag or parameters.destination_url}"
            )

        cls.logger.info(
            f"{parameters.destination_node_tag or parameters.destination_url} agreed on version {current} for {parameters.operation}"
        )

        return TcpConnectionHeaderMessage.get_supported_features_for(parameters.operation, current)

    @classmethod
    def _send_tcp_version_info(
        cls, sock: socket.socket, parameters: TcpNegotiateParameters, current_version: int
    ) -> None:
        cls.logger.info(f"Send negotiation for {parameters.operation} in version {current_version}")
        json_dict = {
            "DatabaseName": parameters.database,
            "Operation": parameters.operation.value,
            "SourceNodeTag": parameters.source_node_tag,
            "OperationVersion": current_version,
            "AuthorizeInfo": parameters.authorize_info.to_json() if parameters.authorize_info is not None else None,
        }

        sock.send(json.dumps(json_dict).encode("utf-8"))


class TcpConnectionStatus(Enum):
    OK = "Ok"
    AUTHORIZATION_FAILED = "AuthorizationFailed"
    TCP_VERSION_MISMATCH = "TcpVersionMismatch"


class TcpConnectionHeaderResponse:
    def __init__(self, status: TcpConnectionStatus, message: str, version: int):
        self.status = status
        self.message = message
        self.version = version

    @classmethod
    def from_json(cls, json_dict: Dict) -> TcpConnectionHeaderResponse:
        return cls(
            TcpConnectionStatus(json_dict.get("Status", None)),
            json_dict.get("Message", None),
            json_dict.get("Version", None),
        )
