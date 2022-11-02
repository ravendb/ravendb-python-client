from __future__ import annotations

import concurrent.futures
import datetime
import json
import logging
import os
import time
from concurrent.futures import Future
from enum import Enum
from json import JSONDecoder, JSONDecodeError
from socket import socket
from typing import TypeVar, Generic, Type, Optional, Callable, Dict, List, TYPE_CHECKING, Any

from ravendb import constants
from ravendb.documents.session.entity_to_json import EntityToJson
from ravendb.documents.commands.subscriptions import GetTcpInfoForRemoteTaskCommand
from ravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
from ravendb.documents.session.document_info import DocumentInfo
from ravendb.documents.session.misc import SessionOptions, TransactionMode
from ravendb.documents.subscriptions.revision import Revision
from ravendb.exceptions.cluster import NodeIsPassiveException
from ravendb.exceptions.exceptions import (
    AuthorizationException,
    DatabaseDoesNotExistException,
    SubscriptionInUseException,
    SubscriptionClosedException,
    SubscriptionInvalidStateException,
    SubscriptionDoesNotBelongToNodeException,
    SubscriptionChangeVectorUpdateConcurrencyException,
    SubscriberErrorException,
    SubscriptionDoesNotExistException,
    AllTopologyNodesDownException,
)
from ravendb.exceptions.raven_exceptions import ClientVersionMismatchException
from ravendb.extensions.json_extensions import JsonExtensions
from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
from ravendb.documents.session.document_session import DocumentSession
from ravendb.documents.subscriptions.options import SubscriptionWorkerOptions
from ravendb.primitives.exceptions import OperationCancelledException
from ravendb.primitives.misc import CancellationTokenSource
from ravendb.http.request_executor import RequestExecutor
from ravendb.serverwide.commands import GetTcpInfoCommand
from ravendb.serverwide.tcp import (
    TcpNegotiateParameters,
    TcpConnectionHeaderMessage,
    TcpNegotiation,
    TcpConnectionHeaderResponse,
    TcpConnectionStatus,
)
from ravendb.tools.generate_id import GenerateEntityIdOnTheClient
from ravendb.http.server_node import ServerNode
from ravendb.util.tcp_utils import TcpUtils

_T = TypeVar("_T")
_T_Item = TypeVar("_T_Item")

if TYPE_CHECKING:
    from ravendb.documents.store.definition import DocumentStore


class SubscriptionConnectionServerMessage:
    class MessageType(Enum):
        NONE = "None"
        CONNECTION_STATUS = "ConnectionStatus"
        END_OF_BATCH = "EndOfBatch"
        DATA = "Data"
        INCLUDES = "Includes"
        COUNTER_INCLUDES = "CounterIncludes"
        TIME_SERIES_INCLUDES = "TimeSeriesIncludes"
        CONFIRM = "Confirm"
        ERROR = "Error"

    class ConnectionStatus(Enum):
        NONE = "None"
        ACCEPTED = "Accepted"
        IN_USE = "InUse"
        CLOSED = "Closed"
        NOT_FOUND = "NotFound"
        REDIRECT = "Redirect"
        FORBIDDEN_READ_ONLY = "ForbiddenReadOnly"
        FORBIDDEN = "Forbidden"
        INVALID = "Invalid"
        CONCURRENCY_RECONNECT = "ConcurrencyReconnect"

    class SubscriptionRedirectData:
        def __init__(
            self,
            current_tag: Optional[str] = None,
            redirected_tag: Optional[str] = None,
            reasons: Optional[Dict[str, str]] = None,
        ):
            self.current_tag = current_tag
            self.redirected_tag = redirected_tag
            self.reasons = reasons

    def __init__(
        self,
        type_of_message: Optional[MessageType] = None,
        status: Optional[ConnectionStatus] = None,
        data: Optional[Dict] = None,
        includes: Optional[Dict] = None,
        counter_includes: Optional[Dict] = None,
        included_counter_names: Optional[Dict[str, List[str]]] = None,
        time_series_includes: Optional[Dict] = None,
        exception: Optional[str] = None,
        message: Optional[str] = None,
    ):
        self.type_of_message = type_of_message
        self.status = status
        self.data = data
        self.includes = includes
        self.counter_includes = counter_includes
        self.included_counter_names = included_counter_names
        self.time_series_includes = time_series_includes
        self.exception = exception
        self.message = message

    @classmethod
    def from_json(cls, json_dict: Dict) -> SubscriptionConnectionServerMessage:
        return cls(
            SubscriptionConnectionServerMessage.MessageType(json_dict["Type"]),
            SubscriptionConnectionServerMessage.ConnectionStatus(json_dict["Status"])
            if "Status" in json_dict
            else None,
            json_dict["Data"] if "Data" in json_dict else None,
            json_dict["Includes"] if "Includes" in json_dict else None,
            json_dict["CounterIncludes"] if "CounterIncludes" in json_dict else None,
            json_dict["IncludedCounterNames"] if "IncludedCounterNames" in json_dict else None,
            json_dict["TimeSeriesIncludes"] if "TimeSeriesIncludes" in json_dict else None,
            json_dict["Exception"] if "Exception" in json_dict else None,
            json_dict["Message"] if "Message" in json_dict else None,
        )


class SubscriptionConnectionClientMessage:
    class MessageType(Enum):
        NONE = "None"
        ACKNOWLEDGE = "Acknowledge"
        DISPOSED_NOTIFICATION = "DisposedNotification"

    def __init__(self, type_of_message: Optional[MessageType] = None, change_vector: Optional[str] = None):
        self.type_of_message = type_of_message
        self.change_vector = change_vector

    def to_json(self) -> Dict:
        return {"Type": self.type_of_message.value, "ChangeVector": self.change_vector}


class SubscriptionWorker(Generic[_T]):
    def __init__(
        self,
        object_type: Type[_T],
        options: SubscriptionWorkerOptions,
        with_revisions: bool,
        document_store: DocumentStore,
        db_name: str,
    ):
        self._object_type = object_type
        self._options = options
        self._revisions = with_revisions
        if not options.subscription_name or options.subscription_name.isspace():
            raise ValueError("SubscriptionConnectionOptions must specify the subscription name")

        self._store = document_store
        self._db_name = self._store.get_effective_database(db_name)
        self._logger = logging.getLogger(SubscriptionWorker.__class__.__name__)

        self.after_acknowledgment = []
        self.on_subscription_connection_retry = []
        self.on_unexpected_subscription_error = []

        self._processing_cts = CancellationTokenSource()
        self._subscriber: Optional[Callable[[SubscriptionBatch[_T]], None]] = None
        self._tcp_client: Optional[socket] = None
        self._disposed: Optional[bool] = None
        self._subscription_task: Optional[Future[None]] = None
        self._forced_topology_update_attempts: int = 0

        self._after_acknowledgment: List[Callable[[SubscriptionBatch[_T]], None]] = []
        self._on_subscription_connection_retry: List[Callable[[Exception], None]] = []
        self._on_unexpected_subscription_error: List[Callable[[Exception], None]] = []

        self._redirect_node: Optional[ServerNode] = None
        self._subscription_local_request_executor: Optional[RequestExecutor] = None
        self._on_closed: Optional[Callable[[SubscriptionWorker[_T]], None]] = None

        self._last_connection_failure: Optional[datetime.datetime] = None
        self._supported_features: Optional[TcpConnectionHeaderMessage.SupportedFeatures] = None

        self._json_decoder = JSONDecoder()
        self._buffer = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close(True)

    def add_after_acknowledgment(self, handler: Callable[[SubscriptionBatch[_T]], None]) -> None:
        self._after_acknowledgment.append(handler)

    def remove_after_acknowledgment(self, handler: Callable[[SubscriptionBatch[_T]], None]) -> None:
        self._after_acknowledgment.remove(handler)

    def invoke_after_acknowledgment(self, batch: SubscriptionBatch[_T]) -> None:
        for event in self._after_acknowledgment:
            event(batch)

    def add_on_subscription_connection_retry(self, handler: Callable[[Exception], None]) -> None:
        self._on_subscription_connection_retry.append(handler)

    def remove_on_subscription_connection_retry(self, handler: Callable[[Exception], None]) -> None:
        self._on_subscription_connection_retry.remove(handler)

    def invoke_on_subscription_connection_retry(self, err: Exception) -> None:
        for event in self._on_subscription_connection_retry:
            event(err)

    def add_on_unexpected_subscription_error(self, handler: Callable[[Exception], None]) -> None:
        self._on_unexpected_subscription_error.append(handler)

    def remove_on_unexpected_subscription_error(self, handler: Callable[[Exception], None]) -> None:
        self._on_unexpected_subscription_error.remove(handler)

    def invoke_on_unexpected_subscription_error(self, err: Exception) -> None:
        for event in self._on_unexpected_subscription_error:
            event(err)

    def close(self, wait_for_subscription_task: bool = True) -> None:
        if self._disposed:
            return

        try:
            self._disposed = True
            self._processing_cts.cancel()

            self._close_tcp_client()  # we disconnect immediately

            if self._subscription_task is not None and wait_for_subscription_task:
                try:
                    self._subscription_task.result()
                except Exception:
                    pass  # just need to wait for it to end

            if self._subscription_local_request_executor is not None:
                self._subscription_local_request_executor.close()

        except Exception as ex:
            self._logger.debug(f"Error during close of subscription: {ex.args[0]}", ex)

        finally:
            if self._on_closed is not None:
                self._on_closed(self)

    def run(self, process_documents: Optional[Callable[[SubscriptionBatch[_T]], Any]]) -> Future[None]:
        if process_documents is None:
            raise ValueError("process_documents cannot be None")

        self._subscriber = process_documents
        return self._run()

    def _run(self) -> Future[None]:
        if self._subscription_task is not None:
            raise RuntimeError("The subscription is already running")

        self._subscription_task = self._run_subscription_async()
        return self._subscription_task

    @property
    def current_node_tag(self) -> Optional[str]:
        if self._redirect_node is not None:
            return self._redirect_node.cluster_tag
        return None

    @property
    def subscription_name(self) -> Optional[str]:
        if self._options is not None:
            return self._options.subscription_name
        return None

    def _connect_to_server(self) -> socket:
        command = GetTcpInfoForRemoteTaskCommand(
            f"Subscription/{self._db_name}",
            self._db_name,
            self._options.subscription_name if self._options else None,
            True,
        )

        request_executor = self._store.get_request_executor(self._db_name)

        if self._redirect_node is not None:
            try:
                request_executor.execute(self._redirect_node, None, command, False, None)
                tcp_info = command.result
            except ClientVersionMismatchException:
                tcp_info = self._legacy_try_get_tcp_info(request_executor, self._redirect_node)
            except Exception as e:
                # if we failed to talk to a node, we'll forget about it and let the topology to
                # redirect us to the current node

                self._redirect_node = None
                raise RuntimeError(e)

        else:
            try:
                request_executor.execute_command(command)
                tcp_info = command.result

                if tcp_info.node_tag is not None:
                    final_tcp_info = tcp_info
                    equal_nodes = list(
                        filter(lambda x: final_tcp_info.node_tag == x.cluster_tag, request_executor.topology.nodes)
                    )
                    self._redirect_node = equal_nodes[0] if len(equal_nodes) > 0 else None

            except ClientVersionMismatchException:
                tcp_info = self._legacy_try_get_tcp_info(request_executor)

        self._tcp_client, chosen_url = TcpUtils.connect_with_priority(
            tcp_info, command.result.certificate, self._store.certificate_pem_path
        )

        database_name = self._store.get_effective_database(self._db_name)

        parameters = TcpNegotiateParameters()
        parameters.database = database_name
        parameters.operation = TcpConnectionHeaderMessage.OperationTypes.SUBSCRIPTION
        parameters.version = TcpConnectionHeaderMessage.SUBSCRIPTION_TCP_VERSION
        parameters.read_response_and_get_version_callback = self._read_server_response_and_get_version
        parameters.destination_node_tag = self.current_node_tag
        parameters.destination_url = chosen_url

        self._supported_features = TcpNegotiation.negotiate_protocol_version(self._tcp_client, parameters)

        if self._supported_features.protocol_version <= 0:
            raise RuntimeError(
                f"{self._options.subscription_name} : TCP negotiation resulted with an invalid protocol version: "
                f"{self._supported_features.protocol_version}"
            )

        options = JsonExtensions.write_value_as_bytes(self._options)

        self._tcp_client.send(options)

        if self._subscription_local_request_executor is not None:
            self._subscription_local_request_executor.close()

        self._subscription_local_request_executor = (
            RequestExecutor.create_for_single_node_without_configuration_updates(
                command.requested_node.url,
                self._db_name,
                self._store.conventions,
                request_executor.certificate_path,
                request_executor.trust_store_path,
                self._store.thread_pool_executor,
            )
        )

        self._store.register_events_for_request_executor(self._subscription_local_request_executor)

        return self._tcp_client

    def _legacy_try_get_tcp_info(self, request_executor: RequestExecutor, node: Optional[ServerNode] = None):
        tcp_command = GetTcpInfoCommand(f"Subscription/{self._db_name}", self._db_name)

        try:
            request_executor.execute(node, None, tcp_command, False, None)
        except Exception as e:
            self._redirect_node = None
            raise e

        return tcp_command.result

    def _ensure_parser(self) -> None:
        # python doesn't use parsers
        pass

    def _read_server_response_and_get_version(self, url: str) -> int:
        # reading reply from server
        self._ensure_parser()
        response = self._tcp_client.recv(self._options.receive_buffer_size)
        reply = TcpConnectionHeaderResponse.from_json(json.loads(response.decode("utf-8")))

        if reply.status == TcpConnectionStatus.OK:
            return reply.version
        if reply.status == TcpConnectionStatus.AUTHORIZATION_FAILED:
            raise AuthorizationException(f"Cannot access database {self._db_name} because {reply.message}")
        if reply.status == TcpConnectionStatus.TCP_VERSION_MISMATCH:
            if reply.version != TcpNegotiation.OUT_OF_RANGE_STATUS:
                return reply.version
            # Kindly request the server to drop the connection
            self._send_drop_message(reply)
            raise RuntimeError(f"Can't connect to database {self._db_name} because: {reply.message}")

        return reply.version

    def _send_drop_message(self, reply: TcpConnectionHeaderResponse) -> None:
        drop_msg = TcpConnectionHeaderMessage()
        drop_msg.operation = TcpConnectionHeaderMessage.OperationTypes.DROP
        drop_msg.database_name = self._db_name
        drop_msg.operation_version = TcpConnectionHeaderMessage.SUBSCRIPTION_TCP_VERSION
        drop_msg.info = (
            f"Couldn't agree on subscription tcp version "
            f"ours: {TcpConnectionHeaderMessage.SUBSCRIPTION_TCP_VERSION} theirs: {reply.version}"
        )

        header = json.dumps(drop_msg.to_json()).encode("utf-8")
        self._tcp_client.send(header)

    def _assert_connection_state(self, connection_status: SubscriptionConnectionServerMessage) -> None:
        if connection_status.type_of_message == SubscriptionConnectionServerMessage.MessageType.ERROR:
            if "DatabaseDoesNotExistException" in connection_status.exception:
                raise DatabaseDoesNotExistException(f"{self._db_name} does not exists. {connection_status.message}")

        if connection_status.type_of_message != SubscriptionConnectionServerMessage.MessageType.CONNECTION_STATUS:
            raise RuntimeError(
                f"Server returned illegal type message when expecting connection status, "
                f"was: {connection_status.type_of_message}"
            )

        if connection_status.status == SubscriptionConnectionServerMessage.ConnectionStatus.ACCEPTED:
            pass
        elif connection_status.status == SubscriptionConnectionServerMessage.ConnectionStatus.IN_USE:
            raise SubscriptionInUseException(
                f"Subscription with id {self._options.subscription_name} cannot be "
                f"opened, because it's in use and the connection strategy "
                f"is {self._options.strategy}"
            )
        elif connection_status.status == SubscriptionConnectionServerMessage.ConnectionStatus.CLOSED:
            can_reconnect = connection_status.data.get("CanReconnect")
            raise SubscriptionClosedException(
                f"Subscription with id {self._options.subscription_name} was closed. " f"{connection_status.exception}",
                can_reconnect=can_reconnect,
            )
        elif connection_status.status == SubscriptionConnectionServerMessage.ConnectionStatus.INVALID:
            raise SubscriptionInvalidStateException(
                f"Subscription with id {self._options.subscription_name} "
                f"cannot be opened, because it is in invalid state. "
                f"{connection_status.exception}"
            )
        elif connection_status.status == SubscriptionConnectionServerMessage.ConnectionStatus.NOT_FOUND:
            raise SubscriptionInvalidStateException(
                f"Subscription with id {self._options.subscription_name} "
                f"cannot be opened, because it does not exist. "
                f"{connection_status.exception}"
            )
        elif connection_status.status == SubscriptionConnectionServerMessage.ConnectionStatus.REDIRECT:
            data = connection_status.data
            redirected_tag = data.get("RedirectedTag")
            appropriate_node = None if redirected_tag is None else redirected_tag
            current_node = data.get("CurrentTag")
            raw_reasons = data.get("Reasons")
            reasons_dictionary = {}

            if isinstance(raw_reasons, list):
                for item in raw_reasons:
                    if isinstance(item, dict):
                        if len(item) == 1:
                            reasons_dictionary.update(item)

            reasons_joined = str(reasons_dictionary)

            not_belong_to_node_exception = SubscriptionDoesNotBelongToNodeException(
                f"Subscription with id '{self._options.subscription_name}' cannot be processed by current node "
                f"'{current_node}', it will be redirected to {appropriate_node}{os.linesep}{reasons_joined}"
            )
            not_belong_to_node_exception.appropriate_node = appropriate_node
            not_belong_to_node_exception.reasons = reasons_dictionary
            raise not_belong_to_node_exception
        elif connection_status.status == SubscriptionConnectionServerMessage.ConnectionStatus.CONCURRENCY_RECONNECT:
            raise SubscriptionChangeVectorUpdateConcurrencyException(connection_status.message)
        else:
            raise RuntimeError(
                f"Subscription {self._options.subscription_name} could not be opened, "
                f"reason: {connection_status.status}"
            )

    def _process_subscription(self) -> None:
        try:
            self._processing_cts.get_token().throw_if_cancellation_requested()

            with self._connect_to_server():
                self._processing_cts.get_token().throw_if_cancellation_requested()

                tcp_client_copy = self._tcp_client

                connection_status = self._read_next_object(tcp_client_copy)
                if self._processing_cts.get_token().is_cancellation_requested():
                    return

                if (
                    connection_status.type_of_message
                    != SubscriptionConnectionServerMessage.MessageType.CONNECTION_STATUS
                    or connection_status.status != SubscriptionConnectionServerMessage.ConnectionStatus.ACCEPTED
                ):
                    self._assert_connection_state(connection_status)

                self._last_connection_failure = None
                if self._processing_cts.get_token().is_cancellation_requested():
                    return

                notified_subscriber = concurrent.futures.Future()
                notified_subscriber.set_result(None)

                batch = SubscriptionBatch(
                    self._revisions,
                    self._subscription_local_request_executor,
                    self._store,
                    self._db_name,
                    self._logger,
                    self._object_type,
                )

                def __run_async():
                    try:
                        self._subscriber(batch)
                    except Exception as ex:
                        self._logger.debug(
                            f"Subscription {self._options.subscription_name}. "
                            f"Subscriber threw an exception on document batch",
                            ex,
                        )

                        if not self._options.ignore_subscriber_errors:
                            raise SubscriberErrorException(
                                f"Subscriber threw an exception in subscription " f"{self._options.subscription_name}",
                                ex,
                            )

                    if tcp_client_copy is not None:
                        self._send_ack(last_received_change_vector, tcp_client_copy)

                while not self._processing_cts.get_token().is_cancellation_requested():
                    # start reading next batch from server on 1'st thread (can be before client started processing)
                    read_from_server = self._store.thread_pool_executor.submit(
                        self._read_single_subscription_batch_from_server, tcp_client_copy, batch
                    )
                    try:
                        notified_subscriber.result()
                    except Exception as e:
                        # if the subscriber errored, we shut down
                        try:
                            self._close_tcp_client()
                        except Exception:
                            # nothing to be done here
                            pass

                        raise e

                    incoming_batch = read_from_server.result(
                        self._options.time_to_wait_before_connection_retry.total_seconds()
                    )

                    self._processing_cts.get_token().throw_if_cancellation_requested()

                    last_received_change_vector = batch.initialize(incoming_batch)

                    notified_subscriber = self._store.thread_pool_executor.submit(__run_async)
        except OperationCancelledException as e:
            if not self._disposed:
                raise e

            # otherwise this is thrown when shutting down,
            # it isn't an error, so we don't need to treat it as such

    def _read_single_subscription_batch_from_server(
        self, sock: socket, batch: SubscriptionBatch[_T]
    ) -> BatchFromServer:
        incoming_batch: List[SubscriptionConnectionServerMessage] = []
        includes: List[Dict] = []
        counter_includes: List[BatchFromServer.CounterIncludeItem] = []
        time_series_includes: List[Dict] = []

        end_of_batch = False
        while not end_of_batch and not self._processing_cts.get_token().is_cancellation_requested():
            received_message = self._read_next_object(sock)
            if received_message is None or self._processing_cts.get_token().is_cancellation_requested():
                break

            if received_message.type_of_message == SubscriptionConnectionServerMessage.MessageType.DATA:
                incoming_batch.append(received_message)
            elif received_message.type_of_message == SubscriptionConnectionServerMessage.MessageType.INCLUDES:
                includes.append(received_message.includes)
            elif received_message.type_of_message == SubscriptionConnectionServerMessage.MessageType.COUNTER_INCLUDES:
                counter_includes.append(
                    BatchFromServer.CounterIncludeItem(
                        received_message.counter_includes, received_message.included_counter_names
                    )
                )
            elif (
                received_message.type_of_message == SubscriptionConnectionServerMessage.MessageType.TIME_SERIES_INCLUDES
            ):
                time_series_includes.append(received_message.time_series_includes)
            elif received_message.type_of_message == SubscriptionConnectionServerMessage.MessageType.END_OF_BATCH:
                end_of_batch = True
            elif received_message.type_of_message == SubscriptionConnectionServerMessage.MessageType.CONFIRM:
                self.invoke_after_acknowledgment(batch)
                incoming_batch.clear()
                batch.items.clear()
            elif received_message.type_of_message == SubscriptionConnectionServerMessage.MessageType.CONNECTION_STATUS:
                self._assert_connection_state(received_message)
            elif received_message.type_of_message == SubscriptionConnectionServerMessage.MessageType.ERROR:
                self._throw_subscription_error(received_message)
            else:
                self._throw_invalid_server_response(received_message)

        batch_from_server = BatchFromServer()
        batch_from_server.messages = incoming_batch
        batch_from_server.includes = includes
        batch_from_server.counter_includes = counter_includes
        batch_from_server.time_series_includes = time_series_includes
        return batch_from_server

    @staticmethod
    def _throw_invalid_server_response(received_message: SubscriptionConnectionServerMessage) -> None:
        raise ValueError(f"Unrecognized message {received_message.type_of_message} type received from server")

    @staticmethod
    def _throw_subscription_error(received_message: SubscriptionConnectionServerMessage) -> None:
        raise RuntimeError(f"Connection terminated by server. Exception: {received_message.exception or None}")

    def _read_next_object(self, sock: socket) -> Optional[SubscriptionConnectionServerMessage]:
        if self._processing_cts.get_token().is_cancellation_requested():  # or  check if tcp client is not connected
            return None

        if self._disposed:  # if we are disposed, nothing to do...
            return None

        def _receive_more_data():
            return sock.recv(self._options.receive_buffer_size).decode("utf-8").strip("\r\n")

        while True:
            if self._buffer and not self._buffer.isspace():
                try:
                    decoded_json, index = self._json_decoder.raw_decode(self._buffer)
                    self._buffer = self._buffer[index:]
                    return SubscriptionConnectionServerMessage.from_json(decoded_json)
                except JSONDecodeError:
                    self._buffer += _receive_more_data()
            else:
                self._buffer += _receive_more_data()

    def _send_ack(self, last_received_change_vector: str, network_stream: socket) -> None:
        msg = SubscriptionConnectionClientMessage()
        msg.change_vector = last_received_change_vector
        msg.type_of_message = SubscriptionConnectionClientMessage.MessageType.ACKNOWLEDGE

        ack = json.dumps(msg.to_json()).encode("utf-8")
        network_stream.send(ack)

    def _run_subscription_async(self) -> Future[None]:
        def __run_async() -> None:
            while not self._processing_cts.get_token().is_cancellation_requested():
                try:
                    self._close_tcp_client()
                    self._logger.info(f"Subscription {self._options.subscription_name}. Connection to server...")
                    self._process_subscription()

                except Exception as ex:
                    if self._processing_cts.get_token().is_cancellation_requested():
                        if not self._disposed:
                            raise ex
                        return

                    self._logger.info(
                        f"Subscription {self._options.subscription_name}. "
                        f"Pulling task threw the following exception",
                        exc_info=ex,
                    )

                    if self._should_try_to_reconnect(ex):
                        time.sleep(self._options.time_to_wait_before_connection_retry.total_seconds())

                        if self._redirect_node is None:
                            req_ex = self._store.get_request_executor(self._db_name)
                            cur_topology = req_ex.topology_nodes
                            self._forced_topology_update_attempts += 1
                            next_node_index = self._forced_topology_update_attempts % len(cur_topology)
                            try:
                                self._redirect_node = cur_topology[next_node_index]
                                self._logger.info(
                                    f"Subscription '{self._options.subscription_name}'. "
                                    f"Will modify redirect node from None to "
                                    f"{self._redirect_node.cluster_tag}",
                                    exc_info=ex,
                                )
                            except Exception as e:
                                # will let topology to decide
                                self._logger.info(
                                    f"Subscription '{self._options.subscription_name}'. "
                                    f"Could not select the redirect node will keep it None.",
                                    exc_info=e,
                                )

                        self.invoke_on_subscription_connection_retry(ex)
                    else:
                        self._logger.error(
                            f"Connection to subscription {self._options.subscription_name} "
                            f"have been shut down because of an error",
                            exc_info=ex,
                        )
                        raise ex

        return self._store.thread_pool_executor.submit(__run_async)

    def _assert_last_connection_failure(self) -> None:
        if self._last_connection_failure is None:
            self._last_connection_failure = datetime.datetime.utcnow()
            return

        if (
            datetime.datetime.utcnow().timestamp() - self._last_connection_failure.timestamp()
            > self._options.max_erroneous_period.total_seconds()
        ):
            raise SubscriptionInvalidStateException(
                f"Subscription connection was in invalid state for more than "
                f"{self._options.max_erroneous_period.total_seconds()} seconds "
                f"and therefore will be terminated."
            )

    def _should_try_to_reconnect(self, ex: Exception) -> bool:
        if isinstance(ex, SubscriptionDoesNotBelongToNodeException):
            ex: SubscriptionDoesNotBelongToNodeException

            request_executor = self._store.get_request_executor(self._db_name)

            if ex.appropriate_node is None:
                self._assert_last_connection_failure()

                self._redirect_node = None
                return True
            proper_nodes = list(filter(lambda x: x.cluster_tag == ex.appropriate_node, request_executor.topology_nodes))
            node_redirect_to = proper_nodes[0] if proper_nodes else None

            if node_redirect_to is None:
                raise RuntimeError(
                    f"Could not redirect to {ex.appropriate_node}, "
                    f"because it was not found in local topology, even after retrying"
                )

            self._redirect_node = node_redirect_to
            return True

        elif isinstance(ex, NodeIsPassiveException):
            # if we failed to talk to a node, we'll forget about it and let the topology to
            # redirect us to the current node
            self._redirect_node = None
            return True
        elif isinstance(ex, SubscriptionChangeVectorUpdateConcurrencyException):
            return True
        elif isinstance(ex, SubscriptionClosedException):
            if ex.can_reconnect:
                return True

            self._processing_cts.cancel()
            return False

        if isinstance(
            ex,
            (
                SubscriptionInUseException,
                SubscriptionDoesNotExistException,
                DatabaseDoesNotExistException,
                AuthorizationException,
                AllTopologyNodesDownException,
                SubscriberErrorException,
            ),
        ):
            self._processing_cts.cancel()
            return False

        self.invoke_on_unexpected_subscription_error(ex)
        self._assert_last_connection_failure()
        return True

    def _close_tcp_client(self) -> None:
        if self._tcp_client is not None:
            self._tcp_client.close()
            self._tcp_client = None


class BatchFromServer:
    def __init__(self):
        self.messages: Optional[List[SubscriptionConnectionServerMessage]] = None
        self.includes: Optional[List[Dict]] = None
        self.counter_includes: Optional[List[BatchFromServer.CounterIncludeItem]] = None
        self.time_series_includes: Optional[List[Dict]] = None

    class CounterIncludeItem:
        def __init__(self, includes: Dict, counter_includes: Dict[str, List[str]]):
            self._includes = includes
            self._counter_includes = counter_includes

        @property
        def includes(self) -> Dict:
            return self._includes

        @property
        def counter_includes(self) -> Dict:
            return self._counter_includes


class SubscriptionBatch(Generic[_T]):
    class Item(Generic[_T_Item]):
        """
        Represents a single item in a subscription batch results.
        This class should be used only inside the subscription's run delegate,
        using it outside this scope might cause unexpected behavior.
        """

        def __init__(self):
            self._result: Optional[_T_Item] = None
            self._exception_message: Optional[str] = None
            self._key: Optional[str] = None
            self._change_vector: Optional[str] = None
            self._projection: Optional[bool] = None
            self._revision: Optional[bool] = None

            self.raw_result: Optional[Dict] = None
            self.raw_metadata: Optional[Dict] = None
            self._metadata: Optional[MetadataAsDictionary] = None

        def _throw_item_process_exception(self):
            raise RuntimeError(
                f"Failed to process document {self._key} with Change Vector {self._change_vector} because: {os.linesep}"
                f"{self._exception_message}"
            )

        @property
        def key(self) -> str:
            return self._key

        @property
        def change_vector(self) -> str:
            return self._change_vector

        @property
        def is_projection(self) -> bool:
            return self._projection

        @property
        def is_revision(self) -> bool:
            return self._revision

        @property
        def result(self) -> _T_Item:
            if self._exception_message is not None:
                self._throw_item_process_exception()
            return self._result

        @result.setter
        def result(self, result: _T_Item):
            self._result = result

        @property
        def metadata(self) -> MetadataAsDictionary:
            if self._metadata is None:
                self._metadata = MetadataAsDictionary(self.raw_metadata)
            return self._metadata

    def __init__(
        self,
        revisions: bool,
        request_executor: RequestExecutor,
        store: DocumentStore,
        db_name: str,
        logger: logging.Logger,
        object_type: Type[_T],
    ):
        self._object_type: Optional[Type[_T]] = object_type
        self._revisions = revisions
        self._request_executor = request_executor
        self._store = store
        self._db_name = db_name
        self._logger = logger

        def __err(entity: object) -> str:
            raise RuntimeError("Shouldn't be generating new ids here")

        self._generate_entity_id_on_the_client: Optional[GenerateEntityIdOnTheClient] = GenerateEntityIdOnTheClient(
            self._request_executor.conventions, __err
        )

        self._items: List[SubscriptionBatch.Item[_T]] = []
        self._includes: Optional[List[Dict]] = None
        self._counter_includes: Optional[List[BatchFromServer.CounterIncludeItem]] = None
        self._time_series_includes: Optional[Dict] = None

    @property
    def items(self) -> List[Item[_T]]:
        return self._items

    @property
    def number_of_items_in_batch(self) -> int:
        return 0 if self.items is None else len(self.items)

    def open_session(self, options: Optional[SessionOptions] = None) -> DocumentSession:
        if not options:
            options = SessionOptions()
        else:
            self._validate_session_options(options)

        options.database = self._db_name
        options.request_executor = self._request_executor
        return self._open_session_internal(options)

    def _open_session_internal(self, options: SessionOptions) -> DocumentSession:
        s = self._store.open_session(session_options=options)
        self._load_data_to_session(s)
        return s

    @staticmethod
    def _validate_session_options(options: SessionOptions) -> None:
        if options.database is not None:
            raise RuntimeError("Cannot set database when session is opened in subscription")

        if options.request_executor is not None:
            raise RuntimeError("Cannot set request_executor when session is opened in subscription")

        if options.transaction_mode != TransactionMode.SINGLE_NODE:
            raise RuntimeError(
                "Cannot set transaction_mode when session is opened in subscription. " "Only SINGLE_NODE is supported."
            )

    def _load_data_to_session(self, s: InMemoryDocumentSessionOperations) -> None:
        if s.no_tracking:
            return

        if self._includes:
            for item in self._includes:
                s.register_includes(item)

        if self._counter_includes:
            for item in self._counter_includes:
                s.register_counters(item.includes, item.counter_includes)

        if self._time_series_includes:
            for item in self._time_series_includes:
                s.register_time_series(item)

        for item in self.items:  # todo: typehints on Item
            if item.is_projection or item.is_revision:
                continue

            document_info = DocumentInfo(
                item.key,
                item.change_vector,
                document=item.raw_result,
                metadata=item.raw_metadata,
                entity=item.result,
                new_document=False,
            )

            s.register_external_loaded_into_the_session(document_info)

    def initialize(self, batch: BatchFromServer) -> str:
        self._includes = batch.includes
        self._counter_includes = batch.counter_includes
        self._time_series_includes = batch.time_series_includes

        self._items.clear()
        last_received_change_vector: Optional[str] = None

        for item in batch.messages:
            cur_doc = item.data

            metadata = cur_doc.get(constants.Documents.Metadata.KEY)
            if metadata is None:
                self.__throw_required("@metadata field")

            key = metadata.get(constants.Documents.Metadata.ID)
            if key is None:
                self.__throw_required("@id field")

            change_vector = metadata.get(constants.Documents.Metadata.CHANGE_VECTOR)
            if change_vector is None:
                self.__throw_required("@change-vector field")

            else:
                last_received_change_vector = change_vector

            projection = metadata.get(constants.Documents.Metadata.PROJECTION) or False
            self._logger.debug(f"Got {key} (change vector: [{last_received_change_vector}], size: {len(cur_doc)})")

            instance: Optional[_T] = None

            if item.exception is None:
                if self._object_type == dict:
                    instance = cur_doc
                else:
                    if self._revisions:
                        # parse outer object manually as Previous/Current has PascalCase
                        previous = cur_doc.get("Previous")
                        current = cur_doc.get("Current")
                        revision = Revision()
                        if current:
                            revision.current = EntityToJson.convert_to_entity_by_key_static(
                                self._object_type, key, current, self._request_executor.conventions
                            )
                        if previous:
                            revision.previous = EntityToJson.convert_to_entity_static(
                                self._object_type, key, previous, self._request_executor.conventions
                            )
                        instance = revision
                    else:
                        instance = EntityToJson.convert_to_entity_by_key_static(
                            self._object_type, key, cur_doc, self._request_executor.conventions
                        )

                if key:
                    self._generate_entity_id_on_the_client.try_set_id_on_entity(instance, key)

            item_to_add = SubscriptionBatch.Item()
            item_to_add._change_vector = change_vector
            item_to_add._key = key
            item_to_add.raw_result = cur_doc
            item_to_add.raw_metadata = metadata
            item_to_add._result = instance
            item_to_add._exception_message = item.exception
            item_to_add._projection = projection
            item_to_add._revision = self._revisions

            self._items.append(item_to_add)

        return last_received_change_vector

    def __throw_required(self, name: str):
        raise RuntimeError(f"Document must have a {name}")
