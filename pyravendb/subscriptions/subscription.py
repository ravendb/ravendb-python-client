import logging
import socket
import ssl
import sys
from datetime import datetime
from pyravendb.tools.utils import Utils
from pyravendb.connection.requests_helpers import PropagatingThread
from urllib.parse import urlparse
from pyravendb.commands.raven_commands import GetTcpInfoCommand
from pyravendb.subscriptions.data import IncrementalJsonParser
from pyravendb.custom_exceptions.exceptions import *
from pyravendb.connection.requests_executor import RequestsExecutor
from pyravendb.custom_exceptions.exceptions import AggregateException
import ijson


class SubscriptionWorker:
    def __init__(self, options, store, database_name, confirm_callback=None, on_subscription_connection_retry=None,
                 object_type=None, nested_object_types=None):
        """
        @param SubscriptionWorkerOptions options: The subscription connection options.
        @param DocumentStore store: The document store
        @param str database_name: The database name
        @param func(SubscriptionBatch) confirm_callback: Allows the user to define stuff that happens after the confirm was received from the server
        @param func(Exception) on_subscription_connection_retry: Allows the user to define what will happen after connection retry
        @param Type object_type: The type of the object we want to track the entity to
        @param Dict[str, Type] nested_object_types: A dict of classes for nested object the key will be the name of the
        class and the value will be the object we want to get for that attribute
        """
        self._options = options
        self._store = store
        self._database_name = database_name if database_name is not None else store.database
        self._logger = logging.getLogger("subscription_" + self._database_name)
        handler = logging.FileHandler('subscriptions.log')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)
        self._logger.setLevel(logging.DEBUG)

        if options.subscription_name is None:
            raise AttributeError("SubscriptionWorkerOptions must specify the subscription_name", "options")

        self._deleted = False
        self._process_documents = None
        self._my_socket = None
        self.request_executor = None
        self._subscription_thread = None
        self._closed = False
        self.object_type = object_type
        self.nested_object_types = nested_object_types
        self._batch = None
        self.confirm_callback = confirm_callback
        self._redirect_node = None

        self.on_subscription_connection_retry = on_subscription_connection_retry
        self._last_connection_failure = None

    def connect_to_server(self):
        command = GetTcpInfoCommand("Subscription/" + self._database_name, self._database_name)
        request_executor = self._store.get_request_executor(self._database_name)

        send_buffer_size = 32 * 1024
        receive_buffer_size = 4096

        if self._redirect_node:
            try:
                result = request_executor.execute_with_node(self._redirect_node, command, should_retry=False)
            except Exception:
                # if we failed to talk to a node, we'll forget about it and let the topology to
                # redirect us to the current nod
                self._redirect_node = None
                raise
        else:
            result = request_executor.execute(command)

        uri = urlparse(result["Url"])
        host = uri.hostname
        port = uri.port

        self._my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._my_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, send_buffer_size)
        self._my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer_size)
        self._my_socket.settimeout(30)

        if self._store.certificate:
            try:
                store_cert = self._store.certificate
                (cert, key) = store_cert if isinstance(store_cert, tuple) else (store_cert, None)

                self._my_socket = ssl.wrap_socket(self._my_socket, certfile=cert, keyfile=key,
                                                  ssl_version=ssl.PROTOCOL_TLSv1_2)
            except Exception as e:
                raise NonRecoverableSubscriptionException("Failed to create SSL wrapper") from e

        self._my_socket.connect((host, port))

        header = Utils.dict_to_bytes(
            {"Operation": "Subscription", "DatabaseName": self._database_name, "OperationVersion": 40})
        self._my_socket.sendall(header)

        parser = IncrementalJsonParser(self._my_socket)
        response = parser.next_object()

        if response['Status'] != "Ok":
            if response['Status'] == "AuthorizationFailed":
                raise ConnectionRefusedError(
                    "Cannot access database {0} because {1}".format(self._database_name, response['Message']))
            elif response['Status'] == "TcpVersionMismatch":
                raise InvalidOperationException(
                    "Cannot access database {0} because {1}".format(self._database_name, response['Message']))

        options = Utils.dict_to_bytes(self._options.to_json())
        self._my_socket.sendall(options)

        if self.request_executor:
            self.request_executor.close()
        self.request_executor = RequestsExecutor.create_for_single_node(command.requested_node.url,
                                                                        self._database_name,
                                                                        self._store.certificate)

        return parser

    def read_single_batch_from_server(self, parser):
        while True and not self._closed:
            item = parser.next_object()
            if item is None:
                break
            if item['Type'] == 'Data':
                self._batch.raw_items.append(item)
                continue
            if item['Type'] == 'EndOfBatch':
                return
            if item['Type'] == 'Confirm':
                if self.confirm_callback:
                    self.confirm_callback(self._batch)
                self._batch.clear()
                continue

            if item['Type'] == 'ConnectionStatus':
                self.assert_connection_state(item)
            if item['Type'] == 'Error':
                err = item.get('Exception', 'None')
                raise InvalidOperationException("Connection terminated by server Exception: " + err)

            raise InvalidOperationException("Unrecognized message from server: " + item['Type'])

    def run_subscription(self):
        import time
        while not self._closed:
            try:
                self.process_subscription()
            except Exception as ex:
                try:
                    self._logger.warning("Subscription '{0}'. Pulling task threw the following exception : {1}".format(
                        self._options.subscription_name, ex))
                    if self.should_try_to_reconnect(ex):
                        time.sleep(self._options.time_to_wait_before_connection_retry.seconds)
                        if self.on_subscription_connection_retry:
                            self.on_subscription_connection_retry(ex)
                    else:
                        self._logger.warning(
                            "Connection to subscription '{0}'. have been shut down because of an error : {1}".format(
                                self._options.subscription_name, ex))
                        raise
                except Exception as e:
                    if e == ex:
                        raise
                    raise AggregateException(e, ex)

    def process_subscription(self):
        try:
            parser = self.connect_to_server()
            if self._closed:
                return
            connection_status = parser.next_object()
            if connection_status['Type'] != "ConnectionStatus" or connection_status['Status'] != "Accepted":
                self.assert_connection_state(connection_status)

            self._batch = SubscriptionBatch(self.request_executor, self._store, self._database_name, self._logger,
                                            self.object_type, self.nested_object_types)
            while not self._closed:
                self.read_single_batch_from_server(parser)
                last_change_vector = self._batch.initialize()
                try:
                    self._process_documents(self._batch)
                except Exception as ex:
                    self._logger.warning(
                        "Subscription '{0}'. Subscriber threw an exception on document batch : {1}".format(
                            self._options.subscription_name, str(ex)))
                    if not self._options.ignore_subscriber_errors:
                        raise SubscriberErrorException(
                            "Subscriber threw an exception in subscription" + self._options.subscription_name, ex)

                header = Utils.dict_to_bytes(
                    {"ChangeVector": last_change_vector, "Type": "Acknowledge"})
                self._my_socket.sendall(header)
        except (InvalidOperationException, OSError) as e:
            "This is raise when shutting down, it isn't an error, so we don't need to treat it as such"
        finally:
            if self._my_socket:
                self._my_socket.close()

    def should_try_to_reconnect(self, exception):
        if isinstance(exception, SubscriptionDoesNotBelongToNodeException):
            self.assert_last_connection_failure()
            request_executor = self._store.get_request_executor(self._database_name)
            if not exception.appropriate_node:
                return True

            node_to_redirect_to = Utils.first_or_default(request_executor.topology_nodes,
                                                         lambda node: node.cluster_tag == exception.appropriate_node,
                                                         None)

            if not node_to_redirect_to:
                raise AggregateException(exception, InvalidOperationException(
                    "Could not redirect to {0}, "
                    "because it was not found in local topology, even after retrying".format(
                        exception.appropriate_node)))

            self._redirect_node = node_to_redirect_to
            return True
        if isinstance(exception, SubscriptionChangeVectorUpdateConcurrencyException):
            return True
        if isinstance(exception, (
                SubscriptionInUseException, SubscriptionDoesNotExistException, SubscriptionClosedException,
                NonRecoverableSubscriptionException, SubscriptionInvalidStateException, DatabaseDoesNotExistException,
                AuthorizationException, AllTopologyNodesDownException, SubscriberErrorException,
                ValueError, NotImplementedError, AttributeError, ijson.backend.UnexpectedSymbol)):
            self.close()
            return False
        self.assert_last_connection_failure()
        return True

    def assert_last_connection_failure(self):
        if not self._last_connection_failure:
            self._last_connection_failure = datetime.now()
        elif (datetime.now() - self._last_connection_failure) > self._options.max_erroneous_period:
            raise SubscriptionInvalidStateException(
                "Subscription connection was in invalid state for more than {0} and therefore will be terminated".format(
                    self._options.max_erroneous_period))

    def assert_connection_state(self, connection_status):
        if connection_status['Type'] == "Error":
            if "DatabaseDoesNotExistException" in connection_status['Exception']:
                raise "Database '{0}' does not exist.".format(self._database_name)
        if connection_status['Type'] != "ConnectionStatus":
            raise Exception(
                "Server returned illegal type message when expecting connection status, was: " + connection_status[
                    'Type'])

        status = connection_status['Status']
        if status != "Accepted":
            if status == "InUse":
                raise SubscriptionInUseException(
                    "Subscription With Id '{0}' cannot be opened, "
                    "because it's in use and the connection strategy is {1}".format(
                        self._options.subscription_name, self._options.strategy))
            if status == "Closed":
                raise SubscriptionClosedException(
                    "Subscription With Id '{0}' cannot be opened, because it is in invalid state. {1}".format(
                        self._options.subscription_name, connection_status["Exception"]))
            if status == "Invalid":
                raise SubscriptionInvalidStateException(
                    "Subscription With Id '{0}' cannot be opened, because it is in invalid state. {1}".format(
                        self._options.subscription_name, connection_status["Exception"]))
            if status == "NotFound":
                raise SubscriptionDoesNotExistException(
                    "Subscription With Id '{0}' cannot be opened, because it does not exist. {1}".format(
                        self._options.subscription_name, connection_status["Exception"]))
            if status == "Redirect":
                if "Data" in connection_status:
                    if "RedirectedTag" in connection_status["Data"]:
                        appropriate_node = connection_status["Data"]["RedirectedTag"]
                        raise SubscriptionDoesNotBelongToNodeException(
                            appropriate_node,
                            "Subscription With Id '{0}' cannot be processed by current node, it will be redirected to {1}".format(
                                self._options.subscription_name,
                                appropriate_node if appropriate_node is not None else "None"))
            if status == "ConcurrencyReconnect":
                raise SubscriptionChangeVectorUpdateConcurrencyException(connection_status["Exception"])
            raise AttributeError(
                "Subscription '{0}' could not be opened, reason: {1}".format(
                    self._options.subscription_name, status))

    def run(self, process_documents):
        if self._subscription_thread:
            raise SubscriptionInUseException("Subscription already in use")
        self._process_documents = process_documents
        self._subscription_thread = PropagatingThread(target=self.run_subscription)
        self._subscription_thread.start()
        return self._subscription_thread

    def close(self):
        try:
            if self._closed or not self._subscription_thread:
                return

            self._closed = True
            if self._my_socket:
                self._my_socket.close()
            self._subscription_thread.join()
        except Exception as ex:
            self._logger.debug("Error during closing the subscription: " + str(ex))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class _SubscriptionBatchItem:
    def __init__(self, change_vector, document_id, raw_result, raw_metadata, result, exception_message):
        self.change_vector = change_vector
        self.document_id = document_id
        self.raw_result = raw_result
        self.raw_metadata = raw_metadata
        self.result = result
        self.exception_message = exception_message


class SubscriptionBatch:
    def __init__(self, request_executor, store, database_name, logger, object_type, nested_object_type):
        self._request_executor = request_executor
        self._store = store
        self._database_name = database_name
        self._logger = logger
        self._object_type = object_type
        self._nested_object_type = nested_object_type
        self.raw_items = []
        self.items = []  # List[_SubscriptionBatchItem]

    def open_session(self):
        return self._store.open_session(self._database_name, self._request_executor)

    def clear(self):
        self.raw_items.clear()
        self.items.clear()

    def initialize(self):
        last_change_vector = None
        for item in self.raw_items:

            entity, metadata, _, _ = Utils.convert_to_entity(item['Data'], self._object_type,
                                                             self._store.conventions,
                                                             self._store.events,
                                                             nested_object_types=self._nested_object_type)
            if not metadata:
                raise InvalidOperationException("Document must have a @metadata field")
            document_id = metadata.get('@id', None)
            if not id:
                raise InvalidOperationException("Document must have a @id field")
            last_change_vector = metadata.get('@change-vector', None)
            if not last_change_vector:
                raise InvalidOperationException("Document must have a @change-vector field")
            self._logger.info(
                "Got {0} (change vector: [{1}], size {2}".format(document_id, last_change_vector,
                                                                 len(item["Data"])))

            self.items.append(
                _SubscriptionBatchItem(change_vector=last_change_vector, document_id=document_id,
                                       raw_result=item["Data"], raw_metadata=metadata, result=entity,
                                       exception_message=item.get("Exception", None)))
        return last_change_vector
