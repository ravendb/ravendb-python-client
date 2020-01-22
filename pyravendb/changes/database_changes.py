from threading import Lock
from pyravendb.changes.observers import Observable
from pyravendb.subscriptions.data import IncrementalJsonParser
import websocket
from pyravendb.custom_exceptions.exceptions import NotSupportedException
from pyravendb.custom_exceptions.exceptions import ChangeProcessingException
from pyravendb.tools.utils import Utils
import copy
from time import sleep
from concurrent.futures import ThreadPoolExecutor, TimeoutError, Future
import logging
import sys


class DatabaseChanges:
    def __init__(self, request_executor, database_name, on_close, on_error=None, executor=None):
        self._request_executor = request_executor
        self._conventions = request_executor.conventions
        self._database_name = database_name

        self._command_id = 0
        self.client_websocket = websocket.WebSocket()
        self._closed = False
        self._on_close = on_close
        self.on_error = on_error
        self._observables = dict()

        self._executor = executor if executor else ThreadPoolExecutor(max_workers=10)
        self._worker = self._executor.submit(self.do_work)
        self.send_lock = Lock()
        self._confirmations_lock = Lock()
        self._confirmations = {}

        self._command_id = 0
        self._immediate_connection = 0

        self._logger = logging.getLogger("database_changes")
        handler = logging.FileHandler('changes.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)
        self._logger.setLevel(logging.DEBUG)

    def do_work(self):
        preferred_node = self._request_executor.get_preferred_node()
        url = "{0}/databases/{1}/changes".format(preferred_node.url, self._database_name).lower(). \
            replace("http://", "ws://").replace("https://", "wss://").replace(".fiddler", "")

        while not self._closed:
            try:
                if not self.client_websocket.connected:
                    if self._request_executor.certificate:
                        if isinstance(self._request_executor.certificate, tuple):
                            (crt, key) = self._request_executor.certificate
                            self.client_websocket.sock_opt.sslopt.update(
                                {'certfile': crt, 'keyfile': key})
                        else:
                            self.client_websocket.sock_opt.sslopt.update({'ca_certs': self._request_executor.certificate})
                    self.client_websocket.connect(url)
                    for observables in self._observables.values():
                        for observer in observables.values():
                            observer.set(self._executor.submit(observer.on_connect))
                    self._immediate_connection = 1
                self.process_changes()
            except ChangeProcessingException as e:
                self.notify_about_error(e)
                continue
            except websocket.WebSocketException as e:
                if getattr(sys, 'gettrace', None):
                    self._logger.error(e)
                self.notify_about_error(e)
                return
            except Exception as e:
                self._immediate_connection = 0
                self.client_websocket = websocket.WebSocket()
                self.notify_about_error(e)
            sleep(1)

    def process_changes(self):
        parser = IncrementalJsonParser(self.client_websocket, is_websocket=True)
        while not self._closed:
            try:
                response = parser.next_object()
                if response:
                    response_type = response.get("Type", None)
                    if not response_type:
                        continue
                    if response_type == "Error":
                        exception = response["Exception"]
                        self.notify_about_error(Exception(exception))
                    elif response_type == "Confirm":
                        command_id = response.get("CommandId", None)
                        if command_id and command_id in self._confirmations:
                            with self._confirmations_lock:
                                future = self._confirmations.pop(command_id)
                                future.set_result("done complete future")
                    else:
                        value = response.get("Value", None)
                        if value:
                            if response_type not in ("DocumentChange", "IndexChange", "OperationsStatusChange"):
                                raise NotSupportedException(response_type)
                            self._notify_subscribers(value, copy.copy(self._observables[response_type]))
            except Exception as e:
                self.notify_about_error(e)
                raise ChangeProcessingException(e)

    def _notify_subscribers(self, value, observables):
        for observable in observables.values():
            observable.send(value)

    def close(self):
        self._closed = True
        self.client_websocket.close()

        for observable in self._observables.values():
            for observer in observable.values():
                observer.close()

        with self._confirmations_lock:
            for confirmation in self._confirmations.values():
                confirmation.cancel()

        self._observables.clear()
        if self._on_close:
            self._on_close(self._database_name)

        self._executor.shutdown(wait=True)

    def notify_about_error(self, e):
        if self.on_error:
            self.on_error(e)

        for observables in self._observables.values():
            for observer in observables.values():
                observer.error(e)

        for _, observable in self._observables.items():
            observable.error(e)

    def for_all_documents(self):
        observable = self.get_or_add_observable("DocumentChange", "all-docs", "watch-docs", "unwatch-docs", None)(
            lambda x: True)
        return observable

    def for_all_operations(self):
        observable = self.get_or_add_observable("OperationsStatusChange", "all-operations", "watch-operations",
                                                "unwatch-operations", None)(lambda x: True)
        return observable

    def for_all_indexes(self):
        observable = self.get_or_add_observable("IndexChange", "all-indexes", "watch-indexes", "unwatch-indexes",
                                                None)(lambda x: True)
        return observable

    def for_index(self, index_name):
        observable = self.get_or_add_observable("IndexChange", "indexes/" + index_name, "watch-index", "unwatch-index",
                                                index_name)(lambda x: x["Name"].casefold() == index_name.casefold())
        return observable

    def for_operation_id(self, operation_id):
        observable = self.get_or_add_observable("OperationsStatusChange", "operations/" + str(operation_id),
                                                "watch-operation", "unwatch-operation", str(operation_id))(
            lambda x: x["operationId"] == str(operation_id))
        return observable

    def for_document(self, doc_id):
        observable = self.get_or_add_observable("DocumentChange", "docs/" + doc_id, "watch-doc", "unwatch-doc", doc_id)(
            lambda x: x["Id"].casefold() == doc_id.casefold())
        return observable

    def for_documents_start_with(self, doc_id_prefix):
        observable = self.get_or_add_observable("DocumentChange", "prefixes/" + doc_id_prefix, "watch-prefix",
                                                "unwatch-prefix", doc_id_prefix)(
            lambda x: x['Id'] is not None and x["Id"].casefold().startswith(doc_id_prefix.casefold()))
        return observable

    def for_documents_in_collection(self, collection_name):
        observable = self.get_or_add_observable("DocumentChange", "collections/" + collection_name, "watch-collection",
                                                "unwatch-collection", collection_name)(
            lambda x: x["CollectionName"].casefold().starts_with(collection_name.casefold()))
        return observable

    def get_or_add_observable(self, group, name, watch_command, unwatch_command, value):
        if group not in self._observables:
            self._observables[group] = {}

        if name not in self._observables[group]:
            def on_disconnect():
                try:
                    if self.client_websocket.connected:
                        self.send(unwatch_command, value)
                except websocket.WebSocketException:
                    pass

            def on_connect():
                self.send(watch_command, value)

            observable = Observable(on_connect=on_connect, on_disconnect=on_disconnect, executor=self._executor)
            self._observables[group][name] = observable
            if self._immediate_connection != 0:
                observable.set(self._executor.submit(observable.on_connect))
        return self._observables[group][name]

    def send(self, command, value):
        current_command_id = 0
        future = Future()
        try:
            with self.send_lock:
                self._command_id += 1
                current_command_id += self._command_id
                data = Utils.dict_to_bytes({"CommandId": current_command_id, "Command": command, "Param": value})
                with self._confirmations_lock:
                    self._confirmations[current_command_id] = future
                self.client_websocket.send(data)

                try:
                    future.result(timeout=15)
                except TimeoutError:
                    future.cancel()
                    raise TimeoutError("Did not get a confirmation for command #" + current_command_id)
                except Exception as e:
                    if getattr(sys, 'gettrace', None):
                        self._logger.info('The coroutine raised an exception: {!r}'.format(e))
        except websocket.WebSocketConnectionClosedException:
            pass
