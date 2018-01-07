from pyravendb.connection.requests_executor import RequestsExecutor
from pyravendb.connection.requests_helpers import PropagatingThread
from pyravendb.custom_exceptions.exceptions import ChangeProcessingException
import ssl
import socket
import abc


class Observer(metaclass=abc.ABCMeta):
    def __init__(self, on_connect, on_disconnect):
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self._value = 0

    @abc.abstractmethod
    def update(self, arg):
        pass


class Subscriber(Observer):
    def update(self, arg):
        pass


class DatabaseChanges:
    def __init__(self, request_executor, database_name, on_close, on_error=None):
        self._request_executor = request_executor
        self._conventions = request_executor.conventions
        self._database_name = database_name

        self._my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._my_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # TODO: wrap SSL
        if self._store.certificate:
            self._my_socket = ssl.wrap_socket(self._my_socket, certfile=self._request_executor.certificate,
                                              ssl_version=ssl.PROTOCOL_TLSv1_2)
        self._closed = False
        self.connection_status_changed = []
        self._on_close = on_close
        self.on_error = on_error
        self._subscribers = dict()
        self._worker = PropagatingThread(target=self.do_work(), daemon=True)
        self._worker.start()

    def do_work(self):
        try:
            self._request_executor.get_preferred_node()
        except ChangeProcessingException as e:
            return "sds"

    def close(self):
        self._closed = True

    def get_or_add_subscriber(self, name, watch_command, unwatch_command, value):
        if name in self._subscribers:
            return self._subscribers[name]

        subscriber = Subscriber()
