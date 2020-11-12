from pyravendb.connection.requests_executor import RequestsExecutor
from pyravendb.custom_exceptions import exceptions
from pyravendb.data.document_conventions import DocumentConventions
from pyravendb.hilo.hilo_generator import MultiDatabaseHiLoKeyGenerator
from pyravendb.store.document_session import DocumentSession
from pyravendb.changes.database_changes import DatabaseChanges
from pyravendb.subscriptions.document_subscriptions import DocumentSubscriptions
from threading import Lock
import uuid
import time


class DocumentStore(object):
    def __init__(self, urls=None, database=None, certificate=None):
        """
        :type urls: List[str]
        :type database: str
        :type certificate: Any
                EITHER  object - {"pfx": "/path/to/cert.pfx", "password": "optional password"}
                OR      tuple  - ("/path/to/cert.pem")
                OR      tuple  - ("/path/to/cert.crt", "/path/to/cert.key")
        """
        if not isinstance(urls, list):
            urls = [urls]
        self.urls = urls
        self.database = database
        self._conventions = DocumentConventions()
        self._certificate = certificate
        self._request_executors = {}
        self._initialize = False
        self.generator = None
        self._operations_executor = None
        self.lock = Lock()
        self.add_change_lock = Lock()
        self._maintenance_operation_executor = None
        self.subscriptions = DocumentSubscriptions(self)
        self._database_changes = {}
        self._events = DocumentEvents()

    @property
    def events(self):
        return self._events

    @property
    def conventions(self):
        return self._conventions

    @conventions.setter
    def conventions(self, value):
        if self._conventions.is_frozen():
            raise exceptions.InvalidOperationException(
                "Conventions has frozen after store.initialize() and no changes can be applied to them")
        self._conventions = value

    @property
    def certificate(self):
        return self._certificate

    @property
    def maintenance(self):
        self._assert_initialize()
        if not self._maintenance_operation_executor:
            self._maintenance_operation_executor = MaintenanceOperationExecutor(self, self.database)
        return self._maintenance_operation_executor

    @property
    def operations(self):
        self._assert_initialize()
        if not self._operations_executor:
            self._operations_executor = OperationExecutor(self)
        return self._operations_executor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self.generator is not None:
            self.generator.return_unused_range()

        if self.subscriptions:
            self.subscriptions.close()

        database_changes_keys = list(self._database_changes.keys())
        for key in database_changes_keys:
            self._database_changes[key].close()

        for _, request_executor in self._request_executors.items():
            request_executor.close()

    def changes(self, database=None, on_error=None, executor=None):
        self._assert_initialize()
        if not database:
            database = self.database
        with self.add_change_lock:
            if database not in self._database_changes:
                self._database_changes[database] = DatabaseChanges(request_executor=self.get_request_executor(database),
                                                                   database_name=database,
                                                                   on_close=self._on_close_change, on_error=on_error,
                                                                   executor=executor)
            return self._database_changes[database]

    def _on_close_change(self, database):
        del self._database_changes[database]

    def get_request_executor(self, db_name=None):
        self._assert_initialize()

        db_name = db_name if db_name is not None else self.database
        if db_name is None:
            raise ValueError("database name cannot be None")

        with self.lock:
            if self._request_executors.get(db_name) is None:
                self._request_executors.setdefault(db_name,
                                                   RequestsExecutor.create(self.urls, db_name, self._certificate,
                                                                           self.conventions))
            return self._request_executors[db_name]

    def initialize(self):
        if not self._initialize:
            if self.urls is None or len(self.urls) == 0:
                raise ValueError("Document store URLs cannot be empty", "urls")

            self.generator = MultiDatabaseHiLoKeyGenerator(self)
            self.conventions.freeze()
            self._initialize = True

    def _assert_initialize(self):
        if not self._initialize:
            raise exceptions.InvalidOperationException(
                "You cannot open a session or access the database commands before initializing the document store.\
                Did you forget calling initialize()?")

    def open_session(self, database=None, request_executor=None):
        self._assert_initialize()
        session_id = uuid.uuid4()
        requests_executor = request_executor if request_executor is not None else self.get_request_executor(database)
        session = DocumentSession(database, self, requests_executor, session_id)
        self.events.session_created(session)
        return session

    def generate_id(self, db_name, entity):
        if self.generator:
            return self.generator.generate_document_key(db_name, entity)


class DocumentEvents:
    def __init__(self):
        self.before_store = lambda session, doc_id, entity: None
        self.before_query = lambda session, query: None
        self.after_save_change = lambda session, doc_id, entity: None
        self.before_delete = lambda session, doc_id, entity: None
        self.session_created = lambda session: None
        self.before_conversion_to_entity = lambda document, metadata, type_from_metadata: None
        self.after_conversion_to_entity = lambda entity, document, metadata: None

    def clone(self):
        other = DocumentEvents()
        other.__dict__ = self.__dict__.copy()
        return other


# ------------------------------Operation executors ---------------------------->
class MaintenanceOperationExecutor:
    def __init__(self, document_store, database_name=None):
        self._store = document_store
        self._database_name = database_name if database_name is not None else document_store.database
        self._server_operation_executor = None
        self._request_executor = None

    @property
    def server(self):
        if not self._server_operation_executor:
            self._server_operation_executor = ServerOperationExecutor(self._store)
        return self._server_operation_executor

    @property
    def request_executor(self):
        if self._request_executor is None:
            self._request_executor = self._store.get_request_executor(self._database_name)
        return self._request_executor

    def send(self, operation):
        try:
            operation_type = getattr(operation, 'operation')
            if operation_type != "MaintenanceOperation":
                raise ValueError("operation type cannot be {0} need to be Operation".format(operation_type))
        except AttributeError:
            raise ValueError("Invalid operation")

        command = operation.get_command(self.request_executor.conventions)
        return self.request_executor.execute(command)


class ServerOperationExecutor:
    def __init__(self, document_store):
        self._store = document_store
        self._request_executor = None

    @property
    def request_executor(self):
        if self._request_executor is None:
            from pyravendb.connection.cluster_requests_executor import ClusterRequestExecutor
            if self._store.conventions.disable_topology_update:
                self._request_executor = ClusterRequestExecutor.create_for_single_node(self._store.urls[0],
                                                                                       self._store.certificate)
            else:
                self._request_executor = ClusterRequestExecutor.create(self._store.urls, self._store.certificate)
        return self._request_executor

    def send(self, operation):
        try:
            operation_type = getattr(operation, 'operation')
            if operation_type != "ServerOperation":
                raise ValueError("operation type cannot be {0} need to be Operation".format(operation_type))
        except AttributeError:
            raise ValueError("Invalid operation")

        command = operation.get_command(self.request_executor.conventions)
        return self.request_executor.execute(command)


class OperationExecutor(object):
    def __init__(self, document_store, database_name=None):
        self._store = document_store
        self._database_name = database_name if database_name is not None else document_store.database
        self._request_executor = None

    @property
    def request_executor(self):
        if self._request_executor is None:
            self._request_executor = self._store.get_request_executor(self._database_name)
        return self._request_executor

    def wait_for_operation_complete(self, operation_id, timeout=None):
        from pyravendb.commands.raven_commands import GetOperationStateCommand
        start_time = time.time()
        try:
            get_operation_command = GetOperationStateCommand(operation_id)
            while True:
                response = self.request_executor.execute(get_operation_command)
                if "Error" in response:
                    raise ValueError(response["Error"])
                if timeout and time.time() - start_time > timeout:
                    raise exceptions.TimeoutException("The Operation did not finish before the timeout end")
                if response["Status"] == "Completed":
                    return response
                if response["Status"] == "Faulted":
                    raise exceptions.InvalidOperationException(response["Result"]["Message"])
                time.sleep(0.5)
        except ValueError as e:
            raise exceptions.InvalidOperationException(e)

    def send(self, operation):
        try:
            operation_type = getattr(operation, 'operation')
            if operation_type != "Operation":
                raise ValueError("operation type cannot be {0} need to be Operation".format(operation_type))
        except AttributeError:
            raise ValueError("Invalid operation")

        command = operation.get_command(self._store, self.request_executor.conventions)
        return self.request_executor.execute(command)
