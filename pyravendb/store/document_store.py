from pyravendb.connection.requests_executor import RequestsExecutor
from pyravendb.custom_exceptions import exceptions
from pyravendb.data.document_convention import DocumentConvention
from pyravendb.hilo.hilo_generator import MultiDatabaseHiLoKeyGenerator
from pyravendb.store.document_session import DocumentSession
import uuid
import time


class DocumentStore(object):
    def __init__(self, urls=None, database=None, certificate=None):
        if not isinstance(urls, list):
            urls = [urls]
        self.urls = urls
        self.database = database
        self.conventions = DocumentConvention()
        self.certificate = certificate
        self._request_executor = {}
        self._initialize = False
        self.generator = None
        self._operations = None
        self.admin = AdminOperationExecutor(self, database)

    @property
    def operations(self):
        self._assert_initialize()
        return self._operations

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.generator is not None:
            self.generator.return_unused_range()

    def get_request_executor(self, db_name=None):
        if db_name is None:
            db_name = self.database

        if db_name not in self._request_executor:
            self._request_executor[db_name] = RequestsExecutor.create(self.urls, db_name, self.certificate,
                                                                      self.conventions)
        return self._request_executor[db_name]

    def initialize(self):
        if not self._initialize:
            if self.database is None:
                raise exceptions.InvalidOperationException("None database is not valid")

            self._operations = OperationExecutor(self)
            self.generator = MultiDatabaseHiLoKeyGenerator(self)
            self._initialize = True

    def _assert_initialize(self):
        if not self._initialize:
            raise exceptions.InvalidOperationException(
                "You cannot open a session or access the database commands before initializing the document store.\
                Did you forget calling initialize()?")

    def open_session(self, database=None):
        self._assert_initialize()
        session_id = uuid.uuid4()
        requests_executor = self.get_request_executor(database)
        return DocumentSession(database, self, requests_executor, session_id)

    def generate_id(self, db_name, entity):
        if self.generator:
            return self.generator.generate_document_key(db_name, entity)


# ------------------------------Operation executors ---------------------------->
class AdminOperationExecutor:
    def __init__(self, document_store, database_name=None):
        self._store = document_store
        self._database_name = database_name if database_name is not None else document_store.database
        self.server = ServerOperationExecutor(self._store)
        self._request_executor = None

    @property
    def request_executor(self):
        if self._request_executor is None:
            self._request_executor = self._store.get_request_executor(self._database_name)
        return self._request_executor

    def send(self, operation):
        try:
            operation_type = getattr(operation, 'operation')
            if operation_type != "AdminOperation":
                raise ValueError("operation type cannot be {0} need to be Operation".format(operation_type))
        except AttributeError:
            raise ValueError("Invalid operation")

        command = operation.get_command(self.request_executor.convention)
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

        command = operation.get_command(self.request_executor.convention)
        return self.request_executor.execute(command)


class OperationExecutor(object):
    def __init__(self, document_store, database_name=None):
        self._document_store = document_store
        self._database_name = database_name
        self._request_executor = document_store.get_request_executor(db_name=database_name)

    def wait_for_operation_complete(self, operation_id, timeout=None):
        from pyravendb.commands.raven_commands import GetOperationStateCommand
        start_time = time.time()
        try:
            get_operation_command = GetOperationStateCommand(operation_id)
            while True:
                response = self.request_executor.execute(get_operation_command)
                if timeout and time.time() - start_time > timeout:
                    raise exceptions.TimeoutException("The Operation did not finish before the timeout end")
                if response["Status"] == "Completed":
                    return response
                if response["Status"] == "Faulted":
                    raise exceptions.InvalidOperationException(response["Result"]["Error"])
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

        command = operation.get_command(self._document_store, self._request_executor.convention)
        return self._request_executor.execute(command)
