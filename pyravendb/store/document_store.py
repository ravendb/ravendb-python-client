from pyravendb.connection.requests_executor import RequestsExecutor
from pyravendb.connection.operation_executor import OperationExecutor
from pyravendb.connection.server_operation_executor import ServerOperationExecutor
from pyravendb.custom_exceptions import exceptions
from pyravendb.data.document_convention import DocumentConvention
from pyravendb.hilo.hilo_generator import MultiDatabaseHiLoKeyGenerator
from pyravendb.store.document_session import DocumentSession
import uuid
import os


class DocumentStore(object):
    def __init__(self, urls=None, database=None, certificate=None):
        if not isinstance(urls, list):
            urls = [urls]
        self.urls = urls
        self.database = database
        self.conventions = DocumentConvention()
        self.certificate = certificate
        self._request_executors = {}
        self._initialize = False
        self.generator = None
        self._operations = None
        self.admin = _Admin(self, database)
        print(os.getcwd())

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

        if db_name not in self._request_executors:
            self._request_executors[db_name] = RequestsExecutor.create(self.urls, db_name, self.certificate,
                                                                       self.conventions)
        return self._request_executors[db_name]

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


class _Admin:
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

        command = operation.get_command(self._request_executor.convention)
        return self.request_executor.execute(command)
