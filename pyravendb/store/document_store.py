from pyravendb.connection.requests_executor import RequestsExecutor
from pyravendb.data.operations import Operations
from pyravendb.custom_exceptions import exceptions
from pyravendb.data.document_convention import DocumentConvention
from pyravendb.hilo.hilo_generator import MultiDatabaseHiLoKeyGenerator
from pyravendb.store.document_session import DocumentSession
import uuid
import os


class DocumentStore(object):
    def __init__(self, url=None, database=None, api_key=None):
        self.url = url
        self.database = database
        self.conventions = DocumentConvention()
        self.api_key = api_key
        self._request_executors = {}
        self._initialize = False
        self.generator = None
        self._operations = None
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
            self._request_executors[db_name] = RequestsExecutor(self.url, db_name, self.api_key, self.conventions)
        return self._request_executors[db_name]

    def initialize(self):
        if not self._initialize:
            if self.database is None:
                raise exceptions.InvalidOperationException("None database is not valid")

            self._operations = Operations(self.get_request_executor())
            self.generator = MultiDatabaseHiLoKeyGenerator(self)
            self._initialize = True

    def _assert_initialize(self):
        if not self._initialize:
            raise exceptions.InvalidOperationException(
                "You cannot open a session or access the database commands before initializing the document store.\
                Did you forget calling initialize()?")

    def open_session(self, database=None, force_read_from_master=False):
        self._assert_initialize()
        session_id = uuid.uuid4()
        requests_executor = self.get_request_executor(database)
        return DocumentSession(database, self, requests_executor, session_id, force_read_from_master)

    def generate_id(self, db_name, entity):
        if self.generator:
            return self.generator.generate_document_key(db_name, entity)
