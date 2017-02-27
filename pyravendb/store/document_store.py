from pyravendb.connection.requests_executor import RequestsExecutor
from pyravendb.custom_exceptions import exceptions
from pyravendb.connection.server_node import ServerNode
from pyravendb.data.document_convention import DocumentConvention
from pyravendb.hilo.hilo_generator import MultiDatabaseHiLoKeyGenerator
from pyravendb.store.document_session import DocumentSession
import uuid


class DocumentStore(object):
    def __init__(self, url=None, database=None, api_key=None):
        self.url = url
        self.database = database
        self.conventions = DocumentConvention()
        self.api_key = api_key
        self._requests_executor = RequestsExecutor(ServerNode(url, database), self.conventions)
        self._initialize = False
        self.generator = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.generator is not None:
            self.generator.return_unused_range()

    def get_request_executor(self):
        return self._requests_executor

    def initialize(self):
        if not self._initialize:
            if self.database is None:
                raise exceptions.InvalidOperationException("None database is not valid")

            # self._requests_handler.get_replication_topology()
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
        requests_executor = self.get_request_executor()
        if database is not None:
            requests_executor = RequestsExecutor(self.url, database, self.conventions, force_get_topology=True)
        return DocumentSession(database, self, requests_executor, session_id, force_read_from_master)

    def generate_id(self, db_name, entity):
        if self.generator:
            return self.generator.generate_document_key(db_name, entity)
