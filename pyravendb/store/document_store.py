from pyravendb.connection.requests_handler import HttpRequestsHandler
from pyravendb.custom_exceptions import exceptions
from pyravendb.d_commands import raven_commands
from pyravendb.connection.server_node import ServerNode
from pyravendb.data.document_convention import DocumentConvention
from pyravendb.hilo.hilo_generator import HiloGenerator
from pyravendb.store.document_session import DocumentSession
from pyravendb.tools.utils import Utils
import uuid


class DocumentStore(object):
    def __init__(self, url=None, database=None, api_key=None):
        self.url = url
        self.database = database
        self.conventions = DocumentConvention()
        self.api_key = api_key
        self._requests_handler = HttpRequestsHandler(ServerNode(url, database), self.conventions)
        self._initialize = False
        self.generator = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def get_request_handler(self):
        return self._requests_handler

    def initialize(self):
        if not self._initialize:
            if self.database is None:
                raise exceptions.InvalidOperationException("None database is not valid")

            # self._requests_handler.get_replication_topology()
            # self.generator = HiloGenerator(self.conventions.max_ids_to_catch, self._database_commands)
            self._initialize = True

    def _assert_initialize(self):
        if not self._initialize:
            raise exceptions.InvalidOperationException(
                "You cannot open a session or access the database commands before initializing the document store.\
                Did you forget calling initialize()?")

    def open_session(self, database=None, api_key=None, force_read_from_master=False):
        self._assert_initialize()
        session_id = uuid.uuid4()
        requests_handler = self._requests_handler
        if database is not None:
            requests_handler = HttpRequestsHandler(self.url, database, self.conventions, force_get_topology=True,
                                                   api_key=api_key)
            path = "Raven/Databases/{0}".format(database)
            response = requests_handler.check_database_exists("docs?id=" + Utils.quote_key(path))
            if response.status_code != 200:
                raise exceptions.ErrorResponseException("Could not open database named:{0}".format(database))
        return DocumentSession(database, self, requests_handler, session_id, force_read_from_master)

    def generate_id(self, entity):
        return self.generator.generate_document_id(entity, self.conventions, self._requests_handler)
