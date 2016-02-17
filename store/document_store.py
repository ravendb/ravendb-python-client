from connection.requests_factory import HttpRequestsFactory
from data.convention import Convention
from store.hilo_generator import HiloGenerator
from store.document_session import DocumentSession
from d_commands import database_commands
from custom_exceptions import exceptions
from tools.utils import Utils


class DocumentStore(object):
    def __init__(self, url=None, database=None):
        self.url = url
        self.database = database
        self.conventions = Convention()
        self.__requests_handler = HttpRequestsFactory(url, database, self.conventions)
        self.__database_commands = None
        self._initialize = False
        self.generator = None

    @property
    def database_commands(self):
        self._assert_initialize()
        return self.__database_commands

    def initialize(self):
        if self.database is None:
            raise ValueError("None database Name is not valid")
        path = "id=Raven/Databases/{0}".format(self.database)
        response = self.__requests_handler.database_open_request(Utils.quote_key(path))
        if response.status_code != 200:
            error = "Could not open database named: {0}, database does not exists".format(self.database)
            raise exceptions.ErrorResponseException(error)
        self.__database_commands = database_commands.DatabaseCommands(self.__requests_handler)
        self.generator = HiloGenerator(32)
        self._initialize = True

    def _assert_initialize(self):
        if not self._initialize:
            raise exceptions.InvalidOperationException(
                "You cannot open a session or access the database commands before initializing the document store.\
                Did you forget calling initialize()?")

    def open_session(self, database=None):
        self._assert_initialize()
        if database is None:
            database = self.database
        return DocumentSession(database, self)

    def generate_max_id_for_session(self, entity):
        return self.generator.generate_document_id(entity, self.conventions, self.__requests_handler)
