from connection.requests_factory import HttpRequestsFactory
from custom_exceptions import exceptions
from d_commands import database_commands
from data.document_convention import DocumentConvention
from hilo.hilo_generator import HiloGenerator
from data.database import DatabaseDocument
from store.document_session import DocumentSession
from tools.utils import Utils
import traceback


class DocumentStore(object):
    def __init__(self, url=None, database=None):
        self.url = url
        self.database = database
        self.conventions = DocumentConvention()
        self._requests_handler = HttpRequestsFactory(url, database, self.conventions)
        self._database_commands = None
        self._initialize = False
        self.generator = None

    @property
    def database_commands(self):
        self._assert_initialize()
        return self._database_commands

    def initialize(self):
        if self.database is None:
            raise ValueError("None database Name is not valid")
        path = "Raven/Databases/{0}".format(self.database)
        self._database_commands = database_commands.DatabaseCommands(self._requests_handler)
        response = self._requests_handler.database_open_request("id=" + Utils.quote_key(path))
        # here we unsure database exists if not create new one
        if response.status_code != 200:
            try:
                raise exceptions.ErrorResponseException(
                    "Could not open database named: {0}, database does not exists".format(self.database))
            except exceptions.ErrorResponseException:
                print(traceback.format_exc())
                self._database_commands.admin_commands.create_database(
                    DatabaseDocument(self.database, {"Raven/DataDir": "~\\{0}".format(self.database)}))

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

    def generate_id(self, entity):
        return self.generator.generate_document_id(entity, self.conventions, self._requests_handler)
