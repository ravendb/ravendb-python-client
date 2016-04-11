from connection.requests_factory import HttpRequestsFactory
from custom_exceptions import exceptions
from d_commands import database_commands
from data.document_convention import DocumentConvention
from hilo.hilo_generator import HiloGenerator
from data.database import DatabaseDocument
from store.document_session import documentsession
from tools.utils import Utils
import traceback
import uuid


class documentstore(object):
    def __init__(self, url=None, database=None, api_key=None):
        self.url = url
        self.database = database
        self.conventions = DocumentConvention()
        self.api_key = api_key
        self._requests_handler = HttpRequestsFactory(url, database, self.conventions, api_key=self.api_key)
        self._database_commands = None
        self._initialize = False
        self.generator = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def database_commands(self):
        self._assert_initialize()
        return self._database_commands

    def initialize(self):
        if not self._initialize:
            if self.database is None:
                raise ValueError("None database Name is not valid")
            path = "Raven/Databases/{0}".format(self.database)
            self._database_commands = database_commands.DatabaseCommands(self._requests_handler)
            response = self._requests_handler.check_database_exists("docs?id=" + Utils.quote_key(path))
            # here we unsure database exists if not create new one
            if response.status_code == 404:
                try:
                    raise exceptions.ErrorResponseException(
                        "Could not open database named: {0}, database does not exists".format(self.database))
                except exceptions.ErrorResponseException:
                    print(traceback.format_exc())
                    self._database_commands.admin_commands.create_database(
                        DatabaseDocument(self.database, {"Raven/DataDir": "~\\{0}".format(self.database)}))
            self._requests_handler.get_replication_topology()
            self.generator = HiloGenerator(self.conventions.max_ids_to_catch, self._database_commands)
            self._initialize = True

    def _assert_initialize(self):
        if not self._initialize:
            raise exceptions.InvalidOperationException(
                "You cannot open a session or access the database commands before initializing the document store.\
                Did you forget calling initialize()?")

    def open_session(self, database=None, force_read_from_master=False):
        self._assert_initialize()
        session_id = uuid.uuid4()
        database_commands_for_session = self._database_commands
        if database is not None:
            path = "Raven/Databases/{0}".format(database)
            response = self._requests_handler.check_database_exists("docs?id=" + Utils.quote_key(path))
            if response.status_code != 200:
                raise exceptions.ErrorResponseException("Could not open database named:{0}".format(database))
            # Need to make new for other database
            # TODO make sure I know the right api_key
            requests_handler = HttpRequestsFactory(self.url, database, self.conventions, force_get_topology=True,
                                                   api_key=self.api_key)
            database_commands_for_session = database_commands.DatabaseCommands(requests_handler)
        return documentsession(database, self, database_commands_for_session, session_id, force_read_from_master)

    def generate_id(self, entity):
        return self.generator.generate_document_id(entity, self.conventions, self._requests_handler)
