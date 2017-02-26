from pyravendb.store.document_store import DocumentStore
from pyravendb.d_commands.commands_data import DeleteCommandData
from pyravendb.d_commands.raven_commands import CreateDatabaseCommand, PutDocumentCommand, GetDocumentCommand
from pyravendb.connection.requests_executor import RequestsExecutor
from pyravendb.data.database import DatabaseDocument
from pyravendb.d_commands.raven_commands import *


class Users(object):
    def __init__(self, Name, Id=None):
        self.Id = Id
        self.Name = Name


if __name__ == "__main__":
    with DocumentStore("http://localhost.fiddler:8080", "NorthWind") as store:
        store.initialize()

        with store.open_session() as session:
            session.store(Users("Ko", "users/6"))
            session.save_changes()

        with store.open_session() as session:
            c = session.load("users/6", object_type=Users)
            s = session.load("users/6", object_type=Users)

            print(session.advanced.number_of_requests_in_session())
            c.Name = "Go"
            session.save_changes()
