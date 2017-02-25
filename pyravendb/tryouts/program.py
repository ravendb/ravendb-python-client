from pyravendb.store.document_store import DocumentStore
from pyravendb.d_commands.commands_data import DeleteCommandData
from pyravendb.d_commands.raven_commands import CreateDatabaseCommand, PutDocumentCommand, GetDocumentCommand
from pyravendb.connection.requests_handler import HttpRequestsHandler
from pyravendb.data.database import DatabaseDocument
from pyravendb.d_commands.raven_commands import *

if __name__ == "__main__":
    with DocumentStore("http://localhost.fiddler:8080", "NorthWind") as store:
        store.initialize()
        handler = store.get_request_handler()
        # create_database = CreateDatabaseCommand(DatabaseDocument("NorthWind", {"Raven/DataDir": "test"}))
        # handler.http_request_handler(dd)
        store = PutDocumentCommand("tests/2", {"name": "Idan"})
        handler.http_request_handler(store)
        store2 = PutDocumentCommand("tests/3", {"name": "ilay"})
        handler.http_request_handler(store2)
        get = GetDocumentCommand(["tests/2", "tests/3"])
        c = handler.http_request_handler(get)
        if isinstance(c, list):
            for doc in c:
                print(doc['name'])
        else:
            print(c[0]["name"])
