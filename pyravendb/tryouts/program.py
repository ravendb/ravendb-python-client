from pyravendb.store.document_store import documentstore
from pyravendb.connection.requests_handler import HttpRequestsHandler
from pyravendb.d_commands.raven_commands import *


class Test(object):
    def __init__(self, Name):
        self.Id = None
        self.Name = Name


class Person(object):
    def __init__(self):
        self.name = "testing"
        self.Id = None

if __name__ == "__main__":
    with documentstore(url="http://localhost:8080", database="NorthWind") as store:
        store.initialize()
        # with store.open_session() as session:
        #     person = Person()
        #     session.store(person)
        #     session.save_changes()
        #     last_document_key = session.advanced.get_document_id(person)
        request_handler = HttpRequestsHandler("http://localhost:8080", "NorthWind")
        indexes = GetIndexCommand(None, requests_handler=request_handler).create_request()

        for index in indexes:
            print 1

