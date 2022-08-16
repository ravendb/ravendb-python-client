from typing import TYPE_CHECKING

import requests

from ravendb.documents.session.misc import DocumentQueryCustomization
from ravendb.http.topology import Topology

if TYPE_CHECKING:
    from ravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations


class EventArgs:
    pass  # empty by design


class BeforeStoreEventArgs(EventArgs):
    def __init__(self, session: "InMemoryDocumentSessionOperations", document_id: str, entity: object):
        self.__session = session
        self.__document_id = document_id
        self.__entity = entity
        self.__document_metadata = None

    @property
    def session(self) -> "InMemoryDocumentSessionOperations":
        return self.__session

    @property
    def document_id(self) -> str:
        return self.__document_id

    @property
    def entity(self) -> object:
        return self.__entity

    @property
    def is_metadata_accessed(self) -> bool:
        return self.__document_metadata is not None

    @property
    def document_metadata(self):
        if self.__document_metadata is None:
            self.__document_metadata = self.__session.advanced.get_metadata_for(self.__entity)
        return self.__document_metadata


class SessionClosingEventArgs(EventArgs):
    def __init__(self, session: "InMemoryDocumentSessionOperations"):
        self.__session = session

    @property
    def session(self) -> "InMemoryDocumentSessionOperations":
        return self.__session


class BeforeConversionToDocumentEventArgs(EventArgs):
    def __init__(self, key: str, entity: object, session: "InMemoryDocumentSessionOperations"):
        self.__key = key
        self.__entity = entity
        self.__session = session

    @property
    def key(self) -> str:
        return self.__key

    @property
    def entity(self) -> object:
        return self.__entity

    @property
    def session(self) -> "InMemoryDocumentSessionOperations":
        return self.__session


class AfterConversionToDocumentEventArgs(EventArgs):
    def __init__(self, session: "InMemoryDocumentSessionOperations", key: str, entity: object, document: dict):
        self.__session = session
        self.__key = key
        self.__entity = entity
        self.__document = document

    @property
    def key(self) -> str:
        return self.__key

    @property
    def entity(self) -> object:
        return self.__entity

    @property
    def session(self) -> "InMemoryDocumentSessionOperations":
        return self.__session

    @property
    def document(self) -> dict:
        return self.__document


class BeforeConversionToEntityEventArgs(EventArgs):
    def __init__(self, session: "InMemoryDocumentSessionOperations", key: str, object_type: type, document: dict):
        self.__session = session
        self.__key = key
        self.__object_type = object_type
        self.__document = document

    @property
    def session(self) -> "InMemoryDocumentSessionOperations":
        return self.__session

    @property
    def key(self) -> str:
        return self.__key

    @property
    def object_type(self) -> type:
        return self.__object_type

    @property
    def document(self) -> dict:
        return self.__document


class AfterConversionToEntityEventArgs(EventArgs):
    def __init__(self, session: "InMemoryDocumentSessionOperations", key: str, document: dict, entity: object):
        self.__session = session
        self.__key = key
        self.__document = document
        self.__entity = entity

    @property
    def session(self) -> "InMemoryDocumentSessionOperations":
        return self.__session

    @property
    def key(self) -> str:
        return self.__key

    @property
    def document(self) -> dict:
        return self.__document

    @property
    def entity(self) -> object:
        return self.__entity


class AfterSaveChangesEventArgs(EventArgs):
    def __init__(self, session: "InMemoryDocumentSessionOperations", document_id: str, entity: object):
        self.__session = session
        self.__document_id = document_id
        self.__entity = entity

    @property
    def session(self) -> "InMemoryDocumentSessionOperations":
        return self.__session

    @property
    def document_id(self) -> str:
        return self.__document_id

    @property
    def entity(self) -> object:
        return self.__entity


class BeforeDeleteEventArgs(EventArgs):
    def __init__(self, session: "InMemoryDocumentSessionOperations", key: str, entity: object):
        self.__session = session
        self.__key = key
        self.__entity = entity

    @property
    def session(self) -> "InMemoryDocumentSessionOperations":
        return self.session

    @property
    def key(self) -> str:
        return self.__key

    @property
    def entity(self) -> object:
        return self.__entity


class BeforeQueryEventArgs(EventArgs):
    def __init__(self, session: "InMemoryDocumentSessionOperations", query_customization: DocumentQueryCustomization):
        self.__session = session
        self.__query_customization = query_customization

    @property
    def session(self) -> "InMemoryDocumentSessionOperations":
        return self.session

    @property
    def query_customization(self) -> DocumentQueryCustomization:
        return self.__query_customization


class SessionCreatedEventArgs(EventArgs):
    def __init__(self, session: "InMemoryDocumentSessionOperations"):
        self.__session = session

    @property
    def session(self) -> "InMemoryDocumentSessionOperations":
        return self.__session


class BeforeRequestEventArgs(EventArgs):
    def __init__(self, database: str, url: str, request: requests.Request, attempt_number: int):
        self.__database = database
        self.__url = url
        self.__request = request
        self.__attempt_number = attempt_number

    @property
    def database(self) -> str:
        return self.__database

    @property
    def url(self) -> str:
        return self.__url

    @property
    def request(self) -> requests.Request:
        return self.__request

    @property
    def attempt_number(self) -> int:
        return self.__attempt_number


class SucceedRequestEventArgs(EventArgs):
    def __init__(
        self, database: str, url: str, response: requests.Response, request: requests.Request, attempt_number: int
    ):
        self.__database = database
        self.__url = url
        self.__response = response
        self.__request = request
        self.__attempt_number = attempt_number

    @property
    def database(self) -> str:
        return self.__database

    @property
    def url(self) -> str:
        return self.__url

    @property
    def response(self) -> requests.Response:
        return self.__response

    @property
    def request(self) -> requests.Request:
        return self.__request

    @property
    def attempt_number(self) -> int:
        return self.__attempt_number


class FailedRequestEventArgs(EventArgs):
    def __init__(self, database: str, url: str, exception: Exception):
        self.__database = database
        self.__url = url
        self.__exception = exception

    @property
    def database(self) -> str:
        return self.__database

    @property
    def url(self) -> str:
        return self.__url

    @property
    def exception(self) -> Exception:
        return self.__exception


class TopologyUpdatedEventArgs(EventArgs):
    def __init__(self, topology: Topology):
        self.__topology = topology

    @property
    def topology(self) -> Topology:
        return self.__topology
