from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations


class EventArgs:
    pass  # empty by design


class BeforeStoreEventArgs(EventArgs):
    def __init__(self, session: InMemoryDocumentSessionOperations, document_id: str, entity: object):
        self.__session = session
        self.__document_id = document_id
        self.__entity = entity
        self.__document_metadata = None

    @property
    def session(self) -> InMemoryDocumentSessionOperations:
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
            self.__document_metadata = self.__session.get_metadata_for(self.__entity)
        return self.__document_metadata
