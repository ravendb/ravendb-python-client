from typing import Any

from pyravendb import constants
from pyravendb.documents.session.concurrency_check_mode import ConcurrencyCheckMode
from pyravendb.json.metadata_as_dictionary import MetadataAsDictionary


class DocumentInfo:
    def __init__(
        self,
        key: str = None,
        change_vector: str = None,
        concurrency_check_mode: ConcurrencyCheckMode = None,
        ignore_changes: bool = None,
        metadata: dict = None,
        document: dict = None,
        metadata_instance: MetadataAsDictionary = None,
        entity: Any = None,
        new_document: bool = None,
        collection: str = None,
    ):
        self.key = key
        self.change_vector = change_vector
        self.concurrency_check_mode = concurrency_check_mode
        self.ignore_changes = ignore_changes
        self.metadata = metadata
        self.document = document
        self.metadata_instance = metadata_instance
        self.entity = entity
        self.new_document = new_document
        self.collection = collection

    @staticmethod
    def get_new_document_info(document):
        metadata = document.get(constants.Documents.Metadata.KEY)
        if not metadata:
            raise ValueError("Document must have a metadata")
        key = metadata.get(constants.Documents.Metadata.ID)
        if not key or not isinstance(key, str):
            raise ValueError("Document must have a key")

        change_vector = metadata.get(constants.Documents.Metadata.CHANGE_VECTOR)
        if not change_vector or not isinstance(change_vector, str):
            raise ValueError(f"Document {key} must have a Change Vector")

        return DocumentInfo(key=key, document=document, metadata=metadata, entity=None, change_vector=change_vector)
