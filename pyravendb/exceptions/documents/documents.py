from __future__ import annotations

import json

from pyravendb.exceptions.exceptions import BadResponseException


class DocumentConflictException(BadResponseException):
    def __init__(self, message: str, doc_id: str = None, largest_etag: int = None):
        super(DocumentConflictException, self).__init__(message)
        self.doc_id = doc_id
        self.largest_etag = largest_etag

    @staticmethod
    def from_message(message: str) -> DocumentConflictException:
        return DocumentConflictException(message, None, 0)

    @staticmethod
    def from_json(json_s: str) -> DocumentConflictException:
        try:
            json_node = json.loads(json_s)
            doc_id = json_node.get("DocId")
            message = json_node.get("Message")
            largest_etag = json_node.get("LargestEtag")

            return DocumentConflictException(str(message), str(doc_id), int(largest_etag))
        except IOError as e:
            raise BadResponseException("Unable to parse server response: ", e)
