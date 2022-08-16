from __future__ import annotations

import json

from ravendb.exceptions.raven_exceptions import ConflictException, BadResponseException


class DocumentConflictException(ConflictException):
    def __init__(self, message: str, doc_id: str = None, largest_etag: int = None):
        super(DocumentConflictException, self).__init__(message)
        self.doc_id = doc_id
        self.largest_etag = largest_etag

    @classmethod
    def from_message(cls, message: str) -> DocumentConflictException:
        return cls(message, None, 0)

    @classmethod
    def from_json(cls, json_s: str) -> DocumentConflictException:
        try:
            json_node = json.loads(json_s)
            doc_id = json_node.get("DocId")
            message = json_node.get("Message")
            largest_etag = json_node.get("LargestEtag")

            return cls(str(message), str(doc_id), int(largest_etag))
        except IOError as e:
            raise BadResponseException("Unable to parse server response: ", e)
