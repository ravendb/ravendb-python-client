from __future__ import annotations

import http
import json
from typing import Optional, TYPE_CHECKING, List

import requests

from ravendb import constants
from ravendb.data.operation import AttachmentType
from ravendb.documents.operations.definitions import IOperation, VoidOperation
from ravendb.http.http_cache import HttpCache
from ravendb.http.misc import ResponseDisposeHandling
from ravendb.http.raven_command import RavenCommand, RavenCommandResponseType, VoidRavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.tools.utils import Utils

if TYPE_CHECKING:
    from ravendb.documents import DocumentStore
    from ravendb.documents.conventions import DocumentConventions


class AttachmentName:
    def __init__(self, name: str, hash: str, content_type: str, size: int):
        self.name = name
        self.hash = hash
        self.content_type = content_type
        self.size = size

    @classmethod
    def from_json(cls, json_dict: dict) -> AttachmentName:
        return cls(json_dict["Name"], json_dict["Hash"], json_dict["ContentType"], json_dict["Size"])


class AttachmentDetails(AttachmentName):
    def __init__(
        self, name: str, hash: str, content_type: str, size: int, change_vector: str = None, document_id: str = None
    ):
        super().__init__(name, hash, content_type, size)
        self.change_vector = change_vector
        self.document_id = document_id

    @classmethod
    def from_json(cls, json_dict: dict) -> AttachmentDetails:
        return cls(
            json_dict["Name"],
            json_dict["Hash"],
            json_dict["ContentType"],
            json_dict["Size"],
            json_dict["ChangeVector"],
            json_dict["DocumentId"],
        )


class CloseableAttachmentResult:
    def __init__(self, response: requests.Response, details: AttachmentDetails):
        self.__details = details
        self.__response = response

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def data(self):
        return self.__response.content

    @property
    def details(self):
        return self.__details

    def close(self):
        self.__response.close()


class AttachmentRequest:
    def __init__(self, document_id: str, name: str):
        if document_id.isspace():
            raise ValueError("Document_id cannot be None or whitespace")
        if name.isspace():
            raise ValueError("Name cannot be None or whitespace")

        self.document_id = document_id
        self.name = name


class PutAttachmentOperation(IOperation[AttachmentDetails]):
    def __init__(
        self,
        document_id: str,
        name: str,
        stream: bytes,
        content_type: Optional[str] = None,
        change_vector: Optional[str] = None,
    ):
        super().__init__()
        self.__document_id = document_id
        self.__name = name
        self.__stream = stream
        self.__content_type = content_type
        self.__change_vector = change_vector

    def get_command(self, store, conventions, cache=None):
        return self.__PutAttachmentCommand(
            self.__document_id,
            self.__name,
            self.__stream,
            self.__content_type,
            self.__change_vector,
        )

    class __PutAttachmentCommand(RavenCommand[AttachmentDetails]):
        def __init__(self, document_id: str, name: str, stream: bytes, content_type: str, change_vector: str):
            super().__init__(AttachmentDetails)

            if not document_id:
                raise ValueError("document_id cannot be None or blank")

            if not name:
                raise ValueError("name cannot be None or blank")

            self.__document_id = document_id
            self.__name = name
            self.__stream = stream
            self.__content_type = content_type
            self.__change_vector = change_vector

        def create_request(self, node: ServerNode) -> requests.Request:
            url = (
                f"{node.url}/databases/{node.database}/attachments?id={self.__document_id}"  # todo : escape
                f"&name={Utils.escape(self.__name, True,False)}"
            )

            if not self.__content_type.isspace():
                url += f"&contentType={Utils.escape(self.__content_type, True, False)}"

            request = requests.Request("PUT", url)
            if isinstance(self.__stream, (bytes, bytearray)):
                request.files = {self.__name: (self.__name, self.__stream, self.__content_type)}
            else:
                request.data = self.__stream
            self._add_change_vector_if_not_none(self.__change_vector, request)
            return request

        def set_response(self, response: str, from_cache: bool) -> None:
            self.result = AttachmentDetails.from_json(json.loads(response))

        def is_read_request(self) -> bool:
            return False


class GetAttachmentOperation(IOperation):
    def __init__(self, document_id: str, name: str, attachment_type: AttachmentType, change_vector: str):
        if document_id is None:
            raise ValueError("Invalid document_id")
        if name is None:
            raise ValueError("Invalid name")

        if attachment_type != AttachmentType.document and change_vector is None:
            raise ValueError(
                "change_vector",
                "Change Vector cannot be null for attachment type {0}".format(attachment_type),
            )

        super(GetAttachmentOperation, self).__init__()
        self.__document_id = document_id
        self.__name = name
        self.__attachment_type = attachment_type
        self.__change_vector = change_vector

    def get_command(self, store, conventions, cache=None) -> RavenCommand[dict]:
        return self.__GetAttachmentCommand(
            self.__document_id, self.__name, self.__attachment_type, self.__change_vector
        )

    class __GetAttachmentCommand(RavenCommand[CloseableAttachmentResult]):
        def __init__(self, document_id: str, name: str, attachment_type: AttachmentType, change_vector: str):
            super().__init__(CloseableAttachmentResult)
            self.__document_id = document_id
            self.__name = name
            self.__attachment_type = attachment_type
            self.__change_vector = change_vector

        def create_request(self, server_node: ServerNode) -> requests.Request:
            method = "GET"
            url = (
                f"{server_node.url}/databases/{server_node.database}/attachments"
                f"?id={Utils.quote_key(self.__document_id)}&name={Utils.quote_key(self.__name)}"
            )
            data = None
            if self.__attachment_type != AttachmentType.document:
                method = "POST"
                data = {
                    "Type": str(self.__attachment_type),
                    "ChangeVector": self.__change_vector,
                }
            return requests.Request(method, url, data=data)

        def is_read_request(self) -> bool:
            return True

        def process_response(self, cache: HttpCache, response: requests.Response, url) -> http.ResponseDisposeHandling:
            content_type = response.headers.get("Content-Type")
            change_vector = response.headers.get(constants.Headers.ETAG)
            hash = response.headers.get("Attachment-Hash")
            size = response.headers.get("Attachment-Size", 0)
            attachment_details = AttachmentDetails(
                self.__name, hash, content_type, size, change_vector, self.__document_id
            )
            self.result = CloseableAttachmentResult(response, attachment_details)
            return ResponseDisposeHandling.MANUALLY


class GetAttachmentsOperation(IOperation[CloseableAttachmentResult]):  # unfinished
    def __init__(self, attachments: List[AttachmentRequest], attachment_type: AttachmentType):
        self.__attachment_type = attachment_type
        self.__attachments = attachments

    def get_command(
        self, store: DocumentStore, conventions: DocumentConventions, cache: HttpCache
    ) -> RavenCommand[CloseableAttachmentResult]:
        return self.__GetAttachmentsCommand(self.__attachments, self.__attachment_type)

    class __GetAttachmentsCommand(RavenCommand[CloseableAttachmentResult]):
        def __init__(self, attachments: List[AttachmentRequest], attachment_type: AttachmentType):
            super().__init__(CloseableAttachmentResult)
            self.__attachment_type = attachment_type
            self.__attachments = attachments
            self._response_type = RavenCommandResponseType.EMPTY
            self.__attachments_metadata = []

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request(
                "POST",
                f"{node.url}/databases/{node.database}/attachments/bulk",
                data={
                    "AttachmentType": str(self.__attachment_type),
                    "Attachments": [
                        {"DocumentId": attachment.document_id, "Name": attachment.name}
                        for attachment in self.__attachments
                    ],
                },
            )

        def is_read_request(self) -> bool:
            return True

        def process_response(self, cache: HttpCache, response: requests.Response, url) -> ResponseDisposeHandling:
            raise NotImplementedError("Fetching multiple attachments is yet to be implemented")
            # todo: we can't decode it - maybe split bytes (part 1 is json, second part is binary attachments data) -


class DeleteAttachmentOperation(VoidOperation):
    def __init__(self, document_id: str, name: str, change_vector: Optional[str] = None):
        self._document_id = document_id
        self._name = name
        self._change_vector = change_vector

    def get_command(self, store: "DocumentStore", conventions: "DocumentConventions", cache=None) -> VoidRavenCommand:
        return self.__DeleteAttachmentCommand(self._document_id, self._name, self._change_vector)

    class __DeleteAttachmentCommand(VoidRavenCommand):
        def __init__(self, document_id: str, name: str, change_vector: Optional[str] = None):
            super().__init__()
            if not document_id:
                raise ValueError("Invalid document_id")
            if not name:
                raise ValueError("Invalid name")

            self.__document_id = document_id
            self.__name = name
            self.__change_vector = change_vector

        def create_request(self, server_node):
            request = requests.Request(
                "DELETE",
                f"{server_node.url}/databases/{server_node.database}/attachments?id={Utils.quote_key(self.__document_id)}&name={Utils.quote_key(self.__name)}",
            )
            self._add_change_vector_if_not_none(self.__change_vector, request)
            return request
