from pyravendb.commands.raven_commands import *
from pyravendb.data.patches import PatchRequest
from abc import abstractmethod
from pyravendb.tools.utils import Utils
from pyravendb.data.operation import AttachmentType
from pyravendb.custom_exceptions import exceptions
from pyravendb.data.query import IndexQuery


class Operation(object):
    __slots__ = ["__operation"]

    def __init__(self):
        self.__operation = "Operation"

    @property
    def operation(self):
        return self.__operation

    @abstractmethod
    def get_command(self, store, conventions, cache=None):
        raise NotImplementedError


class DeleteAttachmentOperation(Operation):
    def __init__(self, document_id, name, change_vector=None):
        super(DeleteAttachmentOperation, self).__init__()
        self._document_id = document_id
        self._name = name
        self._change_vector = change_vector

    def get_command(self, store, conventions, cache=None):
        return self._DeleteAttachmentCommand(self._document_id, self._name, self._change_vector)

    class _DeleteAttachmentCommand(RavenCommand):
        def __init__(self, document_id, name, change_vector=None):
            super(DeleteAttachmentOperation._DeleteAttachmentCommand, self).__init__(method="DELETE")

            if not document_id:
                raise ValueError("Invalid document_id")
            if not name:
                raise ValueError("Invalid name")

            self._document_id = document_id
            self.name = name
            self._change_vector = change_vector

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/attachments?id={2}&name={3}".format(
                server_node.url,
                server_node.database,
                Utils.quote_key(self._document_id),
                Utils.quote_key(self.name),
            )
            if self._change_vector is not None:
                self.headers = {"If-Match": '"{0}"'.format(self._change_vector)}

        def set_response(self, response):
            pass


class GetAttachmentOperation(Operation):
    def __init__(self, document_id, name, attachment_type, change_vector):
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
        self._document_id = document_id
        self._name = name
        self._attachment_type = attachment_type
        self._change_vector = change_vector

    def get_command(self, store, conventions, cache=None):
        return self._GetAttachmentCommand(self._document_id, self._name, self._attachment_type, self._change_vector)

    class _GetAttachmentCommand(RavenCommand):
        def __init__(self, document_id, name, attachment_type, change_vector):
            """
            @param document_id: The id of the document
             :type str
            @param name: The name of the attachment
            :type str
            @param attachment_type: The type of the attachment
            :type AttachmentType
            @param change_vector: The change vector of the document (needed only in revision)
            :type str
            @return: dict with the response and the attachment details
            """
            super(GetAttachmentOperation._GetAttachmentCommand, self).__init__(
                method="GET", is_read_request=True, use_stream=True
            )
            self._document_id = document_id
            self._name = name
            self._attachment_type = attachment_type
            self._change_vector = change_vector

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/attachments?id={2}&name={3}".format(
                server_node.url,
                server_node.database,
                Utils.quote_key(self._document_id),
                Utils.quote_key(self._name),
            )

            if self._attachment_type != AttachmentType.document:
                self.method = "POST"
                self.data = {
                    "Type": str(self._attachment_type),
                    "ChangeVector": self._change_vector,
                }

        def set_response(self, response):
            if response is None:
                return None

            if response.status_code == 200:
                attachment_details = {
                    "content_type": response.headers.get("Content-Type", None),
                    "change_vector": Utils.get_change_vector_from_header(response),
                    "hash": response.headers.get("Attachment-Hash", None),
                    "size": response.headers.get("Attachment-Size", 0),
                }

                return {"response": response, "details": attachment_details}


class PatchOperation(Operation):
    def __init__(
        self,
        document_id,
        change_vector,
        patch,
        patch_if_missing=None,
        skip_patch_if_change_vector_mismatch=False,
    ):
        """
        @param str document_id: The id of the document
        @param str change_vector: The change_vector
        @param PatchRequest patch: The patch that going to be applied on the document
        @param PatchRequest patch_if_missing: The default patch to applied
        @param bool skip_patch_if_change_vector_mismatch: If True will skip documents that mismatch the change_vector
        """
        super(PatchOperation, self).__init__()
        self._document_id = document_id
        self._change_vector = change_vector
        self._patch = patch
        self._patch_if_missing = patch_if_missing
        self._skip_patch_if_change_vector_mismatch = skip_patch_if_change_vector_mismatch

    def get_command(self, store, conventions, cache=None):
        return PatchCommand(
            self._document_id,
            self._change_vector,
            self._patch,
            self._patch_if_missing,
            self._skip_patch_if_change_vector_mismatch,
        )


class PutAttachmentOperation(Operation):
    def __init__(self, document_id, name, stream, content_type=None, change_vector=None):
        """
        @param document_id: The id of the document
        @param name: Name of the attachment
        @param stream: The attachment as bytes (ex.open("file_path", "rb"))
        @param content_type: The type of the attachment (ex.image/png)
        @param change_vector: The change vector of the document
        """

        super(PutAttachmentOperation, self).__init__()
        self._document_id = document_id
        self._name = name
        self._stream = stream
        self._content_type = content_type
        self._change_vector = change_vector

    def get_command(self, store, conventions, cache=None):
        return PutAttachmentCommand(
            self._document_id,
            self._name,
            self._stream,
            self._content_type,
            self._change_vector,
        )


class GetFacetsOperation(Operation):
    def __init__(self, query):
        """
        @param FacetQuery query: The query we wish to get
        """
        if query is None:
            raise ValueError("Invalid query")

        super(GetFacetsOperation, self).__init__()
        self._query = query

    def get_command(self, store, conventions, cache=None):
        return GetFacetsCommand(self._query)


class GetMultiFacetsOperation(Operation):
    def __init__(self, queries):
        if queries is None or len(queries) == 0:
            raise ValueError("Invalid queries")

        super(GetMultiFacetsOperation, self).__init__()
        self._queries = queries

    def get_command(self, store, conventions, cache=None):
        return self._GetMultiFacetsCommand(self._queries)

    class _GetMultiFacetsCommand(RavenCommand):
        def __init__(self, queries):
            super(GetMultiFacetsOperation._GetMultiFacetsCommand, self).__init__(is_read_request=True)
            requests = {}
            for q in queries:
                if not q:
                    raise ValueError("Invalid query")
                requests.update(
                    {
                        "url": "/queries",
                        "query": "?op=facets&query-hash=" + q.get_query_hash(),
                        "method": "POST",
                        "data": q.to_json(),
                    }
                )

            self._command = MultiGetCommand(requests)

        def create_request(self, server_node):
            return self._command.create_request(server_node)

        def set_response(self, response):
            results = self._command.set_response(response)
            return [result for result in results["Result"]]
