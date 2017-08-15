from pyravendb.commands.raven_commands import *
from pyravendb.data.patches import PatchRequest
from abc import abstractmethod
from pyravendb.tools.utils import Utils
from pyravendb.data.operation import AttachmentType
from pyravendb.custom_exceptions import exceptions
from pyravendb.data.query import IndexQuery


class QueryOperationOptions(object):
    """
      @param allow_stale Indicates whether operations are allowed on stale indexes.
      :type bool
      @param stale_timeout: If AllowStale is set to false and index is stale, then this is the maximum timeout to wait
      for index to become non-stale. If timeout is exceeded then exception is thrown.
      None by default - throw immediately if index is stale.
      :type timedelta
      @param max_ops_per_sec Limits the amount of base Operation per second allowed.
      :type int
      @param retrieve_details Determines whether Operation details about each document should be returned by server.
      :type bool
  """

    def __init__(self, allow_stale=True, stale_timeout=None, max_ops_per_sec=None, retrieve_details=False):
        self.allow_stale = allow_stale
        self.stale_timeout = stale_timeout
        self.retrieve_details = retrieve_details
        self.max_ops_per_sec = max_ops_per_sec


class Operation(object):
    __slots__ = ['__operation']

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
            self.url = "{0}/databases/{1}/attachments?id={2}&name={3}".format(server_node.url, server_node.database,
                                                                              Utils.quote_key(self._document_id),
                                                                              Utils.quote_key(self.name))
            if self._change_vector is not None:
                self.headers = {"If-Match": "\"{0}\"".format(self._change_vector)}

        def set_response(self, response):
            pass


class PatchByIndexOperation(Operation):
    def __init__(self, query_to_update, patch, options=None):
        if query_to_update is None:
            raise ValueError("Invalid query")
        if patch is None:
            raise ValueError("Invalid patch")
        super(PatchByIndexOperation, self).__init__()
        self._query_to_update = query_to_update
        self._patch = patch
        self._options = options

    def get_command(self, store, conventions, cache=None):
        return self.PatchByIndexCommand(self._query_to_update, self._patch, self._options)

    class PatchByIndexCommand(RavenCommand):
        def __init__(self, query_to_update, patch, options):
            """
            @param query_to_update: query that will be performed
            :type IndexQuery
            @param options: various Operation options e.g. AllowStale or MaxOpsPerSec
            :type QueryOperationOptions
            @param patch: JavaScript patch that will be executed on query results( Used only when update)
            :type PatchRequest
            @return: json
            :rtype: dict of operation_id
            """
            super(PatchByIndexOperation.PatchByIndexCommand, self).__init__(method="PATCH")
            self._query_to_update = query_to_update
            self._patch = patch
            self._options = options

        def create_request(self, server_node):
            if not isinstance(self._query_to_update, IndexQuery):
                raise ValueError("query must be IndexQuery Type")

            if self._patch:
                if not isinstance(self._patch, PatchRequest):
                    raise ValueError("_patch must be PatchRequest Type")
                self._patch = self._patch.to_json()

            self.url = server_node.url + "/databases/" + server_node.database + "/queries"
            path = "?allowStale={0}&maxOpsPerSec={1}&details={2}".format(self._options.allow_stale,
                                                                         "" if self._options.max_ops_per_sec is None else self._options.max_ops_per_sec,
                                                                         self._options.retrieve_details)
            if self._options.stale_timeout is not None:
                path += "&staleTimeout=" + str(self._options.stale_timeout)

            self.url += path
            self.data = {"Query": self._query_to_update.to_json(), "Patch": self._patch}

        def set_response(self, response):
            if response is None:
                raise exceptions.ErrorResponseException("Invalid Response")

            if response.status_code != 200 and response.status_code != 202:
                raise response.raise_for_status()

            return {"operation_id": response.json()["OperationId"]}


class DeleteByIndexOperation(Operation):
    def __init__(self, query_to_delete, options=None):
        """
        @param index_name: name of an index to perform a query on
        :type str
        @param query_to_delete: query that will be performed
        :type IndexQuery
        @param options: various Operation options e.g. AllowStale or MaxOpsPerSec
        :type QueryOperationOptions
        :rtype: dict of operation_id
       """
        if not query_to_delete:
            raise ValueError("Invalid query")

        super(DeleteByIndexOperation, self).__init__()
        self._query_to_delete = query_to_delete
        self._options = options if options is not None else QueryOperationOptions()

    def get_command(self, store, conventions, cache=None):
        return self._DeleteByIndexCommand(self._query_to_delete, self._options)

    class _DeleteByIndexCommand(RavenCommand):
        def __init__(self, query_to_delete, options=None):
            """
            @param query_to_delete: query that will be performed
            :type IndexQuery
            @param options: various Operation options e.g. AllowStale or MaxOpsPerSec
            :type QueryOperationOptions
            @return: json
            :rtype: dict
            """
            super(DeleteByIndexOperation._DeleteByIndexCommand, self).__init__(method="DELETE")
            self._query_to_delete = query_to_delete
            self._options = options

        def create_request(self, server_node):
            self.url = server_node.url + "/databases/" + server_node.database + "/queries"
            path = "?allowStale={0}&maxOpsPerSec={1}&details={2}".format(self._options.allow_stale,
                                                                         "" if self._options.max_ops_per_sec is None else self._options.max_ops_per_sec,
                                                                         self._options.retrieve_details)
            if self._options.stale_timeout is not None:
                path += "&staleTimeout=" + str(self._options.stale_timeout)

            self.url += path
            self.data = self._query_to_delete.to_json()

        def set_response(self, response):
            if response is None:
                raise exceptions.ErrorResponseException("Could not find index {0}".format(self.index_name))

            if response.status_code != 200 and response.status_code != 202:
                try:
                    raise exceptions.ErrorResponseException(response.json()["Error"])
                except ValueError:
                    raise response.raise_for_status()
            return {"operation_id": response.json()["OperationId"]}


class PatchCollectionOperation(Operation):
    def __init__(self, collection_name, patch):
        if collection_name is None:
            raise ValueError("Invalid collection_name")
        if patch is None:
            raise ValueError("Invalid patch")

        super(PatchCollectionOperation, self).__init__()
        self._collection_name = collection_name
        self._patch = patch

    def get_command(self, store, conventions, cache=None):
        return self._PatchByCollectionCommand(self._collection_name, self._patch)

    class _PatchByCollectionCommand(RavenCommand):
        def __init__(self, collection_name, patch):
            """
            @param collection_name: name of the collection
            :type str
            @param patch: JavaScript patch that will be executed on query results
            :type PatchRequest
            @return: json
            :rtype: dict
            """
            super(PatchCollectionOperation._PatchByCollectionCommand, self).__init__(method="PATCH")
            self._collection_name = collection_name
            self._patch = patch

        def create_request(self, server_node):
            if self._patch:
                if not isinstance(self._patch, PatchRequest):
                    raise ValueError("Patch must be PatchRequest Type")
                self._patch = self._patch.to_json()

            self.url = "{0}/databases/{1}/collections/docs?name={2}".format(server_node.url, server_node.database,
                                                                            self._collection_name)
            self.data = self._patch

        def set_response(self, response):
            if response is None:
                raise exceptions.ErrorResponseException("Invalid Response")

            if response.status_code != 200 and response.status_code != 202:
                raise response.raise_for_status()
            return {"operation_id": response.json()["OperationId"]}


class DeleteCollectionOperation(Operation):
    def __init__(self, collection_name):
        super(DeleteCollectionOperation, self).__init__()
        self.collection_name = collection_name

    def get_command(self, store, conventions, cache=None):
        return self._DeleteCollectionCommand(self.collection_name)

    class _DeleteCollectionCommand(RavenCommand):
        def __init__(self, collection_name):
            super(DeleteCollectionOperation._DeleteCollectionCommand, self).__init__(method="DELETE")
            self.collection_name = collection_name

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/collections/docs?name={2}".format(server_node.url, server_node.database,
                                                                            self.collection_name)

        def set_response(self, response):
            if response is None:
                raise exceptions.ErrorResponseException("Response is Invalid")

            if response.status_code != 200 and response.status_code != 202:
                try:
                    raise exceptions.ErrorResponseException(response.json()["Error"])
                except ValueError:
                    raise response.raise_for_status()
            return {"operation_id": response.json()["OperationId"]}


class GetAttachmentOperation(Operation):
    def __init__(self, document_id, name, attachment_type, change_vector):
        if document_id is None:
            raise ValueError("Invalid document_id")
        if name is None:
            raise ValueError("Invalid name")

        if attachment_type != AttachmentType.document and change_vector is None:
            raise ValueError("change_vector",
                             "Change Vector cannot be null for attachment type {0}".format(attachment_type))

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
            super(GetAttachmentOperation._GetAttachmentCommand, self).__init__(method="GET", is_read_request=True,
                                                                               use_stream=True)
            self._document_id = document_id
            self._name = name
            self._attachment_type = attachment_type
            self._change_vector = change_vector

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/attachments?id={2}&name={3}".format(
                server_node.url, server_node.database, Utils.quote_key(self._document_id),
                Utils.quote_key(self._name))

            if self._attachment_type != AttachmentType.document:
                self.method = "POST"
                self.data = {"Type": str(self._attachment_type), "ChangeVector": self._change_vector}

        def set_response(self, response):
            if response is None:
                return None

            if response.status_code == 200:
                attachment_details = {"content_type": response.headers.get("Content-Type", None),
                                      "change_vector": Utils.get_change_vector_from_header(response),
                                      "hash": response.headers.get("Attachment-Hash", None),
                                      "size": response.headers.get("Attachment-Size", 0)}

                return {"response": response, "details": attachment_details}


class PatchOperation(Operation):
    def __init__(self, document_id, change_vector, patch, patch_if_missing=None,
                 skip_patch_if_change_vector_mismatch=False):
        super(PatchOperation, self).__init__()
        self._document_id = document_id
        self._change_vector = change_vector
        self._patch = patch
        self._patch_if_missing = patch_if_missing
        self._skip_patch_if_change_vector_mismatch = skip_patch_if_change_vector_mismatch

    def get_command(self, store, conventions, cache=None):
        return PatchCommand(self._document_id, self._change_vector, self._patch, self._patch_if_missing,
                            self._skip_patch_if_change_vector_mismatch)


class PutAttachmentOperation(Operation):
    def __init__(self, document_id, name, stream, content_type=None, change_vector=None):
        """
        @param document_id: The id of the document
        @param name: Name of the attachment
        @param stream: The attachment as bytes (ex.open("file_path", "rb"))
        :param content_type: The type of the attachment (ex.image/png)
        :param change_vector: The change vector of the document
        """

        super(PutAttachmentOperation, self).__init__()
        self._document_id = document_id
        self._name = name
        self._stream = stream
        self._content_type = content_type
        self._change_vector = change_vector

    def get_command(self, store, conventions, cache=None):
        return PutAttachmentCommand(self._document_id, self._name, self._stream, self._content_type,
                                    self._change_vector)


class GetFacetsOperation(Operation):
    def __init__(self, query):
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
                    {"url": "/queries", "query": "?op=facets&query-hash=" + q.get_query_hash(), "method": "POST",
                     "data": q.to_json()})

            self._command = MultiGetCommand(requests)

        def create_request(self, server_node):
            return self._command.create_request(server_node)

        def set_response(self, response):
            results = self._command.set_response(response)
            return [result for result in results["Result"]]
