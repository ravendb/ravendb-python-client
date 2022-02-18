from pyravendb.commands.raven_commands import *
from pyravendb.data.patches import PatchRequest
from abc import abstractmethod
from pyravendb.tools.utils import Utils
from pyravendb.data.operation import AttachmentType
from pyravendb.custom_exceptions import exceptions
from pyravendb.data.query import OldIndexQuery


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
