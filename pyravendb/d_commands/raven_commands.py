# -*- coding: utf-8 -*- #
from pyravendb.data.indexes import IndexQuery
from pyravendb.store.session_query import QueryOperator
from pyravendb.data.patches import PatchRequest
from pyravendb.data.indexes import IndexDefinition
from pyravendb.custom_exceptions import exceptions
from pyravendb.tools.utils import Utils
import collections


class __RavenCommand(object):
    def __init__(self, path=None, method=None, data=None, headers=None, admin_command=False):
        self.path = path
        self.method = method
        self.data = data
        self.headers = headers
        self.admin_command = admin_command
        self.__raven_command = True

    def create_request(self, server_node):
        raise NotImplementedError

    def set_response(self, response):
        raise NotImplementedError

    @property
    def raven_command(self):
        return self.__raven_command


class GetDocumentCommand(__RavenCommand):
    def __init__(self, key_or_keys, includes=None, metadata_only=False, force_read_from_master=False):
        """
        @param key_or_keys: the key of the documents you want to retrieve (key can be a list of ids)
        :type str or list
        @param includes: array of paths in documents in which server should look for a 'referenced' document
        :type list
        @param metadata_only: specifies if only document metadata should be returned
        :type bool
        @return: A list of the id or ids we looked for (if they exists)
        :rtype: dict
        @param force_read_from_master: If True the reading also will be from the master
        :type bool
        """
        super(GetDocumentCommand, self).__init__(method="GET")
        self.key_or_keys = key_or_keys
        self.includes = includes
        self.metadata_only = metadata_only
        self.force_read_from_master = force_read_from_master

    def create_request(self):
        if self.key_or_keys is None:
            raise ValueError("None Key is not valid")
        path = "docs?"
        if self.includes:
            path += "".join("&include=" + Utils.quote_key(item) for item in self.includes)
        # make get method handle a multi document requests in a single request
        if isinstance(self.key_or_keys, list):
            key_or_keys = collections.OrderedDict.fromkeys(self.key_or_keys)
            if self.metadata_only:
                path += "&metadata-only=True"

            # If it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
            if (sum(len(x) for x in key_or_keys)) > 1024:
                self.method = "POST"
                self.data = list(key_or_keys)
            else:
                path += "".join("&id=" + Utils.quote_key(item) for item in key_or_keys)

        else:
            path += "&id={0}".format(Utils.quote_key(self.key_or_keys))

        self.path = path

    def set_response(self, response):
        if response is None:
            raise ValueError("response is invalid.")

        if response.status_code == 200:
            return response.json()['Results']
        return None


class DeleteDocumentCommand(__RavenCommand):
    def __init__(self, key, etag=None):
        super(DeleteDocumentCommand, self).__init__(method="DELETE")
        self.key = key
        self.etag = etag

    def create_request(self, ):
        if self.key is None:
            raise ValueError("None Key is not valid")
        if not isinstance(self.key, str):
            raise ValueError("key must be {0}".format(type("")))

        if self.etag is not None:
            self.headers = {"If-Match": "\"{etag}\"".format(self.etag)}

        self.path = "docs?id={0}".format(Utils.quote_key(self.key))

    def set_response(self, response):
        if response.status_code != 204:
            raise exceptions.ErrorResponseException(response.json()["Error"])


class PutDocumentCommand(__RavenCommand):
    def __init__(self, key, document, etag=None):
        """
        @param key: unique key under which document will be stored
        :type str
        @param document: document data
        :type dict
        @param etag: current document etag, used for concurrency checks (null to skip check)
        :type str
        @return: json file
        :rtype: dict
        """
        super(PutDocumentCommand, self).__init__(method="PUT")
        self.key = key
        self.document = document
        self.etag = etag

    def create_request(self):
        if self.document is None:
            self.document = {}
        if self.etag is not None:
            self.headers = {"If-Match": "\"{etag}\"".format(self.etag)}
        if not isinstance(self.key, str):
            raise ValueError("key must be {0}".format(type("")))
        if not isinstance(self.document, dict):
            raise ValueError("document and metadata must be dict")

        self.data = self.document
        self.path = "docs?id={0}".format(self.key)

    def set_response(self, response):
        try:
            response = response.json()
            if "Error" in response:
                if "ActualEtag" in response:
                    raise exceptions.FetchConcurrencyException(response["Error"])
                raise exceptions.ErrorResponseException(response["Error"][:85])
            return response
        except ValueError:
            raise exceptions.ErrorResponseException(
                "Failed to put document in the database please check the connection to the server")


class BatchCommand(__RavenCommand):
    def __init__(self, commands_array):
        super(BatchCommand, self).__init__(method="POST")
        self.commands_array = commands_array

    def create_request(self):
        data = []
        for command in self.commands_array:
            if not hasattr(command, 'command'):
                raise ValueError("Not a valid command")
            data.append(command.to_json())

        self.path = "bulk_docs"
        self.data = data

    def set_response(self, response):
        try:
            response = response.json()
            if "Error" in response:
                raise ValueError(response["Error"])
            return response
        except ValueError:
            raise exceptions.ErrorResponseException(
                "Failed to put document in the database please check the connection to the server")


class PutIndexesCommand(__RavenCommand):
    def __init__(self, *indexes_to_add):
        """
        @param indexesToAdd:List of IndexDefinitions to add
        :type args
        @param overwrite: if set to True overwrite
        """
        super(PutIndexesCommand, self).__init__(method="PUT")
        if indexes_to_add is None:
            raise ValueError("None indexes_to_add is not valid")

        self.indexes_to_add = []
        for index_definition in indexes_to_add:
            if not isinstance(index_definition, IndexDefinition):
                raise ValueError("index_definition in indexes_to_add must be IndexDefinition type")
            if index_definition.name is None:
                raise ValueError("None Index name is not valid")
            self.indexes_to_add.append(index_definition.to_json())

    def create_request(self):
        self.path = "indexes"
        self.data = self.indexes_to_add

    def set_response(self, response):
        if response.status_code == 201:
            return response.json()['Results']
        response.raise_for_status()


class GetIndexCommand(__RavenCommand):
    def __init__(self, index_name, force_read_from_master=False):
        """
       @param index_name: Name of the index you like to get or delete
       :type str
       @param force_read_from_master: If True the reading also will be from the master
       :type bool
       """
        super(GetIndexCommand, self).__init__(method="GET")
        self.index_name = index_name
        self.force_read_from_master = force_read_from_master

    def create_request(self):
        self.path = "indexes?{0}".format(Utils.quote_key(self.index_name) if self.index_name else "")

    def set_response(self, response):
        if response.status_code != 200:
            return None
        return response.json()['Results']


class DeleteIndexCommand(__RavenCommand):
    def __init__(self, index_name):
        """
        @param index_name: Name of the index you like to get or delete
        :type str
        """
        super(DeleteIndexCommand, self).__init__(method="DELETE")
        self.index_name = index_name

    def create_request(self):
        if not self.index_name:
            raise ValueError("None or empty index_name is invalid")

        self.path = "indexes?name={0}".format(Utils.quote_key(self.index_name))

    def set_response(self, response):
        pass


class PatchByIndexCommand(__RavenCommand):
    def __init__(self, index_name, query_to_update, patch=None, options=None):
        """
        @param index_name: name of an index to perform a query on
        :type str
        @param query_to_update: query that will be performed
        :type IndexQuery
        @param options: various operation options e.g. AllowStale or MaxOpsPerSec
        :type BulkOperationOptions
        @param patch: JavaScript patch that will be executed on query results( Used only when update)
        :type PatchRequest
        @return: json
        :rtype: dict
        """
        super(PatchByIndexCommand, self).__init__(method="PATCH")
        self.index_name = index_name
        self.query_to_update = query_to_update
        self.patch = patch
        self.options = options

    def create_request(self):
        if not isinstance(self.query_to_update, IndexQuery):
            raise ValueError("query must be IndexQuery Type")

        if self.patch:
            if not isinstance(self.patch, PatchRequest):
                raise ValueError("scripted_patch must be ScriptedPatchRequest Type")
            self.patch = self.patch.to_json()

        self.path = Utils.build_path(self.index_name, self.query_to_update, self.options)
        self.data = self.patch

    def set_response(self, response):
        if response.status_code != 200 and response.status_code != 202:
            raise response.raise_for_status()
        return response.json()


class DeleteByIndexCommand(__RavenCommand):
    def __init__(self, index_name, query, options=None):
        """
        @param index_name: name of an index to perform a query on
        :type str
        @param query: query that will be performed
        :type IndexQuery
        @param options: various operation options e.g. AllowStale or MaxOpsPerSec
        :type BulkOperationOptions
        @return: json
        :rtype: dict
        """
        super(DeleteByIndexCommand, self).__init__(method="DELETE")
        self.index_name = index_name
        self.query = query
        self.options = options

    def create_request(self):
        self.path = Utils.build_path(self.index_name, self.query, self.options)

    def set_response(self, response):
        if response.status_code != 200 and response.status_code != 202:
            try:
                raise exceptions.ErrorResponseException(response.json()["Error"][:100])
            except ValueError:
                raise response.raise_for_status()
        return response.json()


class PatchCommand(__RavenCommand):
    def __init__(self, key, patch, etag=None, patch_if_missing=None,
                 skip_patch_if_etag_mismatch=False, return_debug_information=False):
        super(PatchCommand, self).__init__(method="PATCH")
        self.key = key
        self.patch = patch
        self.etag = etag
        self.patch_if_missing = patch_if_missing
        self._skip_patch_if_etag_mismatch = skip_patch_if_etag_mismatch
        self._return_debug_information = return_debug_information

    def create_request(self):

        if self.key is None:
            raise ValueError("None key is invalid")
        if self.patch is None:
            raise ValueError("None patch is invalid")
        if self.patch_if_missing and not self.patch_if_missing.script:
            raise ValueError("None or Empty script is invalid")

        path = "docs?id={0}".format(Utils.quote_key(self.key))
        if self._skip_patch_if_etag_mismatch:
            path += "&skipPatchIfEtagMismatch=true"
        if self._return_debug_information:
            path += "&debug=true"
        if self.etag is not None:
            self.headers = {"If-Match": "\"{etag}\"".format(self.etag)}

        self.path = path
        self.data = {"Patch": self.patch.to_json(),
                     "PatchIfMissing": self.patch_if_missing.to_json() if self.patch_if_missing else None}

    def set_response(self, response):
        if response.status_code == 201:
            return response.json()
        return None


class QueryCommand(__RavenCommand):
    def __init__(self, index_name, index_query, includes=None, metadata_only=False, index_entries_only=False,
                 force_read_from_master=False):
        """
        @param index_name: A name of an index to query
        @param force_read_from_master: If True the reading also will be from the master
        :type bool
        :type str
        @param index_query: A query definition containing all information required to query a specified index.
        :type IndexQuery
        @param includes: An array of relative paths that specify related documents ids
        which should be included in a query result.
        :type list
        @param metadata_only: True if returned documents should include only metadata without a document body.
        :type bool
        @param index_entries_only: True if query results should contain only index entries.
        :type bool
        @return:json
        :rtype:dict
        """
        super(QueryCommand, self).__init__(method="GET")
        self.index_name = index_name
        self.index_query = index_query
        self.includes = includes
        self.metadata_only = metadata_only
        self.index_entries_only = index_entries_only
        self.force_read_from_master = force_read_from_master

    def create_request(self):
        if not self.index_name:
            raise ValueError("index_name cannot be None or empty")
        if self.index_query is None:
            raise ValueError("None query is invalid")
        if not isinstance(self.index_query, IndexQuery):
            raise ValueError("query must be IndexQuery type")
        path = "queries/{0}?&pageSize={1}".format(Utils.quote_key(self.index_name), self.index_query.page_size)
        if self.index_query.default_operator is QueryOperator.AND:
            path += "&operator={0}".format(self.index_query.default_operator.value)
        if self.index_query.query:
            path += "&query={0}".format(Utils.quote_key(self.index_query.query))
        if self.index_query.sort_hints:
            for hint in self.index_query.sort_hints:
                path += "&{0}".format(hint)
        if self.index_query.sort_fields:
            for field in self.index_query.sort_fields:
                path += "&sort={0}".format(field)
        if self.index_query.fetch:
            for item in self.index_query.fetch:
                path += "&fetch={0}".format(item)
        if self.metadata_only:
            path += "&metadata-only=true"
        if self.index_entries_only:
            path += "&debug=entries"
        if self.includes and len(self.includes) > 0:
            path += "".join("&include=" + item for item in self.includes)

        if len(self.index_query.query) <= self._requests_handler.convention.max_length_of_query_using_get_url:
            self.method = "POST"

        self.path = path

    def set_response(self, response):
        if response is None or response.status_code != 200:
            return None
        response = response.json()
        if "Error" in response:
            raise exceptions.ErrorResponseException(response["Error"][:100])
        return response


class GetStatisticsCommand(__RavenCommand):
    def __init__(self):
        super(GetStatisticsCommand, self).__init__(method="GET")

    def create_request(self):
        self.path = "stats"

    def set_response(self, response):
        if response.status_code == 200:
            response = response.json()
        return response


# For Admin use only (create or delete databases)
class CreateDatabaseCommand(__RavenCommand):
    def __init__(self, database_document):
        """
        Creates a database

        @param database_document: has to be DatabaseDocument type
        """
        super(CreateDatabaseCommand, self).__init__(method="PUT", admin_command=True)
        self.database_document = database_document

    def create_request(self):
        if "Raven/DataDir" not in self.database_document.settings:
            raise exceptions.InvalidOperationException("The Raven/DataDir setting is mandatory")
        db_name = self.database_document.database_id.replace("Raven/Databases/", "")
        Utils.name_validation(db_name)

        self.path = "databases?name={0}".format(Utils.quote_key(db_name))
        self.data = self.database_document.to_json()

    def set_response(self, response):
        if response is None:
            raise ValueError("response is invalid.")

        if response.status_code == 200:
            response = response.json()
        return response


class DeleteDatabaseCommand(__RavenCommand):
    def __init__(self, name, hard_delete=False):
        """
        delete a database

        @param name: The name of the database
        :type str
        @param hard_delete: If true delete the database from the memory
        :type bool
        """
        super(DeleteDatabaseCommand, self).__init__(method="DELETE", admin_command=True)
        self.name = name
        self.hard_delete = hard_delete

    def create_request(self, server_node):
        db_name = self.name.replace("Rave/Databases/", "")
        path = "databases?name={0}".format(Utils.quote_key(db_name))
        if self.hard_delete:
            path += "?hard-delete=true"

        self.path = path

    def set_response(self, response):
        raise ValueError("response is invalid.")
