# -*- coding: utf-8 -*- #
from pyravendb.data.indexes import IndexQuery
from pyravendb.store.session_query import QueryOperator
from pyravendb.data.patches import ScriptedPatchRequest
from pyravendb.data.indexes import IndexDefinition
from pyravendb.custom_exceptions import exceptions
from pyravendb.tools.utils import Utils
import collections


class DatabaseCommands(object):
    def __init__(self, request_handler):
        self._requests_handler = request_handler
        self.admin_commands = self.Admin(self._requests_handler)

    def change_database(self, database):
        self._requests_handler.database = database
        self._requests_handler.copy = True

    @staticmethod
    def run_async(func, func_parameter=()):

        """
        @param func: The instance of the function you want to run
        :type instancemethod
        @param func_parameter: The function parameter
        :type tuple
        """

        from multiprocessing.pool import ThreadPool
        pool = ThreadPool(processes=1)
        async_result = pool.apply_async(func, func_parameter)
        return async_result.get()

    def get(self, key_or_keys, includes=None, metadata_only=False, force_read_from_master=False):
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
        if key_or_keys is None:
            raise ValueError("None Key is not valid")
        path = "queries/?"
        method = "GET"
        data = None
        if includes:
            path += "".join("&include=" + Utils.quote_key(item) for item in includes)
        # make get method handle a multi document requests in a single request
        if isinstance(key_or_keys, list):
            key_or_keys = collections.OrderedDict.fromkeys(key_or_keys)
            if metadata_only:
                path += "&metadata-only=True"

            # If it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
            if (sum(len(x) for x in key_or_keys)) > 1024:
                method = "POST"
                data = list(key_or_keys)
            else:
                path += "".join("&id=" + Utils.quote_key(item) for item in key_or_keys)

        else:
            path += "&id={0}".format(Utils.quote_key(key_or_keys))

        response = self._requests_handler.http_request_handler(path, method, data=data,
                                                               force_read_from_master=force_read_from_master)
        if response.status_code == 200:
            response = response.json()
        return response

    def delete(self, key, etag=None):
        if key is None:
            raise ValueError("None Key is not valid")
        if not isinstance(key, str):
            raise ValueError("key must be {0}".format(type("")))
        headers = {}
        if etag is not None:
            headers["If-None-Match"] = etag
        key = Utils.quote_key(key)
        path = "docs/{0}".format(key)
        response = self._requests_handler.http_request_handler(path, "DELETE", headers=headers)
        if response.status_code != 204:
            raise exceptions.ErrorResponseException(response.json()["Error"])

    def put(self, key, document, metadata=None, etag=None):
        """
        @param key: unique key under which document will be stored
        :type str
        @param document: document data
        :type dict
        @param metadata: document metadata
        :type dict
        @param etag: current document etag, used for concurrency checks (null to skip check)
        :type str
        @return: json file
        :rtype: dict
        """
        headers = None
        if document is None:
            document = {}
        if metadata is None:
            metadata = {}
        if not isinstance(key, str):
            raise ValueError("key must be {0}".format(type("")))
        if not isinstance(document, dict) or not isinstance(metadata, dict):
            raise ValueError("document and metadata must be dict")
        data = [{"Key": key, "Document": document, "Metadata": metadata, "AdditionalData": None,
                 "Method": "PUT", "Etag": etag}]
        if etag:
            headers = {"if-None-Match": etag}
        response = self._requests_handler.http_request_handler("bulk_docs", "POST", data=data, headers=headers).json()
        if "Error" in response:
            if "ActualEtag" in response:
                raise exceptions.FetchConcurrencyException(response["Error"])
            raise exceptions.ErrorResponseException(response["Error"][:85])
        return response

    def batch(self, commands_array):
        data = []
        for command in commands_array:
            if not hasattr(command, 'command'):
                raise ValueError("Not a valid command")
            data.append(command.to_json())
        response = self._requests_handler.http_request_handler("bulk_docs", "POST", data=data).json()
        if "Error" in response:
            raise ValueError(response["Error"])
        return response

    def put_index(self, index_name, index_def, overwrite=False):
        """
        @param index_name:The name of the index
        @param index_def: IndexDefinition class a definition of a RavenIndex
        @param overwrite: if set to True overwrite
        """
        if index_name is None:
            raise ValueError("None index_name is not valid")
        if not isinstance(index_def, IndexDefinition):
            raise ValueError("index_def must be IndexDefinition type")
        index_name = Utils.quote_key(index_name)
        path = "indexes/{0}?definition=yes".format(index_name)
        response = self._requests_handler.http_request_handler(path, "GET")
        if not overwrite and response.status_code != 404:
            raise exceptions.InvalidOperationException("Cannot put index:{0},index already exists".format(index_name))

        data = index_def.to_json()
        return self._requests_handler.http_request_handler(path, "PUT", data=data).json()

    def get_index(self, index_name, force_read_from_master=False):
        """
        @param index_name: Name of the index you like to get or delete
        :type str
        @return: json or None
        :rtype: dict
        @param force_read_from_master: If True the reading also will be from the master
        :type bool
        """
        path = "indexes/{0}?definition=yes".format(Utils.quote_key(index_name))
        response = self._requests_handler.http_request_handler(path, "GET",
                                                               force_read_from_master=force_read_from_master)
        if response.status_code != 200:
            return None
        return response.json()

    def delete_index(self, index_name):
        """
        @param index_name: Name of the index you like to get or delete
        :type str
        @return: json or None
        :rtype: dict
        """
        if not index_name:
            raise ValueError("None or empty index_name is invalid")
        path = "indexes/{0}".format(Utils.quote_key(index_name))
        self._requests_handler.http_request_handler(path, "DELETE")

    def update_by_index(self, index_name, query, scripted_patch=None, options=None):
        """
        @param index_name: name of an index to perform a query on
        :type str
        @param query: query that will be performed
        :type IndexQuery
        @param options: various operation options e.g. AllowStale or MaxOpsPerSec
        :type BulkOperationOptions
        @param scripted_patch: JavaScript patch that will be executed on query results( Used only when update)
        :type ScriptedPatchRequest
        @return: json
        :rtype: dict
        """
        if not isinstance(query, IndexQuery):
            raise ValueError("query must be IndexQuery Type")
        path = Utils.build_path(index_name, query, options)
        if scripted_patch:
            if not isinstance(scripted_patch, ScriptedPatchRequest):
                raise ValueError("scripted_patch must be ScriptedPatchRequest Type")
            scripted_patch = scripted_patch.to_json()

        response = self._requests_handler.http_request_handler(path, "EVAL", data=scripted_patch)
        if response.status_code != 200 and response.status_code != 202:
            raise response.raise_for_status()
        return response.json()

    def delete_by_index(self, index_name, query, options=None):
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
        path = Utils.build_path(index_name, query, options)
        response = self._requests_handler.http_request_handler(path, "DELETE")
        if response.status_code != 200 and response.status_code != 202:
            try:
                raise exceptions.ErrorResponseException(response.json()["Error"][:100])
            except ValueError:
                raise response.raise_for_status()
        return response.json()

    def patch(self, key, scripted_patch, etag=None, ignore_missing=True, default_metadata=None, patch_default=None):
        from pyravendb.d_commands import commands_data
        if default_metadata is None:
            default_metadata = {}
        batch_result = self.batch([
            commands_data.ScriptedPatchCommandData(key, scripted_patch, etag, default_metadata, patch_default)])
        if not ignore_missing and batch_result[0]["PatchResult"] == "DocumentDoesNotExists" and patch_default is None:
            raise exceptions.DocumentDoesNotExistsException("Document with key {0} does not exist.".format(key))
        return batch_result

    def query(self, index_name, index_query, includes=None, metadata_only=False, index_entries_only=False,
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
        if not index_name:
            raise ValueError("index_name cannot be None or empty")
        if index_query is None:
            raise ValueError("None query is invalid")
        if not isinstance(index_query, IndexQuery):
            raise ValueError("query must be IndexQuery type")
        path = "indexes/{0}?&pageSize={1}".format(Utils.quote_key(index_name), index_query.page_size)
        if index_query.default_operator is QueryOperator.AND:
            path += "&operator={0}".format(index_query.default_operator.value)
        if index_query.query:
            path += "&query={0}".format(Utils.quote_key(index_query.query))
        if index_query.sort_hints:
            for hint in index_query.sort_hints:
                path += "&{0}".format(hint)
        if index_query.sort_fields:
            for field in index_query.sort_fields:
                path += "&sort={0}".format(field)
        if index_query.fetch:
            for item in index_query.fetch:
                path += "&fetch={0}".format(item)
        if metadata_only:
            path += "&metadata-only=true"
        if index_entries_only:
            path += "&debug=entries"
        if includes and len(includes) > 0:
            path += "".join("&include=" + item for item in includes)
        response = self._requests_handler.http_request_handler(path, "GET",
                                                               force_read_from_master=force_read_from_master).json()
        if "Error" in response:
            raise exceptions.ErrorResponseException(response["Error"][:100])
        return response

    # For Admin use only (create or delete databases)
    class Admin(object):
        def __init__(self, requests_handler):
            self.requests_handler = requests_handler

        def create_database(self, database_document):
            """
            Creates a database

            @param database_document: has to be DatabaseDocument type
            """
            if "Raven/DataDir" not in database_document.settings:
                raise exceptions.InvalidOperationException("The Raven/DataDir setting is mandatory")
            db_name = database_document.database_id.replace("Raven/Databases/", "")
            Utils.name_validation(db_name)
            path = "databases/{0}".format(Utils.quote_key(db_name))
            response = self.requests_handler.http_request_handler(path, "PUT", database_document.to_json(),
                                                                  admin=True)
            if response.status_code != 200:
                raise exceptions.ErrorResponseException(
                    "Database with the name '{0}' already exists".format(database_document.database_id))
            return response

        def delete_database(self, db_name, hard_delete=False):
            db_name = db_name.replace("Rave/Databases/", "")
            path = "databases/{0}".format(Utils.quote_key(db_name))
            if hard_delete:
                path += "?hard-delete=true"
            response = self.requests_handler.http_request_handler(path, "DELETE", admin=True)
            if response.status_code != 200:
                raise response.raise_for_status()
            return response

        def get_store_statistics(self):
            response = self.requests_handler.http_request_handler("stats", "GET", admin=True)
            if response.status_code == 200:
                return response.json()
