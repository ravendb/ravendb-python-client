# -*- coding: utf-8 -*- #
from builtins import ValueError

from pyravendb.subscriptions.data import SubscriptionState
from pyravendb.custom_exceptions import exceptions
from pyravendb.tools.utils import Utils
from abc import abstractmethod
from datetime import timedelta
import collections
import logging
import uuid

logging.basicConfig(filename='responses.log', level=logging.DEBUG)
log = logging.getLogger()


# Todo update the commands
class RavenCommand(object):
    def __init__(self, url=None, method=None, data=None, files=None, headers=None, is_read_request=False,
                 is_raft_request=False, use_stream=False):
        self.url = url
        self.method = method
        self.data = data
        self.headers = headers
        if self.headers is None:
            self.headers = {}
        self.__raven_command = True
        self.is_read_request = is_read_request
        self.use_stream = use_stream
        self.failed_nodes = {}
        self.timeout = None
        self.requested_node = None
        self.files = files
        self.is_raft_request = is_raft_request
        self._raft_unique_request_id = None
        if is_raft_request:
            self._raft_unique_request_id = uuid.uuid4()

    @abstractmethod
    def create_request(self, server_node):
        raise NotImplementedError

    @abstractmethod
    def set_response(self, response):
        raise NotImplementedError

    @property
    def raven_command(self):
        return self.__raven_command

    @property
    def raft_unique_request_id(self):
        return str(self._raft_unique_request_id)

    def is_failed_with_node(self, node):
        return len(self.failed_nodes) > 0 and node in self.failed_nodes


class GetDocumentCommand(RavenCommand):
    def __init__(self, key_or_keys, includes=None, metadata_only=False, start=None, page_size=None,
                 counter_includes=None):
        """
        @param key_or_keys: the key of the documents you want to retrieve (key can be a list of ids)
        :type str or list
        @param includes: array of paths in documents in which server should look for a 'referenced' document
        :type list
        @param metadata_only: specifies if only document metadata should be returned
        :type bool
        @param start: number of results to skip.
        type: int
        @param page_size: maximum number of results to retrieve
        type: int
        @param counter_includes: the names of the counters to be included or True to include all counters
        type: list[str] or bool
        @return: A list of the id or ids we looked for (if they exists)
        :rtype: dict
        """
        super(GetDocumentCommand, self).__init__(method="GET", is_read_request=True)
        self.key_or_keys = key_or_keys
        self.includes = includes
        self.metadata_only = metadata_only
        self.start = start
        self.page_size = page_size
        self.counter_includes = counter_includes

        if self.key_or_keys is None:
            raise ValueError("None Key is not valid")

    def create_request(self, server_node):
        path = "docs?"

        if self.start:
            path += f"&start={self.start}"
        if self.page_size:
            path += f"&pageSize={self.page_size}"
        if self.metadata_only:
            path += "&metadataOnly=true"
        if self.includes:
            path += "".join("&include=" + Utils.quote_key(item) for item in self.includes)
        if self.counter_includes:
            if self.counter_includes is True:
                path += f"&counter={Utils.quote_key('@all_counters')}"
            else:
                path += "".join("&counter=" + Utils.quote_key(item) for item in self.counter_includes)

        # make get method handle a multi document requests in a single request
        if isinstance(self.key_or_keys, list):
            key_or_keys = collections.OrderedDict.fromkeys(self.key_or_keys)
            # If it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
            if (sum(len(x) for x in key_or_keys)) > 1024:
                self.method = "POST"
                self.data = list(key_or_keys)
            else:
                path += "".join("&id=" + Utils.quote_key(item) for item in key_or_keys)

        else:
            path += "&id={0}".format(Utils.quote_key(self.key_or_keys))

        self.url = "{0}/databases/{1}/{2}".format(server_node.url, server_node.database, path)

    def set_response(self, response):
        if response is None:
            return None
        try:
            response = response.json()
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])
        except ValueError:
            raise exceptions.ErrorResponseException(
                "Failed to load document from the database please check the connection to the server")

        return response


class GetDocumentsByPrefixCommand(RavenCommand):
    def __init__(self, start_with, start_after=None, matches=None, exclude=None, start=None, page_size=None,
                 metadata_only=None):

        """
        @param start_with: Retrieve all documents whose IDs begin with this string.
        If the value of this parameter is left empty, all documents in the database are retrieved.
        :type str

        @param start_after: Retrieve only the results after the first document ID that begins with this prefix.
        :type str

        @param matches: Retrieve documents whose IDs are exactly <startsWith>+<matches>.
        Accepts multiple values separated by a pipe character: ' | ' .
        Use ? to represent any single character, and * to represent any string.
        :type str

        @param exclude: Exclude documents whose IDs are exactly <startsWith>+<exclude>.
        Accepts multiple values separated by a pipe character: ' | ' .
        Use ? to represent any single character, and * to represent any string.
        type: str

        @param start: number of results to skip.
        type: int

        @param page_size: maximum number of results to retrieve
        type: int

        @param metadata_only: specifies if only document metadata should be returned
        :type bool

        @return: A list of the id or ids we looked for (if they exists)
        :rtype: dict
        """

        super(GetDocumentsByPrefixCommand, self).__init__(method="GET", is_read_request=True)

        self.start_with = start_with
        self.start_after = start_after
        self.matches = matches
        self.exclude = exclude
        self.start = start
        self.page_size = page_size
        self.metadata_only = metadata_only

        if self.start_with is None:
            raise ValueError("None start_with is not valid")

    def create_request(self, server_node):
        path = f"docs?startsWith={Utils.quote_key(self.start_with)}"

        if self.matches:
            path += f"&matches={Utils.quote_key(self.matches)}"
        if self.exclude:
            path += f"&exclude={Utils.quote_key(self.exclude)}"
        if self.start_after:
            path += f"&startAfter={Utils.quote_key(self.start_after)}"
        if self.start:
            path += f"&start={self.start}"
        if self.page_size:
            path += f"&pageSize={self.page_size}"
        if self.metadata_only:
            path += "&metadataOnly=true"

        self.url = "{0}/databases/{1}/{2}".format(server_node.url, server_node.database, path)

    def set_response(self, response):
        if response is None:
            return

        response = response.json()
        if "Error" in response:
            raise exceptions.ErrorResponseException(response["Error"])
        return response


class DeleteDocumentCommand(RavenCommand):
    def __init__(self, key, change_vector=None):
        super(DeleteDocumentCommand, self).__init__(method="DELETE")
        self.key = key
        self.change_vector = change_vector

    def create_request(self, server_node):
        if self.key is None:
            raise ValueError("None Key is not valid")
        if not isinstance(self.key, str):
            raise ValueError("key must be {0}".format(type("")))

        if self.change_vector is not None:
            self.headers = {"If-Match": "\"{0}\"".format(self.change_vector)}

        self.url = "{0}/databases/{1}/docs?id={2}".format(server_node.url, server_node.database,
                                                          Utils.quote_key(self.key))

    def set_response(self, response):
        if response is None:
            log.info("Couldn't find The Document {0}".format(self.key))
        elif response.status_code != 204:
            response = response.json()
            raise exceptions.ErrorResponseException(response["Error"])


class PutDocumentCommand(RavenCommand):
    def __init__(self, key, document, change_vector=None):
        """
        @param str key: unique key under which document will be stored
        @param dict document: document data
        @param str change_vector: current document change_vector, used for concurrency checks (null to skip check)
        @return: json file
        :rtype: dict
        """
        super(PutDocumentCommand, self).__init__(method="PUT")
        self.key = key
        self.document = document
        self.change_vector = change_vector

    def create_request(self, server_node):
        if self.document is None:
            self.document = {}
        if self.change_vector is not None:
            self.headers = {"If-Match": "\"{0}\"".format(self.change_vector)}
        if not isinstance(self.key, str):
            raise ValueError("key must be {0}".format(type("")))
        if not isinstance(self.document, dict):
            raise ValueError("document and metadata must be dict")

        self.data = self.document
        self.url = "{0}/databases/{1}/docs?id={2}".format(server_node.url, server_node.database,
                                                          Utils.quote_key(self.key))

    def set_response(self, response):
        try:
            response = response.json()
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])
            return response
        except ValueError:
            raise exceptions.ErrorResponseException(
                "Failed to put document in the database please check the connection to the server")


class BatchCommand(RavenCommand):
    def __init__(self, commands):
        super(BatchCommand, self).__init__(method="POST")
        self.commands_array = commands
        self.files = None

    def create_request(self, server_node):
        data = []
        for command in self.commands_array:
            if not hasattr(command, 'command'):
                raise ValueError("Not a valid command")
            if command.type == "AttachmentPUT":
                if not self.files:
                    self.files = {}
                self.files[command.name] = (
                    command.name, command.stream, command.content_type, {"Command-Type": "AttachmentStream"})
            data.append(command.to_json())

        if self.files and len(self.files) > 0:
            self.use_stream = True

        self.url = "{0}/databases/{1}/bulk_docs".format(server_node.url, server_node.database)
        self.data = {"Commands": data}

    def set_response(self, response):
        if response is None:
            raise exceptions.InvalidOperationException(
                "Got null response from the server after doing a batch, "
                "something is very wrong. Probably a garbled response.")

        try:
            response = response.json()
            if "Error" in response:
                raise ValueError(response["Message"])
            return response["Results"]
        except ValueError as e:
            raise exceptions.InvalidOperationException(e)


class DeleteIndexCommand(RavenCommand):
    def __init__(self, index_name, database_name=None):
        """
        @param str index_name: Name of the index you like to get or delete
        @param str database_name: Name of the index database
        """
        super(DeleteIndexCommand, self).__init__(method="DELETE", is_raft_request=True)
        self.index_name = index_name
        self.database_name = database_name

    def create_request(self, server_node):
        if not self.index_name:
            raise ValueError("None or empty index_name is invalid")

        database = self.database_name if self.database_name else server_node.database
        self.url = "{0}/databases/{1}/indexes?name={2}".format(server_node.url, database,
                                                               Utils.quote_key(self.index_name, True))

    def set_response(self, response):
        pass


class PatchCommand(RavenCommand):
    def __init__(self, document_id, change_vector, patch, patch_if_missing,
                 skip_patch_if_change_vector_mismatch=False, return_debug_information=False, test=False):
        """
        @param str document_id: The id of the document
        @param str change_vector: The change_vector
        @param PatchRequest patch: The patch that going to be applied on the document
        @param PatchRequest patch_if_missing: The default patch to applied
        @param bool skip_patch_if_change_vector_mismatch: If True will skip documents that mismatch the change_vector
        """
        if document_id is None:
            raise ValueError("None key is invalid")
        if patch is None:
            raise ValueError("None patch is invalid")
        if patch_if_missing and not patch_if_missing.script:
            raise ValueError("None or Empty script is invalid")

        super(PatchCommand, self).__init__(method="PATCH")
        self._document_id = document_id
        self._change_vector = change_vector
        self._patch = patch
        self._patch_if_missing = patch_if_missing
        self._skip_patch_if_change_vector_mismatch = skip_patch_if_change_vector_mismatch
        self._return_debug_information = return_debug_information
        self._test = test

    def create_request(self, server_node):
        path = "docs?id={0}".format(Utils.quote_key(self._document_id))
        if self._skip_patch_if_change_vector_mismatch:
            path += "&skipPatchIfChangeVectorMismatch=true"
        if self._return_debug_information:
            path += "&debug=true"
        if self._test:
            path += "&test=true"

        if self._change_vector is not None:
            self.headers = {"If-Match": "\"{0}\"".format(self._change_vector)}

        self.url = "{0}/databases/{1}/{2}".format(server_node.url, server_node.database, path)
        self.data = {"Patch": self._patch.to_json(),
                     "PatchIfMissing": self._patch_if_missing.to_json() if self._patch_if_missing else None}

    def set_response(self, response):
        if response is None:
            return None

        if response and response.status_code == 200:
            return response.json()


class QueryCommand(RavenCommand):
    def __init__(self, conventions, index_query, metadata_only=False, index_entries_only=False):
        """
        @param IndexQuery index_query: A query definition containing all information required to query a specified index.
        @param bool metadata_only: True if returned documents should include only metadata without a document body.
        @param bool index_entries_only: True if query results should contain only index entries.
        @return:json
        :rtype:dict
        """
        super(QueryCommand, self).__init__(method="POST", is_read_request=True)
        if index_query is None:
            raise ValueError("Invalid index_query")
        if conventions is None:
            raise ValueError("Invalid convention")
        self._conventions = conventions
        self._index_query = index_query
        self._metadata_only = metadata_only
        self._index_entries_only = index_entries_only

    def create_request(self, server_node):
        self.url = "{0}/databases/{1}/queries?query-hash={2}".format(server_node.url, server_node.database,
                                                                     self._index_query.get_query_hash())
        if self._metadata_only:
            self.url += "&metadataOnly=true"
        if self._index_entries_only:
            self.url += "&debug=entries"

        self.data = self._index_query.to_json()

    def set_response(self, response):
        if response is None:
            return

        response = response.json()
        if "Error" in response:
            raise exceptions.ErrorResponseException(response["Error"])
        return response


class GetStatisticsCommand(RavenCommand):
    def __init__(self, debug_tag=None):
        super(GetStatisticsCommand, self).__init__(method="GET")
        self.debug_tag = debug_tag

    def create_request(self, server_node):
        self.url = "{0}/databases/{1}/stats".format(server_node.url, server_node.database)
        if self.debug_tag:
            self.url += "?" + self.debug_tag

    def set_response(self, response):
        if response and response.status_code == 200:
            return response.json()
        return None


class GetTopologyCommand(RavenCommand):
    def __init__(self, force_url=None):
        super(GetTopologyCommand, self).__init__(method="GET", is_read_request=True)
        self._force_url = force_url

    def create_request(self, server_node):
        self.url = "{0}/topology?name={1}".format(server_node.url, server_node.database)
        if self._force_url is not None and not self._force_url == "":
            self.url += "&url={0}".format(self._force_url)

    def set_response(self, response):
        if response.status_code == 200:
            return response.json()
        if response.status_code == 400:
            log.debug(response.json()["Error"])
        return None


class GetClusterTopologyCommand(RavenCommand):
    def __init__(self):
        super(GetClusterTopologyCommand, self).__init__(method="GET", is_read_request=True)

    def create_request(self, server_node):
        self.url = "{0}/cluster/topology".format(server_node.url)

    def set_response(self, response):
        if response.status_code == 200:
            return response.json()
        if response.status_code == 400:
            log.debug(response.json()["Error"])
        return None


class GetOperationStateCommand(RavenCommand):
    def __init__(self, operation_id, is_server_store_operation=False):
        super(GetOperationStateCommand, self).__init__(method="GET")
        self.operation_id = operation_id
        self.is_server_store_operation = is_server_store_operation

    def create_request(self, server_node):
        self.url = "{0}/databases/{1}/operations/state?id={2}".format(server_node.url, server_node.database,
                                                                      self.operation_id) if not self.is_server_store_operation \
            else "{0}/operations/state?id={2}".format(server_node.url, self.operation_id)

    def set_response(self, response):
        try:
            response = response.json()
        except ValueError:
            raise response.raise_for_status()
        return response


class PutAttachmentCommand(RavenCommand):
    def __init__(self, document_id, name, stream, content_type, change_vector):
        """
        @param document_id: The id of the document
        @param name: Name of the attachment
        @param stream: The attachment as bytes (ex.open("file_path", "rb"))
        @param content_type: The type of the attachment (ex.image/png)
        @param change_vector: The change vector of the document
        """

        if not document_id:
            raise ValueError("Invalid document_id")
        if not name:
            raise ValueError("Invalid name")

        super(PutAttachmentCommand, self).__init__(method="PUT")
        self._document_id = document_id
        self._name = name
        self._stream = stream
        self._content_type = content_type
        self._change_vector = change_vector

    def create_request(self, server_node):
        self.url = "{0}/databases/{1}/attachments?id={2}&name={3}".format(server_node.url, server_node.database,
                                                                          Utils.quote_key(self._document_id),
                                                                          Utils.quote_key(self._name))
        if self._content_type:
            self.url += "&contentType={0}".format(Utils.quote_key(self._content_type))
        self.data = self._stream

        if self._change_vector is not None:
            self.headers = {"If-Match": "\"{0}\"".format(self._change_vector)}

    def set_response(self, response):
        try:
            response = response.json()
            if "Error" in response:
                raise exceptions.InvalidOperationException(response["Error"])
            return response
        except ValueError:
            raise response.raise_for_status()


class GetFacetsCommand(RavenCommand):
    def __init__(self, query):
        """
        @param FacetQuery query: The query we wish to get
        """
        if not query:
            raise ValueError("Invalid query")
        super(GetFacetsCommand, self).__init__(method="POST", is_read_request=True)
        self._query = query
        if query.wait_for_non_stale_results_timeout and query.wait_for_non_stale_results_timeout != timedelta.max:
            self.timeout = self._query.wait_for_non_stale_results_timeout + timedelta(seconds=10)

    def create_request(self, server_node):
        if self._query.facet_setup_doc and len(self._query.facets) > 0:
            raise exceptions.InvalidOperationException("You cannot specify both 'facet_setup_doc' and 'facets'.")

        self.url = "{0}/databases/{1}/queries?op=facets&query-hash={2}".format(server_node.url, server_node.database,
                                                                               self._query.get_query_hash())
        self.data = self._query.to_json()

    def set_response(self, response):
        if response.status_code == 200:
            return response.json()


class MultiGetCommand(RavenCommand):
    def __init__(self, requests):
        """
        @param requests: The requests for the server
        :type list of dict
        dict keys:
            url = Request url (relative).
            headers =  Request headers.
            query = Query information e.g. "?pageStart=10&amp;pageSize=20".
            data = The data for the requests body
            method = The requests method e.g.
        """
        super(MultiGetCommand, self).__init__(method="POST")
        self._requests = requests
        self._base_url = None

    def create_request(self, server_node):
        self._base_url = "{0}/databases/{1}".format(server_node.url, server_node.database)
        commands = []
        for request in self._requests:
            headers = {}
            for key, value in request.get("headers", {}).items():
                headers[key] = value
            commands.append(
                {"Url": "/databases/{0}{1}".format(server_node.database, request["url"]),
                 "Query": request.get("query", None), "Method": request.get("method", None),
                 "Content": request.get("data", None), "Headers": headers})

        self.data = {"Requests": commands}
        self.url = "{0}/multi_get".format(self._base_url)

    def set_response(self, response):
        try:
            response = response.json()
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])
            return response["Results"]
        except:
            raise exceptions.ErrorResponseException("Invalid response")


class GetDatabaseRecordCommand(RavenCommand):
    def __init__(self, database_name):
        super(GetDatabaseRecordCommand, self).__init__(method="GET")
        self._database_name = database_name

    def create_request(self, server_node):
        self.url = "{0}/admin/databases?name={1}".format(server_node.url, self._database_name)

    def set_response(self, response):
        if response is None:
            return None

        try:
            response = response.json()
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])
            return response["Topology"]
        except:
            raise response.raise_for_status()


class WaitForRaftIndexCommand(RavenCommand):
    def __init__(self, index):
        super(WaitForRaftIndexCommand, self).__init__(method="GET", is_read_request=True)
        self._index = index

    def create_request(self, server_node):
        self.url = "{0}/rachis/waitfor?index={1}".format(server_node.url, self._index)

    def set_response(self, response):
        if response is None:
            raise exceptions.ErrorResponseException("Invalid response")


class GetTcpInfoCommand(RavenCommand):
    def __init__(self, tag, database_name=None):
        super(GetTcpInfoCommand, self).__init__(method="GET")
        self._tag = tag
        self._database_name = database_name

    def create_request(self, server_node):
        if self._database_name is None:
            self.url = "{0}/info/tcp?tag={1}".format(server_node.url, self._tag)
        else:
            self.url = "{0}/databases/{1}/info/tcp?tag={2}".format(server_node.url, self._database_name, self._tag)

        self.requested_node = server_node

    def set_response(self, response):
        if response is None:
            raise exceptions.ErrorResponseException("Invalid response")

        return response.json()


class QueryStreamCommand(RavenCommand):
    def __init__(self, index_query):
        super(QueryStreamCommand, self).__init__(method="POST", is_read_request=True, use_stream=True)
        if not index_query:
            raise ValueError("index_query cannot be None")

        if index_query.wait_for_non_stale_results:
            raise exceptions.NotSupportedException("Since stream() does not wait for indexing (by design), "
                                                   "streaming query with wait_for_non_stale_results is not supported.")

        self._index_query = index_query

    def create_request(self, server_node):
        self.url = "{0}/databases/{1}/streams/queries".format(server_node.url, server_node.database)
        self.data = self._index_query.to_json()

    def set_response(self, response):
        if response is None:
            raise exceptions.ErrorResponseException("Invalid response")
        if response.status_code != 200:
            try:
                json_response = response.json()
                if "Error" in json_response:
                    raise Exception(json_response["Error"])
                return response
            except ValueError:
                raise response.raise_for_status()
        return response


# ------------------------SubscriptionCommands----------------------

class CreateSubscriptionCommand(RavenCommand):
    def __init__(self, options):
        """
        @param SubscriptionCreationOptions options: Subscription options
        """

        super(CreateSubscriptionCommand, self).__init__(method="PUT", is_raft_request=True)
        self._options = options

    def create_request(self, server_node):
        self.url = "{0}/databases/{1}/subscriptions".format(server_node.url, server_node.database)
        self.data = self._options.to_json()

    def set_response(self, response):
        try:
            response = response.json()
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])
            return response["Name"]
        except:
            raise response.raise_for_status()


class DeleteSubscriptionCommand(RavenCommand):
    def __init__(self, name):
        super(DeleteSubscriptionCommand, self).__init__(method="DELETE", is_raft_request=True)
        self._name = name

    def create_request(self, server_node):
        self.url = "{0}/databases/{1}/subscriptions?taskName={2}".format(server_node.url, server_node.database,
                                                                         self._name)

    def set_response(self, response):
        pass


class DropSubscriptionConnectionCommand(RavenCommand):
    def __init__(self, name):
        super(DropSubscriptionConnectionCommand, self).__init__(method="POST", is_raft_request=True)
        self._name = name

    def create_request(self, server_node):
        self.url = "{0}/databases/{1}/subscriptions/drop?name={2}".format(server_node.url, server_node.database,
                                                                          self._name)

    def set_response(self, response):
        pass


class GetSubscriptionsCommand(RavenCommand):
    def __init__(self, start, page_size):
        super(GetSubscriptionsCommand, self).__init__(method="GET", is_read_request=True)
        self._start = start
        self._page_size = page_size

    def create_request(self, server_node):
        self.url = "{0}/databases/{1}/subscriptions?start={2}&pageSize={3}".format(
            server_node.url, server_node.database, self._start, self._page_size)

    def set_response(self, response):
        if response is None:
            raise ValueError("response is invalid.")
        inner_data = {}
        data = []
        try:
            response = response.json()
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])
            for item in response["Results"]:
                for key, value in item.items():
                    inner_data[Utils.convert_to_snake_case(key)] = value
                data.append(SubscriptionState(**inner_data))
            return data

        except ValueError:
            raise response.raise_for_status()


class GetSubscriptionStateCommand(RavenCommand):
    def __init__(self, subscription_name):
        super(GetSubscriptionStateCommand, self).__init__(method="GET", is_read_request=True)
        self._subscription_name = subscription_name

    def create_request(self, server_node):
        self.url = "{0}/databases/{1}/subscriptions/state?name={2}".format(
            server_node.url, server_node.database, self._subscription_name)

    def set_response(self, response):
        if response is None:
            raise ValueError("response is invalid.")
        data = {}
        try:
            response = response.json()
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])

            for key, value in response.items():
                data[Utils.convert_to_snake_case(key)] = value
            return SubscriptionState(**data)

        except ValueError:
            raise response.raise_for_status()
