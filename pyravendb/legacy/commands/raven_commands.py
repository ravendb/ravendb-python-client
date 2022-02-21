# -*- coding: utf-8 -*- #
from builtins import ValueError
from pyravendb.legacy.subscriptions.data import SubscriptionState
from pyravendb.exceptions import exceptions
from pyravendb.tools.utils import Utils
from abc import abstractmethod
from datetime import timedelta
import logging
import uuid

logging.basicConfig(filename="responses.log", level=logging.DEBUG)
log = logging.getLogger()


# Todo update the commands
class OldRavenCommand(object):
    def __init__(
        self,
        url=None,
        method=None,
        data=None,
        files=None,
        headers=None,
        is_read_request=False,
        is_raft_request=False,
        use_stream=False,
    ):
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


class GetFacetsCommandOld(OldRavenCommand):
    def __init__(self, query):
        """
        @param FacetQuery query: The query we wish to get
        """
        if not query:
            raise ValueError("Invalid query")
        super(GetFacetsCommandOld, self).__init__(method="POST", is_read_request=True)
        self._query = query
        if query.wait_for_non_stale_results_timeout and query.wait_for_non_stale_results_timeout != timedelta.max:
            self.timeout = self._query.wait_for_non_stale_results_timeout + timedelta(seconds=10)

    def create_request(self, server_node):
        if self._query.facet_setup_doc and len(self._query.facets) > 0:
            raise exceptions.InvalidOperationException("You cannot specify both 'facet_setup_doc' and 'facets'.")

        self.url = "{0}/databases/{1}/queries?op=facets&query-hash={2}".format(
            server_node.url, server_node.database, self._query.get_query_hash()
        )
        self.data = self._query.to_json()

    def set_response(self, response):
        if response.status_code == 200:
            return response.json()


class WaitForRaftIndexCommandOld(OldRavenCommand):
    def __init__(self, index):
        super(WaitForRaftIndexCommandOld, self).__init__(method="GET", is_read_request=True)
        self._index = index

    def create_request(self, server_node):
        self.url = "{0}/rachis/waitfor?index={1}".format(server_node.url, self._index)

    def set_response(self, response):
        if response is None:
            raise exceptions.ErrorResponseException("Invalid response")


class GetTcpInfoCommandOld(OldRavenCommand):
    def __init__(self, tag, database_name=None):
        super(GetTcpInfoCommandOld, self).__init__(method="GET")
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


class QueryStreamCommandOld(OldRavenCommand):
    def __init__(self, index_query):
        super(QueryStreamCommandOld, self).__init__(method="POST", is_read_request=True, use_stream=True)
        if not index_query:
            raise ValueError("index_query cannot be None")

        if index_query._wait_for_non_stale_results:
            raise exceptions.NotSupportedException(
                "Since stream() does not wait for indexing (by design), "
                "streaming query with wait_for_non_stale_results is not supported."
            )

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


class CreateSubscriptionCommandOld(OldRavenCommand):
    def __init__(self, options):
        """
        @param SubscriptionCreationOptions options: Subscription options
        """

        super(CreateSubscriptionCommandOld, self).__init__(method="PUT", is_raft_request=True)
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


class DeleteSubscriptionCommandOld(OldRavenCommand):
    def __init__(self, name):
        super(DeleteSubscriptionCommandOld, self).__init__(method="DELETE", is_raft_request=True)
        self._name = name

    def create_request(self, server_node):
        self.url = "{0}/databases/{1}/subscriptions?taskName={2}".format(
            server_node.url, server_node.database, self._name
        )

    def set_response(self, response):
        pass


class DropSubscriptionConnectionCommandOld(OldRavenCommand):
    def __init__(self, name):
        super(DropSubscriptionConnectionCommandOld, self).__init__(method="POST", is_raft_request=True)
        self._name = name

    def create_request(self, server_node):
        self.url = "{0}/databases/{1}/subscriptions/drop?name={2}".format(
            server_node.url, server_node.database, self._name
        )

    def set_response(self, response):
        pass


class GetSubscriptionsCommandOld(OldRavenCommand):
    def __init__(self, start, page_size):
        super(GetSubscriptionsCommandOld, self).__init__(method="GET", is_read_request=True)
        self._start = start
        self._page_size = page_size

    def create_request(self, server_node):
        self.url = "{0}/databases/{1}/subscriptions?start={2}&pageSize={3}".format(
            server_node.url, server_node.database, self._start, self._page_size
        )

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


class GetSubscriptionStateCommandOld(OldRavenCommand):
    def __init__(self, subscription_name):
        super(GetSubscriptionStateCommandOld, self).__init__(method="GET", is_read_request=True)
        self._subscription_name = subscription_name

    def create_request(self, server_node):
        self.url = "{0}/databases/{1}/subscriptions/state?name={2}".format(
            server_node.url, server_node.database, self._subscription_name
        )

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
