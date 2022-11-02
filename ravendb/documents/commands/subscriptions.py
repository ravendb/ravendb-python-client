from __future__ import annotations

import json
from datetime import timedelta
from typing import Dict, Optional, List

import requests

from ravendb.documents.subscriptions.state import SubscriptionState
from ravendb.http.topology import RaftCommand, ServerNode
from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.subscriptions.options import SubscriptionCreationOptions, SubscriptionUpdateOptions
from ravendb.http.raven_command import RavenCommand, VoidRavenCommand
from ravendb.tools.utils import Utils
from ravendb.util.util import RaftIdGenerator


class CreateSubscriptionResult:
    def __init__(self, name: str):
        self.name = name

    @classmethod
    def from_json(cls, json_dict: Dict) -> CreateSubscriptionResult:
        return CreateSubscriptionResult(json_dict["Name"])


class CreateSubscriptionCommand(RavenCommand[CreateSubscriptionResult], RaftCommand):
    def __init__(
        self, conventions: DocumentConventions, options: SubscriptionCreationOptions, key: Optional[str] = None
    ):
        super().__init__(CreateSubscriptionResult)
        if options is None:
            raise ValueError("Options cannot be None")
        self._options = options
        self._key = key

    def create_request(self, node: ServerNode) -> requests.Request:
        url = f"{node.url}/databases/{node.database}/subscriptions"

        if self._key is not None:
            url += "?id" + Utils.quote_key(self._key)

        request = requests.Request("PUT")
        request.data = self._options.to_json()
        request.url = url
        return request

    def set_response(self, response: Optional[str], from_cache: bool) -> None:
        self.result = CreateSubscriptionResult.from_json(json.loads(response))

    def is_read_request(self) -> bool:
        return False

    def get_raft_unique_request_id(self) -> str:
        return RaftIdGenerator.new_id()


class TcpConnectionInfo:
    def __init__(
        self,
        port: Optional[int] = None,
        url: Optional[str] = None,
        certificate: Optional[str] = None,
        urls: Optional[List[str]] = None,
        node_tag: Optional[str] = None,
    ):
        self.port = port
        self.url = url
        self.certificate = certificate
        self.urls = urls
        self.node_tag = node_tag

    @classmethod
    def from_json(cls, json_dict: Dict) -> TcpConnectionInfo:
        return TcpConnectionInfo(
            json_dict.get("Port", None),
            json_dict.get("Url", None),
            json_dict.get("Certificate", None),
            json_dict.get("Urls", None),
            json_dict.get("NodeTag", None),
        )


class GetTcpInfoForRemoteTaskCommand(RavenCommand[TcpConnectionInfo]):
    def __init__(self, tag: str, remote_database: str, remote_task: str, verify_database: bool = False):
        super(GetTcpInfoForRemoteTaskCommand, self).__init__(TcpConnectionInfo)
        if remote_database is None:
            raise ValueError("remote_database cannot be None")

        self._remote_database = remote_database

        if remote_task is None:
            raise ValueError("remote_task cannot be None")

        self._remote_task = remote_task
        self._tag = tag
        self._verify_database = verify_database
        self.timeout = timedelta(seconds=15)
        self._requested_node: Optional[ServerNode] = None

    def create_request(self, node: ServerNode) -> requests.Request:
        url = (
            f"{node.url}/info/remote-task/tcp?"
            f"database={self._url_encode(self._remote_database)}"
            f"&remote-task={self._url_encode(self._remote_task)}"
            f"&tag={self._url_encode(self._tag)}"
        )

        if self._verify_database:
            url += "&verify-database=true"

        self._requested_node = node
        return requests.Request("GET", url)

    def set_response(self, response: Optional[str], from_cache: bool) -> None:
        if response is None:
            self._throw_invalid_response()

        self.result = TcpConnectionInfo.from_json(json.loads(response))

    @property
    def requested_node(self) -> ServerNode:
        return self._requested_node

    def is_read_request(self) -> bool:
        return True


class GetSubscriptionsResult:
    def __init__(self, results: Optional[List[SubscriptionState]] = None):
        self.results = results

    @classmethod
    def from_json(cls, json_dict: Dict) -> GetSubscriptionsResult:
        return cls([SubscriptionState.from_json(result) for result in json_dict["Results"]])


class GetSubscriptionsCommand(RavenCommand[List[SubscriptionState]]):
    def __init__(self, start: int, page_size: int):
        super(GetSubscriptionsCommand, self).__init__(list)
        self._start = start
        self._page_size = page_size

    def create_request(self, node: ServerNode) -> requests.Request:
        url = f"{node.url}/databases/{node.database}/subscriptions?start={self._start}&pageSize={self._page_size}"

        return requests.Request("GET", url)

    def set_response(self, response: Optional[str], from_cache: bool) -> None:
        if response is None:
            self.result = None
            return

        self.result = GetSubscriptionsResult.from_json(json.loads(response)).results

    def is_read_request(self) -> bool:
        return True


class DeleteSubscriptionCommand(VoidRavenCommand, RaftCommand):
    def __init__(self, name: str):
        super(DeleteSubscriptionCommand, self).__init__()
        self._name = name

    def create_request(self, node: ServerNode) -> requests.Request:
        url = f"{node.url}/databases/{node.database}/subscriptions?taskName={self._name}"

        return requests.Request("DELETE", url)

    def get_raft_unique_request_id(self) -> str:
        return RaftIdGenerator.new_id()


class GetSubscriptionStateCommand(RavenCommand[SubscriptionState]):
    def __init__(self, subscription_name: str):
        super(GetSubscriptionStateCommand, self).__init__(SubscriptionState)
        self._subscription_name = subscription_name

    def is_read_request(self) -> bool:
        return True

    def create_request(self, node: ServerNode) -> requests.Request:
        url = (
            f"{node.url}/databases/{node.database}/subscriptions/state?name={Utils.quote_key(self._subscription_name)}"
        )

        return requests.Request("GET", url)

    def set_response(self, response: Optional[str], from_cache: bool) -> None:
        self.result = SubscriptionState.from_json(json.loads(response))


class DropSubscriptionConnectionCommand(VoidRavenCommand):
    def __init__(self, name: Optional[str] = None):
        super(DropSubscriptionConnectionCommand, self).__init__()
        self._name = name

    def create_request(self, node: ServerNode) -> requests.Request:
        path = []
        path.append(node.url)
        path.append("/databases/")
        path.append(node.database)
        path.append("/subscriptions/drop")

        if self._name and not self._name.isspace():
            path.append("?name=")
            path.append(Utils.quote_key(self._name))

        url = "".join(path)
        return requests.Request("POST", url)


class UpdateSubscriptionResult(CreateSubscriptionResult):
    pass


class UpdateSubscriptionCommand(RavenCommand[UpdateSubscriptionResult], RaftCommand):
    def __init__(self, options: SubscriptionUpdateOptions):
        super(UpdateSubscriptionCommand, self).__init__(UpdateSubscriptionResult)
        self._options = options

    def create_request(self, node: ServerNode) -> requests.Request:
        url = f"{node.url}/databases/{node.database}/subscriptions/update"

        request = requests.Request("POST", url)
        request.data = self._options.to_json()

        return request

    def set_response(self, response: Optional[str], from_cache: bool) -> None:
        if from_cache:
            self.result = UpdateSubscriptionResult(self._options.name)
            return

        if response is None:
            self._throw_invalid_response()

        self.result = UpdateSubscriptionResult.from_json(json.loads(response))

    def is_read_request(self) -> bool:
        return False

    def get_raft_unique_request_id(self) -> str:
        return RaftIdGenerator.new_id()
