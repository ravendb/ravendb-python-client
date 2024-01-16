import json
from typing import Optional, Union, TypeVar, Dict

import requests

from ravendb.documents.conventions import DocumentConventions
from ravendb.http.topology import RaftCommand
from ravendb.http.server_node import ServerNode
from ravendb.http.misc import Broadcast
from ravendb.http.raven_command import RavenCommand
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.util.util import RaftIdGenerator

_T_Result = TypeVar("_T_Result")


class NextIdentityForCommand(RavenCommand, RaftCommand, Broadcast):
    def __init__(self, _id: str):
        super().__init__(int)
        if _id is None:
            raise ValueError("Id cannot be None")

        self._id = _id
        self._raft_unique_request_id = RaftIdGenerator.new_id()

    @classmethod
    def from_copy(cls, copy: RavenCommand[_T_Result]) -> Union[Broadcast, RavenCommand[_T_Result]]:
        copied = super().from_copy(copy)

        copied._raft_unique_request_id = copy._raft_unique_request_id
        copied.id_ = copy._id
        return copied

    def is_read_request(self) -> bool:
        return False

    def create_request(self, node: ServerNode) -> requests.Request:
        self.ensure_is_not_null_or_string(self._id, "id")

        return requests.Request("POST", f"{node.url}/databases/{node.database}/identity/next?name={self._id}")

    def set_response(self, response: Optional[str], from_cache: bool) -> None:
        if response is None:
            self._throw_invalid_response()

        json_node = json.loads(response)
        if "NewIdentityValue" not in json_node:
            self._throw_invalid_response()

        self.result = json_node["NewIdentityValue"]

    def get_raft_unique_request_id(self) -> str:
        return self._raft_unique_request_id

    def prepare_to_broadcast(self, conventions: DocumentConventions) -> Broadcast:
        return NextIdentityForCommand.from_copy(self)


class NextIdentityForOperation(MaintenanceOperation[int]):
    def __init__(self, name: str):
        if not name or name.isspace():
            raise ValueError("The field name cannot be None or whitespace.")
        self._identity_name = name

    def get_command(self, conventions: "DocumentConventions") -> "RavenCommand[int]":
        return NextIdentityForCommand(self._identity_name)


class SeedIdentityForCommand(RavenCommand[int], RaftCommand):
    def __init__(self, id_: str, value: int = None, forced: bool = False):
        super().__init__(int)
        if id_ is None:
            raise ValueError("Id cannot be None")

        self._id = id_
        self._value = value
        self._forced = forced

    def is_read_request(self) -> bool:
        return False

    def create_request(self, node: ServerNode) -> requests.Request:
        self.ensure_is_not_null_or_string(self._id, "id")

        url = f"{node.url}/databases/{node.database}/identity/seed?name={self._id}&value={self._value}"

        if self._forced:
            url += "&force=true"

        return requests.Request("POST", url)

    def set_response(self, response: Optional[str], from_cache: bool) -> None:
        if response is None:
            self._throw_invalid_response()

        json_node = json.loads(response)

        if "NewSeedValue" not in json_node:
            self._throw_invalid_response()

        self.result = json_node["NewSeedValue"]

    def get_raft_unique_request_id(self) -> str:
        return RaftIdGenerator.new_id()


class SeedIdentityForOperation(MaintenanceOperation[int]):
    def __init__(self, name: str, value: int, force_update: bool = False):
        super().__init__()
        if not name or name.isspace():
            raise ValueError("The field name cannot be None or whitespace")

        self._identity_name = name
        self._identity_value = value
        self._force_update = force_update

    def get_command(self, conventions: "DocumentConventions") -> "RavenCommand[_T]":
        return SeedIdentityForCommand(self._identity_name, self._identity_value, self._force_update)


class GetIdentitiesOperation(MaintenanceOperation[Dict[str, int]]):
    def get_command(self, conventions: "DocumentConventions") -> "RavenCommand[Dict[str, int]]":
        return self.GetIdentitiesCommand()

    class GetIdentitiesCommand(RavenCommand[Dict[str, int]]):
        def __init__(self):
            super().__init__(dict)

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/debug/identities"
            return requests.Request(method="GET", url=url)

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            self.result = json.loads(response)
