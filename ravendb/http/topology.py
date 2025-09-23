from __future__ import annotations

import datetime
import threading
import time
import uuid
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Optional
from typing import Union, List, Dict

from ravendb.exceptions.exceptions import (
    DatabaseDoesNotExistException,
    RequestedNodeUnavailableException,
    AllTopologyNodesDownException,
)
from ravendb.http.server_node import ServerNode

if TYPE_CHECKING:
    pass


class Topology:
    def __init__(self, etag: int, nodes: List[ServerNode]):
        self.etag = etag
        self.nodes = nodes


class ClusterTopology:
    def __init__(self):
        self.last_node_id = None
        self.topology_id = None
        self.etag = None

        self.members: Dict[str, str] = {}
        self.promotables: Dict[str, str] = {}
        self.watchers: Dict[str, str] = {}

    def __contains__(self, node: str) -> bool:
        if self.members is not None and node in self.members:
            return True
        if self.promotables is not None and node in self.promotables:
            return True

        return self.watchers is not None and node in self.watchers

    @classmethod
    def from_json(cls, json_dict: Dict) -> ClusterTopology:
        topology = cls()
        topology.etag = json_dict["Etag"]
        topology.topology_id = json_dict["TopologyId"]
        topology.members = json_dict["Members"]
        topology.promotables = json_dict["Promotables"]
        topology.watchers = json_dict["Watchers"]
        topology.last_node_id = json_dict["LastNodeId"]
        return topology


class UpdateTopologyParameters:
    def __init__(self, node: ServerNode):
        if not node:
            raise ValueError("Node cannot be None")
        self.node = node

        self.timeout_in_ms = 15_000
        self.force_update: Union[None, bool] = None
        self.debug_tag: Union[None, str] = None
        self.application_identifier: Union[None, uuid.UUID] = None


class NodeSelector:
    class __NodeSelectorState:
        def __init__(self, topology: Topology):
            self.topology = topology
            self.nodes = topology.nodes
            self.failures = [0] * len(topology.nodes)
            self.fastest_records = [0] * len(topology.nodes)
            self.fastest: Union[None, int] = None
            self.speed_test_mode = 0
            self.unlikely_everyone_faulted_choice_index: Optional[int] = 0

        @property
        def node_when_everyone_marked_as_faulted(self) -> CurrentIndexAndNode:
            index = self.unlikely_everyone_faulted_choice_index
            self.unlikely_everyone_faulted_choice_index = (self.unlikely_everyone_faulted_choice_index + 1) % len(
                self.nodes
            )

            return CurrentIndexAndNode(index, self.nodes[index])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__update_fastest_node_timer is not None:
            self.__update_fastest_node_timer.__exit__()

    def __init__(self, topology: Topology, thread_pool: ThreadPoolExecutor):
        self._state = self.__NodeSelectorState(topology)
        self.__update_fastest_node_timer: Union[None, time] = None
        self.__thread_pool_executor = thread_pool

    @property
    def topology(self) -> Topology:
        return self._state.topology

    def node_is_available(self, index: int) -> bool:
        return self._state.failures[index] == 0

    def on_failed_request(self, node_index: int) -> None:
        state = self._state
        if node_index < 0 or node_index >= len(state.failures):
            return
        state.failures[node_index] += 1

    def on_update_topology(self, topology: Topology, force_update: bool = False) -> bool:
        if topology is None:
            return False

        state_etag = self._state.topology.etag if self._state.topology.etag else 0
        topology_etag = topology.etag if topology.etag else 0

        if state_etag >= topology_etag and not force_update:
            return False

        state = NodeSelector.__NodeSelectorState(topology)
        self._state = state

        return True

    def get_requested_node(self, node_tag: str) -> CurrentIndexAndNode:
        state = self._state
        server_nodes = state.nodes
        for i in range(len(server_nodes)):
            if server_nodes[i].cluster_tag == node_tag:
                return CurrentIndexAndNode(i, server_nodes[i])

        if len(state.nodes) == 0:
            raise DatabaseDoesNotExistException("There are no nodes in the topology at all")
        raise RequestedNodeUnavailableException(f"Could not find requested node {node_tag}")

    def get_preferred_node(self) -> CurrentIndexAndNode:
        state = self._state
        return self.get_preferred_node_internal(state)

    @classmethod
    def get_preferred_node_internal(cls, state: NodeSelector.__NodeSelectorState) -> CurrentIndexAndNode:
        state_failures = state.failures
        server_nodes = state.nodes
        length = min(len(server_nodes), len(state_failures))
        for i in range(length):
            # pick the first node without failures
            if state_failures[i] == 0 and server_nodes[i].server_role == ServerNode.Role.MEMBER:
                return CurrentIndexAndNode(i, server_nodes[i])
        return cls.unlikely_everyone_faulted_choice(state)

    def get_preferred_node_with_topology(self) -> CurrentIndexAndNodeAndEtag:
        state = self._state
        preferred_node = self.get_preferred_node_internal(state)
        etag = (state.topology.etag if state.topology.etag else -2) if state.topology else -2
        return CurrentIndexAndNodeAndEtag(preferred_node.current_index, preferred_node.current_node, etag)

    @staticmethod
    def unlikely_everyone_faulted_choice(state: NodeSelector.__NodeSelectorState) -> CurrentIndexAndNode:
        # if there are all marked as failed, we'll chose the next (the one in CurrentNodeIndex)
        # one so the user will get an error (or recover :-) )
        if len(state.nodes) == 0:
            raise DatabaseDoesNotExistException("There are no nodes in the topology at all")
        return state.node_when_everyone_marked_as_faulted

    def get_node_by_session_id(self, session_id: int) -> CurrentIndexAndNode:
        state = self._state
        if len(state.topology.nodes) == 0:
            raise AllTopologyNodesDownException("There are no nodes in the topology at all")
        index = abs(session_id % len(state.topology.nodes))
        for i in range(index, len(state.failures)):
            if state.failures[i] == 0 and state.nodes[i].server_role == ServerNode.Role.MEMBER:
                return CurrentIndexAndNode(i, state.nodes[i])

        for i in range(index):
            if state.failures[i] == 0 and state.nodes[i].server_role == ServerNode.Role.MEMBER:
                return CurrentIndexAndNode(i, state.nodes[i])

        return self.get_preferred_node()

    def get_fastest_node(self) -> CurrentIndexAndNode:
        state = self._state
        if state.failures[state.fastest] == 0 and state.nodes[state.fastest].server_role == ServerNode.Role.MEMBER:
            return CurrentIndexAndNode(state.fastest, state.nodes[state.fastest])

        # if the fastest node has failures, we'll immediately schedule
        # another run of finding who the fastest node is, in the mantime
        # we'll just use the server preferred node or failover as usual

        self._switch_to_speed_test_phase()
        return self.get_preferred_node()

    def restore_node_index(self, node_index: int) -> None:
        state = self._state
        if len(state.failures) <= node_index:
            return

        state.failures[node_index] = 0

    @staticmethod
    def _throw_empty_topology() -> None:
        raise RuntimeError("Empty database topology, this shouldn't happen.")

    def _switch_to_speed_test_phase(self) -> None:
        state = self._state

        if not state.speed_test_mode == 0:
            state.speed_test_mode = 1
            return

        state.fastest_records = len(state.fastest_records) * [0]

        state.speed_test_mode += 1

    @property
    def in_speed_test_phase(self) -> bool:
        return self._state.speed_test_mode > 1

    def record_fastest(self, index: int, node: ServerNode) -> None:
        state = self._state
        state_fastest = state.fastest_records

        # the following two checks are to verify that things didn't move
        # while we were computing the fastest node, we verify that the index
        # of the fastest node and rhe identity of the node didn't change during
        # our check

        if index < 0 or index >= len(state_fastest):
            return

        if node != state.nodes[index]:
            return

        state_fastest[index] += 1
        if state_fastest[index] >= 10:
            self.__select_fastest(state, index)

        state.speed_test_mode += 1

        if state.speed_test_mode <= len(state.nodes) * 10:
            return

        # too many concurrent speed tests are happening
        max_index = self.__find_max_index(state)
        self.__select_fastest(state, max_index)

    @staticmethod
    def __find_max_index(state: NodeSelector.__NodeSelectorState) -> int:
        state_fastest = state.fastest_records
        max_index = 0
        max_value = 0
        for i in range(len(state_fastest)):
            if max_value >= state_fastest[i]:
                continue

            max_index = i
            max_value = state_fastest[i]

        return max_index

    def __select_fastest(self, state: NodeSelector.__NodeSelectorState, index: int) -> None:
        state.fastest = index
        state.speed_test_mode = 0

        if self.__update_fastest_node_timer is not None:
            # todo: figure out how to reshedule timer, or put it in sleep for 60s
            threading.self.__update_fastest_node_timer

        else:
            self.__update_fastest_node_timer = threading.Timer(
                datetime.timedelta(minutes=1), self._switch_to_speed_test_phase
            )

    def schedule_speed_test(self) -> None:
        self._switch_to_speed_test_phase()


class CurrentIndexAndNode:
    def __init__(self, current_index: int, current_node: ServerNode):
        self.current_index = current_index
        self.current_node = current_node


class CurrentIndexAndNodeAndEtag:
    def __init__(self, current_index: int, current_node: ServerNode, etag: int):
        self.current_index = current_index
        self.current_node = current_node
        self.etag = etag


class NodeStatus:
    def __init__(
        self,
        name: str,
        connected: bool,
        error_details: str,
        last_send: datetime.datetime,
        last_reply: datetime.datetime,
        last_sent_message: str,
        last_matching_index: int,
    ):
        self.name = name
        self.connected = connected
        self.error_details = error_details
        self.last_send = last_send
        self.last_reply = last_reply
        self.last_sent_message = last_sent_message
        self.last_matching_index = last_matching_index

    def __str__(self):
        return (
            "NodeStatus{"
            f"name='{self.name}'"
            f", connected={self.connected}"
            f", errorDetails={self.error_details}"
            f", lastSend={self.last_send}"
            f", lastReply={self.last_reply}"
            f", lastSentMessage={self.last_sent_message}"
            "}"
        )


class RaftCommand:
    @abstractmethod
    def get_raft_unique_request_id(self) -> str:
        pass
