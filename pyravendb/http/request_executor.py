from copy import copy

import requests
import logging
import json
import os
import uuid
from asyncio import Task, Future
import requests

from copy import copy
from typing import Union, Callable, Any, Awaitable

from pyravendb import constants
from pyravendb.custom_exceptions.exceptions import AllTopologyNodesDownException, UnsuccessfulRequestException
from pyravendb.data.document_conventions import DocumentConventions
from pyravendb.documents.operations.configuration.configuration import GetClientConfigurationOperation
from pyravendb.documents.session.session import SessionInfo
from pyravendb.exceptions.exceptions import ExceptionDispatcher, ClientVersionMismatchException
from pyravendb.http.http import (
    CurrentIndexAndNode,
    NodeSelector,
    Broadcast,
    Topology,
    ReadBalanceBehavior,
    UpdateTopologyParameters,
    ResponseDisposeHandling,
)
from pyravendb.http.http_cache import HttpCache
from pyravendb.http.raven_command import RavenCommand, RavenCommandResponseType
from pyravendb.http.server_node import ServerNode
from pyravendb.serverwide.commands import GetDatabaseTopologyCommand

from http import HTTPStatus


class RequestExecutor:
    __INITIAL_TOPOLOGY_ETAG = -2
    __GLOBAL_APPLICATION_IDENTIFIER = uuid.uuid4()
    CLIENT_VERSION = "5.0.0"

    def __init__(self, database_name: str, conventions: DocumentConventions):
        self.conventions = copy(conventions)
        self._node_selector: NodeSelector = None
        self.__default_timeout: datetime.timedelta = conventions.request_timeout
        self.__cache: HttpCache = HttpCache()

        self.__topology_taken_from_node: Union[None, ServerNode] = None

        self.__update_client_configuration_semaphore = asyncio.Semaphore(1)
        self.__update_database_topology_semaphore = asyncio.Semaphore(1)

        self.__database_name = database_name

        self.number_of_server_requests = 0

        self._topology_etag: Union[None, int] = None
        self._client_configuration_etag: Union[None, int] = None
        self._disable_topology_updates: Union[None, bool] = None
        self._disable_client_configuration_updates: Union[None, bool] = None
        self._last_server_version: Union[None, str] = None

        self.__http_session: Union[None, requests.Session] = None

        self.first_broadcast_attempt_timeout: Union[None, datetime.timedelta] = None
        self.second_broadcast_attempt_timeout: Union[None, datetime.timedelta] = None

        self._disposed: Union[None, bool] = None

        # --- events ---
        self.__on_before_request: list[Callable[[str, str, requests.Request, int], Any]] = []
        self.__on_failed_request: list[Callable[[str, str, BaseException], None]] = []

        self._on_topology_updated: list[Callable[[Topology], None]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self._disposed:
            return

        self._disposed = True
        self.__cache.close()

        if self.__update_topology_timer:
            self.__update_topology_timer.close()

        self._dispose_all_failed_nodes_timers()

    @property
    def url(self) -> Union[None, str]:
        if self._node_selector is None:
            return None

        preferred_node = self._node_selector.get_preferred_node()

        return preferred_node.current_node.url if preferred_node is not None else None

    @property
    def last_server_version(self):
        return self._last_server_version

    @property
    def topology_etag(self) -> int:
        return self._topology_etag

    @property
    def topology(self) -> Topology:
        return self._node_selector.topology if self._node_selector else None

    @property
    def http_session(self):
        http_session = self.__http_session
        if http_session:
            return http_session
        self.__http_session = self.__create_http_session()
        return self.__http_session

    def __create_http_session(self) -> requests.Session:
        # todo: check if http client name is required
        return requests.session()

    @property
    def topology_nodes(self) -> list[ServerNode]:
        return self.topology.nodes

    @property
    def client_configuration_etag(self) -> int:
        return self._client_configuration_etag

    @property
    def default_timeout(self) -> datetime.timedelta:
        return self.__default_timeout

    @default_timeout.setter
    def default_timeout(self, value: datetime.timedelta) -> None:
        self.__default_timeout = value

    def __on_failed_request_invoke(self, url: str, e: BaseException):
        for event in self.__on_failed_request:
            event(self.__database_name, url, e)

    def _on_topology_updated_invoke(self, topology: Topology) -> None:
        for event in self._on_topology_updated:
            event(topology)

    def _update_client_configuration_async(self, server_node: ServerNode) -> Task:
        if self._disposed:
            task = asyncio.create_task(asyncio.sleep(0))
            task.set_result(None)
            return task

        async def __run_async():
            try:
                await self.__update_client_configuration_semaphore.acquire()
            except InterruptedError as e:
                raise RuntimeError(e)

            old_disable_client_configuration_updates = self._disable_client_configuration_updates
            self._disable_client_configuration_updates = True

            try:
                if self._disposed:
                    return

                command = GetClientConfigurationOperation.GetClientConfigurationCommand()
                self.execute(server_node, None, command, False, None)

                result = command.result
                if not result:
                    return

                self.conventions.update_from(result.configuration)
                self.client_configuration_etag = result.etag
            finally:
                self._disable_client_configuration_updates = old_disable_client_configuration_updates
                self.__update_client_configuration_semaphore.release()

        asyncio.run(__run_async())

    def update_topology_async(self, parameters: UpdateTopologyParameters):
        if not parameters:
            raise ValueError("Parameters cannot be None")

        if self._disable_topology_updates:
            return asyncio.create_task(asyncio.sleep(0)).set_result(False)

        if self._disposed:
            return asyncio.create_task(asyncio.sleep(0)).set_result(False)

        async def __supply_async():
            # prevent double topology updates if execution takes too much time
            # --> in cases with transient issues
            try:
                await asyncio.sleep(0)
                lock_taken = asyncio.wait_for(
                    self.__update_database_topology_semaphore.acquire(), parameters.timeout_in_ms * 1000
                )
                if not lock_taken:
                    return False
            except RuntimeError:
                raise

            try:
                if self._disposed:
                    return False

                command = GetDatabaseTopologyCommand(
                    parameters.debug_tag,
                    parameters.application_identifier if self.conventions.send_application_identifier else None,
                )
                self.execute(parameters.node, None, command, False, None)
                topology = command.result

                if self._node_selector == None:
                    self._node_selector = NodeSelector(topology)

                    if self.conventions.read_balance_behavior == ReadBalanceBehavior.FASTEST_NODE:
                        self._node_selector.schedule_speed_test()

                elif self._node_selector.on_update_topology(topology, parameters.force_update):
                    self.dispose_all_failed_nodes_timers()
                    if self.conventions.read_balance_behavior == ReadBalanceBehavior.FASTEST_NODE:
                        self._node_selector.schedule_speed_test()

                self._topology_etag = self._node_selector.topology.etag

                self._on_topology_updated_invoke(topology)
            except Exception as e:
                if not self._disposed:
                    raise
            finally:
                self.__update_database_topology_semaphore.release()
            return True

        return asyncio.create_task(__supply_async())

    def execute(
        self,
        chosen_node: ServerNode = None,
        node_index: int = None,
        command: RavenCommand = None,
        should_retry: bool = None,
        session_info: SessionInfo = None,
        ret_request: bool = False,
    ) -> Union[None, requests.Request]:
        if command.failover_topology_etag == self.INITIAL_TOPOLOGY_ETAG:
            command.failover_topology_etag = self.INITIAL_TOPOLOGY_ETAG
            if self._node_selector and self._node_selector.topology:
                topology = self._node_selector.topology
                if topology.etag:
                    command.failover_topology_etag = topology.etag

        request, url = self.__create_request(chosen_node, command)

        if not request:
            return

        if ret_request:
            request_ref = request

        if not request:
            return

        no_caching = session_info.no_caching if session_info else False

        cached_item, change_vector, cached_value = self.__get_from_cache(command, not no_caching, url)
        # todo: if change_vector exists try get from cache - aggressive caching
        with cached_item as cached_item:
            # todo: try get from cache
            self.__set_request_headers(session_info, change_vector, request)

            command.number_of_attempts = command.number_of_attempts + 1
            attempt_num = command.number_of_attempts
            for func in self.__on_before_request:
                func(self.__database_name, url, request, attempt_num)

            response = self.__send_request_to_server(
                chosen_node, node_index, command, should_retry, session_info, request, url
            )

            if not response:
                return

            refresh_task = self.__refresh_if_needed(chosen_node, response)

            command.status_code = response.status_code
            response_dispose = ResponseDisposeHandling.AUTOMATIC

            try:
                if response.status_code == HTTPStatus.NOT_MODIFIED:
                    ...
                if response.status_code >= 400:
                    # if not self.__handle_unsuccessful_response(chosen_node,node_index,command,request,url,session_info,should_retry):
                    #  pass
                    pass
            finally:
                if response_dispose == ResponseDisposeHandling.AUTOMATIC:
                    response.close()

                try:
                    refresh_task.result()
                except:
                    raise

    def __refresh_if_needed(self, chosen_node: ServerNode, response: requests.Response) -> asyncio.Future:
        refresh_topology = response.headers.get(constants.Headers.REFRESH_TOPOLOGY, False)
        refresh_client_configuration = response.headers.get(constants.Headers.REFRESH_CLIENT_CONFIGURATION, False)

        if refresh_topology or refresh_client_configuration:
            server_node = ServerNode(chosen_node.url, self.__database_name)

            update_parameters = UpdateTopologyParameters(server_node)
            update_parameters.timeout_in_ms = 0
            update_parameters.debug_tag = "refresh-topology-header"

            if refresh_topology:
                topology_task = self.update_topology_async(update_parameters)
            else:
                topology_task = asyncio.create_task(asyncio.sleep(0))
                topology_task.set_result(False)

            if refresh_client_configuration:
                client_configuration = self._update_client_configuration_async(server_node)
            else:
                client_configuration = asyncio.create_task(asyncio.sleep(0))
                client_configuration.set_result(None)

            return asyncio.gather(topology_task, client_configuration)
        return asyncio.create_task(asyncio.sleep(0))

    def __send_request_to_server(
        self,
        chosen_node: ServerNode,
        node_index: int,
        command: RavenCommand,
        should_retry: bool,
        session_info: SessionInfo,
        request: requests.Request,
        url: str,
    ) -> requests.Response:
        try:
            self.number_of_server_requests += 1
            timeout = command.timeout if command.timeout else self.__default_timeout
            if timeout:
                try:
                    # todo: create Task from lines below and call it
                    # AggressiveCacheOptions callingTheadAggressiveCaching = aggressiveCaching.get();
                    # CompletableFuture<CloseableHttpResponse> sendTask = CompletableFuture.supplyAsync(() ->
                    # AggressiveCacheOptions aggressiveCacheOptionsToRestore = aggressiveCaching.get();
                    try:
                        return self.__send(chosen_node, command, session_info, request)
                    except IOError:
                        # throw ExceptionsUtils.unwrapException(e);
                        raise
                    # finally aggressiveCaching.set(aggressiveCacheOptionsToRestore);
                except requests.Timeout as t:
                    # request.abort()
                    # net.ravendb.client.exceptions.TimeoutException timeoutException =
                    # new net.ravendb.client.exceptions.TimeoutException(
                    # "The request for " + request.getURI() + " failed with timeout after " +
                    # TimeUtils.durationToTimeSpan(timeout), e);

                    if not should_retry:
                        if command.failed_nodes is None:
                            command.failed_nodes = {}

                        command.failed_nodes[chosen_node] = t
                        raise t

                    if not self.__handle_server_down(
                        url, chosen_node, node_index, command, request, None, t, session_info, should_retry
                    ):
                        self.__throw_failed_to_contact_all_nodes(command, request)

                    return None
                except IOError as e:
                    raise e
            else:
                return self.__send(chosen_node, command, session_info, request)
        except IOError as e:
            if not should_retry:
                raise

            if not self.__handle_server_down(
                url, chosen_node, node_index, command, request, None, e, session_info, should_retry
            ):
                self.__throw_failed_to_contact_all_nodes(command, request)

            return None

    def __send(
        self, chosen_node: ServerNode, command: RavenCommand, session_info: SessionInfo, request: requests.Request
    ) -> requests.Response:
        response: requests.Response = None

        if self.should_execute_on_all(chosen_node, command):
            response = self.__execute_on_all_to_figure_out_the_fastest(chosen_node, command)
        else:
            response = command.send(self.http_session, request)

        # PERF: The reason to avoid rechecking every time is that servers wont change so rapidly
        #       and therefore we dimish its cost by orders of magnitude just doing it
        #       once in a while. We dont care also about the potential race conditions that may happen
        #       here mainly because the idea is to have a lax mechanism to recheck that is at least
        #       orders of magnitude faster than currently.
        if chosen_node.should_update_server_version():
            server_version = self.__try_get_server_version(response)
            if server_version is not None:
                chosen_node.update_server_version(server_version)

        self._last_server_version = chosen_node.last_server_version

        if session_info and session_info.last_cluster_transaction_index:
            # if we reach here it means that sometime a cluster transaction has occurred against this database.
            # Since the current executed command can be dependent on that, we have to wait for the cluster transaction.
            # But we can't do that if the server is an old one.

            if not self._last_server_version or self._last_server_version.lower() < "4.1":
                raise ClientVersionMismatchException(
                    f"The server on {chosen_node.url} has an old version and"
                    f" can't perform the command since this command dependent on"
                    f" a cluster transaction which this node doesn't support"
                )

        return response

    def choose_node_for_request(self, cmd: RavenCommand, session_info: SessionInfo) -> CurrentIndexAndNode:
        # When we disable topology updates we cannot rely on the node tag,
        # Because the initial topology will not have them
        if cmd.selected_node_tag and not cmd.selected_node_tag.isspace():
            return self._node_selector.get_requested_node(cmd.selected_node_tag)

    def __set_request_headers(
        self, session_info: SessionInfo, cached_change_vector: Union[None, str], request: requests.Request
    ) -> None:
        if cached_change_vector is not None:
            request.headers.IF_NONE_MATCH = f'"{cached_change_vector}"'

        if not self._disable_client_configuration_updates:
            request.headers.CLIENT_CONFIGURATION_ETAG = f'"{self._client_configuration_etag}"'

        if session_info and session_info.last_cluster_transaction_index:
            request.headers.LAST_KNOWN_CLUSTER_TRANSACTION_INDEX = str(session_info.last_cluster_transaction_index)

        if not self._disable_topology_updates:
            request.headers.TOPOLOGY_ETAG = f'"{self._topology_etag}"'

        if not request.headers.CLIENT_VERSION:
            request.headers.CLIENT_VERSION = RequestExecutor.CLIENT_VERSION

    def __get_from_cache(
        self, command: RavenCommand, use_cache: bool, url: str
    ) -> (HttpCache.ReleaseCacheItem, str, str):
        if (
            use_cache
            and command.can_cache
            and command.is_read_request()
            and command.response_type == RavenCommandResponseType.OBJECT
        ):
            return self.__cache.get(url)

        return HttpCache.ReleaseCacheItem(), None, None

    @staticmethod
    def __try_get_server_version(response: requests.Response) -> Union[None, str]:
        server_version_header = response.headers.get(constants.Headers.SERVER_VERSION)

        if server_version_header is not None:
            return server_version_header

        return None

    def __throw_failed_to_contact_all_nodes(self, command: RavenCommand, request: requests.Request):
        if not command.failed_nodes:
            raise RuntimeError(
                "Received unsuccessful response and couldn't recover from it. "
                "Also, no record of exceptions per failed nodes. This is weird and should not happen."
            )

        if len(command.failed_nodes) == 1:
            raise command.failed_nodes.popitem()

        message = (
            f"Tried to send {command._result_class.__name__} request via {request.method}"
            f" {request.url} to all configured nodes in the topology, none of the attempt succeeded.{os.linesep}"
        )

        if self.__topology_taken_from_node is not None:
            message += (
                f"I was able to fetch {self.__topology_taken_from_node.database}"
                f" topology from {self.__topology_taken_from_node.url}.{os.linesep}"
            )

        nodes = None

        if self._node_selector and self._node_selector.topology:
            nodes = self._node_selector.topology.nodes

        if nodes is None:
            message += "Topology is empty."
        else:
            message += "Topology: "

            for node in nodes:
                exception = command.failed_nodes.get(node)
                message += (
                    f"{os.linesep}"
                    f"[Url: {node.url}, "
                    f"ClusterTag: {node.cluster_tag}, "
                    f"Exception: {exception.args[0] if exception else 'No exception'}]"
                )

        raise AllTopologyNodesDownException(message)

    def should_execute_on_all(self, chosen_node: ServerNode, command: RavenCommand) -> bool:
        return all(
            [
                self.conventions.read_balance_behavior == ReadBalanceBehavior.FASTEST_NODE,
                self._node_selector,
                self._node_selector.in_speed_test_phase,
                len(self._node_selector.topology.nodes) > 1,
                command.is_read_request(),
                command.response_type == RavenCommandResponseType.OBJECT,
                chosen_node is not None,
                not isinstance(command, Broadcast),
            ]
        )

    def __execute_on_all_to_figure_out_the_fastest(
        self, chosen_node: ServerNode, command: RavenCommand
    ) -> requests.Response:
        number_failed_tasks = [0]
        preferred_task: Awaitable = None

        nodes = self._node_selector.topology.nodes
        tasks: list = []

        for i in range(len(nodes)):
            task_number = i
            self.number_of_server_requests += 1

            async def task_fun(
                single_element_list_number_failed_tasks: list[int],
            ) -> (RequestExecutor.IndexAndResponse, int):
                try:
                    request, str_ref = self.__create_request(nodes[task_number], command)
                    self.__set_request_headers(None, None, request)
                    return self.IndexAndResponse(task_number, command.send(self.http_session, request))
                except Exception as e:
                    single_element_list_number_failed_tasks[0] += 1
                    tasks[task_number] = None
                    raise RuntimeError("Request execution failed", e)

            task = asyncio.create_task(task_fun(number_failed_tasks))
            if nodes[i].cluster_tag == chosen_node.cluster_tag:
                preferred_task: Task = task
            else:
                task.add_done_callback(lambda result: result.response.close(None, None, None))

            tasks[i] = task

        loop = asyncio.get_running_loop()
        while number_failed_tasks[0] < len(tasks):
            try:
                first_finished, slow = asyncio.wait(tasks, loop=loop, return_when=asyncio.FIRST_COMPLETED)
                fastest = first_finished.pop().result()
                fastest: RequestExecutor.IndexAndResponse
                self._node_selector.record_fastest(fastest.index, nodes[fastest.index])
                break
            except Exception:
                for i in range(len(nodes)):
                    if tasks[i].exception() is not None:
                        number_failed_tasks[0] += 1
                        tasks[i] = None

        # we can reach here if number of failed task equal to the number of the nodes,
        # in which case we have nothing to do

        return preferred_task.result().response

    def __create_request(self, node: ServerNode, command: RavenCommand) -> (requests.Request, str):
        request, request.url = command.create_request(node)
        # todo: 1117 - 1133
        return (request, request.url) if request else (None, None)

    def should_broadcast(self, command: RavenCommand) -> bool:
        if not isinstance(command, Broadcast):
            return False

        topology_nodes = self.topology_nodes
        if not topology_nodes:
            return False

        return True

    class BroadcastState:
        def __init__(self, command: RavenCommand, index: int, node: ServerNode, request: requests.Request):
            self.command = command
            self.index = index
            self.node = node
            self.request = request

    def __broadcast(self, command: RavenCommand, session_info: SessionInfo) -> object:
        if not isinstance(command, Broadcast):
            raise RuntimeError("You can broadcast only commands that inherit from Broadcast class.")
        command: Union[RavenCommand, Broadcast]
        failed_nodes = command.failed_nodes

        command.failed_nodes = {}
        broadcast_tasks: dict[Task, RequestExecutor.BroadcastState] = {}

        try:
            self.__send_to_all_nodes(broadcast_tasks, session_info, command)

            return self.__wait_for_broadcast_result(command, broadcast_tasks)
        finally:
            for task, broadcast_state in broadcast_tasks.items():
                if task:

                    def __exceptionally(self_task: Task):
                        if self_task.exception():
                            index = broadcast_state.index
                            node = self._node_selector.topology.nodes[index]
                            if node in failed_nodes:
                                # if other node succeed in broadcast we need to send health checks to
                                # the original failed node
                                pass
                                # todo: health check
                        return None

                    task.add_done_callback(__exceptionally)

    def __wait_for_broadcast_result(self, command: RavenCommand, tasks: dict[Future, BroadcastState]) -> object:
        while tasks:
            error = None
            try:
                first_finished, slow = asyncio.wait(tasks.keys(), return_when=asyncio.FIRST_COMPLETED)
            except Exception as e:
                error = e

            completed = filter(lambda task: task.done(), tasks.keys()).__next__()

            if error is not None:
                failed = tasks.get(completed)
                node = self._node_selector.topology.nodes[failed.index]

                command.failed_nodes[node] = error

                self._node_selector.on_failed_request(failed.index)
                # todo: self.spawn_health_checks(node, failed.index)

                del tasks[completed]
                continue

            # no need to abort requests, 'slow' variable contains cancelled tasks due to completion of first task

            self._node_selector.restore_node_index(tasks.get(completed).index)
            return tasks.get(completed).command.result

        exceptions = ", ".join(
            list(map(lambda x: str(UnsuccessfulRequestException(x[0].url, x[1])), set(command.failed_nodes)))
        )

        raise AllTopologyNodesDownException(f"Broadcasting {command.__class__.__name__} failed: {exceptions}")

    def __send_to_all_nodes(
        self, tasks: dict[Task, BroadcastState], session_info: SessionInfo, command: Union[RavenCommand, Broadcast]
    ):
        for index in range(len(self._node_selector.topology.nodes)):
            state = self.BroadcastState(
                command.prepare_to_broadcast(self.conventions), index, self._node_selector.topology.nodes[index], None
            )

            state.command.timeout = self.second_broadcast_attempt_timeout

            # todo: aggressive caching options

            async def __coro() -> None:
                # aggressive caching options
                try:
                    request = self.execute(state.node, None, state.command, False, session_info, True)
                    state.request = request
                finally:
                    # todo: aggressive_caching
                    pass

            tasks[asyncio.create_task(__coro())] = state

    def __handle_server_down(
        self,
        url: str,
        chosen_node: ServerNode,
        node_index: int,
        command: RavenCommand,
        request: requests.Request,
        response: requests.Response,
        e: Exception,
        session_info: SessionInfo,
        should_retry: bool,
    ):
        if command.failed_nodes is None:
            command.failed_nodes = {}

        command.failed_nodes[chosen_node] = self.__read_exception_from_server(request, response, e)

        if node_index is None:
            # We executed request over a node not in the topology. This means no failover...
            return False

        if self._node_selector is None:
            # todo: spawnHealthChecks(chosenNode, nodeIndex)
            return False

        # As the server is down, we discard the server version to ensure we update when it goes up.
        chosen_node.discard_server_version()

        self._node_selector.on_failed_request(node_index)

        if self.should_broadcast(command):
            command.result = self.__broadcast(command, session_info)
            return True

        # todo: self.spawn_health_checks(chosen_node, node_index)

        index_node_and_etag = self._node_selector.get_preferred_node_with_topology()
        if command.failover_topology_etag != self.topology_etag:
            command.failed_nodes.clear()
            command.failover_topology_etag = self.topology_etag

        if index_node_and_etag.current_node in command.failed_nodes:
            return False

        self.__on_failed_request_invoke(url, e)

        self.execute(
            index_node_and_etag.current_node, index_node_and_etag.current_index, command, should_retry, session_info
        )

        return True

    @staticmethod
    def __read_exception_from_server(request: requests.Request, response: requests.Response, e: Exception) -> Exception:
        if response and response.content:
            response_json = None
            try:
                response_json = response.content.decode("utf-8")

                # todo: change this bs
                def exception_schema_decoder(dictionary: dict) -> ExceptionDispatcher.ExceptionSchema:
                    return ExceptionDispatcher.ExceptionSchema(
                        dictionary.get("url"),
                        dictionary.get("class"),
                        dictionary.get("message"),
                        dictionary.get("error"),
                    )

                return ExceptionDispatcher.get(
                    json.loads(response_json, object_hook=exception_schema_decoder), response.status_code, e
                )
            except:
                exception_schema = ExceptionDispatcher.ExceptionSchema(
                    request.url,
                    "Unparsable Server Response",
                    "Get unrecognized response from the server",
                    response_json,
                )

                return ExceptionDispatcher.get(exception_schema, response.status_code, e)

        exception_schema = ExceptionDispatcher.ExceptionSchema(
            request.url,
            e.__class__.__qualname__,
            e.args[0],
            f"An exception occurred while contacting {request.url}.{os.linesep}{str(e)}",
        )
        return ExceptionDispatcher.get(exception_schema, HTTPStatus.SERVICE_UNAVAILABLE, e)

    class IndexAndResponse:
        def __init__(self, index: int, response: requests.Response):
            self.index = index
            self.response = response

    @client_configuration_etag.setter
    def client_configuration_etag(self, value):
        self._client_configuration_etag = value
