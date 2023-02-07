from __future__ import annotations

import datetime
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, Future, FIRST_COMPLETED, wait, ALL_COMPLETED
import uuid
from threading import Timer, Semaphore, Lock

import requests
from copy import copy

from ravendb import constants
from ravendb.documents.session.event_args import BeforeRequestEventArgs, FailedRequestEventArgs, SucceedRequestEventArgs
from ravendb.exceptions.exceptions import (
    AllTopologyNodesDownException,
    UnsuccessfulRequestException,
    DatabaseDoesNotExistException,
    AuthorizationException,
)
from ravendb.documents.operations.configuration import GetClientConfigurationOperation
from ravendb.exceptions.exception_dispatcher import ExceptionDispatcher
from ravendb.exceptions.raven_exceptions import ClientVersionMismatchException


from ravendb.http.http_cache import HttpCache
from ravendb.http.misc import ReadBalanceBehavior, ResponseDisposeHandling, LoadBalanceBehavior, Broadcast
from ravendb.http.raven_command import RavenCommand, RavenCommandResponseType
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import Topology, NodeStatus, NodeSelector, CurrentIndexAndNode, UpdateTopologyParameters
from ravendb.serverwide.commands import GetDatabaseTopologyCommand, GetClusterTopologyCommand

from http import HTTPStatus


from typing import TYPE_CHECKING, List, Dict, Tuple

if TYPE_CHECKING:
    from ravendb.documents.session import SessionInfo
    from ravendb.documents.conventions import DocumentConventions
    from typing import Union, Callable, Any, Optional


class RequestExecutor:
    __INITIAL_TOPOLOGY_ETAG = -2
    __GLOBAL_APPLICATION_IDENTIFIER = uuid.uuid4()
    CLIENT_VERSION = "5.2.1"
    logger = logging.getLogger("request_executor")

    # todo: initializer should take also cryptography certificates
    def __init__(
        self,
        database_name: str,
        conventions: DocumentConventions,
        certificate_path: Optional[str] = None,
        trust_store_path: Optional[str] = None,
        thread_pool_executor: Optional[ThreadPoolExecutor] = None,
        initial_urls: Optional[List[str]] = None,
    ):
        self.__update_topology_timer: Union[None, Timer] = None
        self.conventions = copy(conventions)
        self._node_selector: NodeSelector = None
        self.__default_timeout: datetime.timedelta = conventions.request_timeout
        self.__cache: HttpCache = HttpCache()

        self.__certificate_path = certificate_path
        self.__trust_store_path = trust_store_path

        self.__topology_taken_from_node: Union[None, ServerNode] = None
        self.__update_client_configuration_semaphore = Semaphore(1)
        self.__update_database_topology_semaphore = Semaphore(1)

        self.__failed_nodes_timers: Dict[ServerNode, NodeStatus] = {}

        self.__database_name = database_name

        self.__last_returned_response: Union[None, datetime.datetime] = None

        self._thread_pool_executor = (
            ThreadPoolExecutor(max_workers=10) if not thread_pool_executor else thread_pool_executor
        )

        self.number_of_server_requests = 0

        self._topology_etag: Union[None, int] = None
        self._client_configuration_etag: Union[None, int] = None
        self._disable_topology_updates: Union[None, bool] = None
        self._disable_client_configuration_updates: Union[None, bool] = None
        self._last_server_version: Union[None, str] = None

        self.__http_session: Union[None, requests.Session] = None

        self.first_broadcast_attempt_timeout: Union[
            None, datetime.timedelta
        ] = conventions.first_broadcast_attempt_timeout
        self.second_broadcast_attempt_timeout: Union[
            None, datetime.timedelta
        ] = conventions.second_broadcast_attempt_timeout

        self._first_topology_update_task: Union[None, Future] = None
        self._last_known_urls: Union[None, List[str]] = None
        self._disposed: Union[None, bool] = None

        self.__synchronized_lock = Lock()

        # --- events ---
        self.__on_before_request: List[Callable[[BeforeRequestEventArgs], Any]] = []
        self.__on_failed_request: List[Callable[[FailedRequestEventArgs], None]] = []
        self.__on_succeed_request: List[Callable[[SucceedRequestEventArgs], None]] = []
        self._on_topology_updated: List[Callable[[Topology], None]] = []

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
            self.__update_topology_timer.cancel()

        self._dispose_all_failed_nodes_timers()

    @property
    def certificate_path(self) -> str:
        return self.__certificate_path

    @property
    def trust_store_path(self) -> str:
        return self.__trust_store_path

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
        session = requests.session()
        session.cert = self.__certificate_path
        session.verify = self.__trust_store_path if self.__trust_store_path else True
        return session

    @property
    def cache(self) -> HttpCache:
        return self.__cache

    @property
    def topology_nodes(self) -> List[ServerNode]:
        return self.topology.nodes if self.topology else None

    @property
    def client_configuration_etag(self) -> int:
        return self._client_configuration_etag

    @property
    def default_timeout(self) -> datetime.timedelta:
        return self.__default_timeout

    @default_timeout.setter
    def default_timeout(self, value: datetime.timedelta) -> None:
        self.__default_timeout = value

    @property
    def preferred_node(self) -> CurrentIndexAndNode:
        self.__ensure_node_selector()
        return self._node_selector.get_preferred_node()

    def __on_failed_request_invoke(self, url: str, e: Exception):
        for event in self.__on_failed_request:
            event(FailedRequestEventArgs(self.__database_name, url, e))

    def __on_succeed_request_invoke(
        self, database: str, url: str, response: requests.Response, request: requests.Request, attempt_number: int
    ):
        for event in self.__on_succeed_request:
            event(SucceedRequestEventArgs(database, url, response, request, attempt_number))

    def _on_topology_updated_invoke(self, topology: Topology) -> None:
        for event in self._on_topology_updated:
            event(topology)

    def add_on_succeed_request(self, event: Callable[[SucceedRequestEventArgs], None]):
        self.__on_succeed_request.append(event)

    def remove_on_succeed_request(self, event: Callable[[SucceedRequestEventArgs], None]):
        self.__on_succeed_request.remove(event)

    def add_on_failed_request(self, event: Callable[[FailedRequestEventArgs], None]):
        self.__on_failed_request.append(event)

    def remove_on_failed_request(self, event: Callable[[FailedRequestEventArgs], None]):
        self.__on_failed_request.remove(event)

    def add_on_before_request(self, event: Callable[[BeforeRequestEventArgs], None]):
        self.__on_before_request.append(event)

    def remove_on_before_request(self, event: Callable[[BeforeRequestEventArgs], None]):
        self.__on_before_request.remove(event)

    def add_on_topology_updated(self, event: Callable[[Topology], None]):
        self._on_topology_updated.append(event)

    def remove_on_topology_updated(self, event: Callable[[Topology], None]):
        self._on_topology_updated.remove(event)

    @classmethod
    def create(
        cls,
        initial_urls: List[str],
        database_name: str,
        conventions: DocumentConventions,
        certificate_path: Optional[str] = None,
        trust_store_path: Optional[str] = None,
        thread_pool_executor: Optional[ThreadPoolExecutor] = None,
    ) -> RequestExecutor:
        executor = cls(database_name, conventions, certificate_path, trust_store_path, thread_pool_executor)
        executor._first_topology_update_task = executor._first_topology_update(
            initial_urls, cls.__GLOBAL_APPLICATION_IDENTIFIER
        )
        return executor

    @classmethod
    def create_for_single_node_with_configuration_updates(
        cls,
        url: str,
        database_name: str,
        conventions: DocumentConventions,
        certificate_path: Optional[str] = None,
        trust_store_path: Optional[str] = None,
        thread_pool_executor: Optional[ThreadPoolExecutor] = None,
    ) -> RequestExecutor:
        executor = cls.create_for_single_node_without_configuration_updates(
            url, database_name, conventions, certificate_path, trust_store_path, thread_pool_executor
        )
        executor._disable_client_configuration_updates = False
        return executor

    @classmethod
    def create_for_single_node_without_configuration_updates(
        cls,
        url: str,
        database_name: str,
        conventions: DocumentConventions,
        certificate_path: Optional[str] = None,
        trust_store_path: Optional[str] = None,
        thread_pool_executor: Optional[ThreadPoolExecutor] = None,
    ) -> RequestExecutor:
        initial_urls = cls.validate_urls([url])
        executor = cls(database_name, conventions, certificate_path, trust_store_path, thread_pool_executor)

        topology = Topology(-1, [ServerNode(initial_urls[0], database_name)])
        executor._node_selector = NodeSelector(topology, thread_pool_executor)
        executor._topology_etag = cls.__INITIAL_TOPOLOGY_ETAG
        executor._disable_topology_updates = True
        executor._disable_client_configuration_updates = True

        return executor

    def _update_client_configuration_async(self, server_node: ServerNode) -> Future:
        if self._disposed:
            future = Future()
            future.set_result(None)
            return future

        def __run_async():
            self.__update_client_configuration_semaphore.acquire()

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

        return self._thread_pool_executor.submit(__run_async)

    def update_topology_async(self, parameters: UpdateTopologyParameters) -> Future:
        if not parameters:
            raise ValueError("Parameters cannot be None")

        if self._disable_topology_updates:
            fut = Future()
            fut.set_result(False)
            return fut

        if self._disposed:
            fut = Future()
            fut.set_result(False)
            return fut

        def __supply_async():
            # prevent double topology updates if execution takes too much time
            # --> in cases with transient issues
            lock_taken = self.__update_database_topology_semaphore.acquire(timeout=parameters.timeout_in_ms)
            if not lock_taken:
                return False

            try:
                if self._disposed:
                    return False

                command = GetDatabaseTopologyCommand(
                    parameters.debug_tag,
                    parameters.application_identifier if self.conventions.send_application_identifier else None,
                )
                self.execute(parameters.node, None, command, False, None)
                topology = command.result

                if self._node_selector is None:
                    self._node_selector = NodeSelector(topology, self._thread_pool_executor)

                    if self.conventions.read_balance_behavior == ReadBalanceBehavior.FASTEST_NODE:
                        self._node_selector.schedule_speed_test()

                elif self._node_selector.on_update_topology(topology, parameters.force_update):
                    self._dispose_all_failed_nodes_timers()
                    if self.conventions.read_balance_behavior == ReadBalanceBehavior.FASTEST_NODE:
                        self._node_selector.schedule_speed_test()

                self._topology_etag = self._node_selector.topology.etag

                self._on_topology_updated_invoke(topology)
            except Exception as e:
                if not self._disposed:
                    raise e

            finally:
                self.__update_database_topology_semaphore.release()
            return True

        return self._thread_pool_executor.submit(__supply_async)

    def _first_topology_update(self, input_urls: List[str], application_identifier: Union[None, uuid.UUID]) -> Future:
        initial_urls = self.validate_urls(input_urls)
        errors: List[Tuple[str, Exception]] = []

        def __run(errors: list):
            for url in initial_urls:
                try:
                    server_node = ServerNode(url, self.__database_name)
                    update_parameters = UpdateTopologyParameters(server_node)
                    update_parameters.timeout_in_ms = 0x7FFFFFFF
                    update_parameters.debug_tag = "first-topology-update"
                    update_parameters.application_identifier = application_identifier

                    self.update_topology_async(update_parameters).result()
                    self.__initialize_update_topology_timer()

                    self.__topology_taken_from_node = server_node
                    return
                except Exception as e:
                    if isinstance(e.__cause__, AuthorizationException):
                        self._last_known_urls = initial_urls
                        raise e.__cause__

                    if isinstance(e.__cause__, DatabaseDoesNotExistException):
                        self._last_known_urls = initial_urls
                        raise e.__cause__

                    errors.append((url, e))

            topology = Topology(
                self._topology_etag,
                self.topology_nodes
                if self.topology_nodes
                else list(map(lambda url_val: ServerNode(url_val, self.__database_name, "!"), initial_urls)),
            )

            self._node_selector = NodeSelector(topology, self._thread_pool_executor)

            if initial_urls:
                self.__initialize_update_topology_timer()
                return

            self._last_known_urls = initial_urls
            # todo: details from exceptions in inner_list

        return self._thread_pool_executor.submit(__run, errors)

    @staticmethod
    def validate_urls(initial_urls: List[str]) -> List[str]:
        # todo: implement validation
        return initial_urls

    def __initialize_update_topology_timer(self):
        if self.__update_topology_timer is not None:
            return

        with self.__synchronized_lock:
            if self.__update_topology_timer is not None:
                return

            self.__update_topology_timer = Timer(60, self.__update_topology_callback)

    def _dispose_all_failed_nodes_timers(self) -> None:
        for node, status in self.__failed_nodes_timers.items():
            status.close()
        self.__failed_nodes_timers.clear()

    def execute_command(self, command: RavenCommand, session_info: Optional[SessionInfo] = None) -> None:
        topology_update = self._first_topology_update_task
        if (
            topology_update is not None
            and (topology_update.done() and (not topology_update.exception()) and (not topology_update.cancelled()))
            or self._disable_topology_updates
        ):
            current_index_and_node = self.choose_node_for_request(command, session_info)
            self.execute(
                current_index_and_node.current_node, current_index_and_node.current_index, command, True, session_info
            )
        else:
            self.__unlikely_execute(command, topology_update, session_info)

    def execute(
        self,
        chosen_node: ServerNode = None,
        node_index: int = None,
        command: RavenCommand = None,
        should_retry: bool = None,
        session_info: SessionInfo = None,
        ret_request: bool = False,
    ) -> Union[None, requests.Request]:
        if command.failover_topology_etag == RequestExecutor.__INITIAL_TOPOLOGY_ETAG:
            command.failover_topology_etag = RequestExecutor.__INITIAL_TOPOLOGY_ETAG
            if self._node_selector and self._node_selector.topology:
                topology = self._node_selector.topology
                if topology.etag:
                    command.failover_topology_etag = topology.etag

        request = self.__create_request(chosen_node, command)
        url = request.url

        if not request:
            return

        if ret_request:
            request_ref = request

        if not request:
            return

        no_caching = session_info.no_caching if session_info else False

        cached_item, change_vector, cached_value = self.__get_from_cache(command, not no_caching, url)
        # todo: if change_vector exists try get from cache - aggressive caching
        with cached_item:
            # todo: try get from cache
            self.__set_request_headers(session_info, change_vector, request)

            command.number_of_attempts = command.number_of_attempts + 1
            attempt_num = command.number_of_attempts
            for func in self.__on_before_request:
                func(BeforeRequestEventArgs(self.__database_name, url, request, attempt_num))
            response = self.__send_request_to_server(
                chosen_node, node_index, command, should_retry, session_info, request, url
            )

            if response is None:
                return

            refresh_tasks = self.__refresh_if_needed(chosen_node, response)

            command.status_code = response.status_code
            response_dispose = ResponseDisposeHandling.AUTOMATIC

            try:
                if response.status_code == HTTPStatus.NOT_MODIFIED:
                    self.__on_succeed_request_invoke(self.__database_name, url, response, request, attempt_num)
                    cached_item.not_modified()
                    if command.response_type == RavenCommandResponseType.OBJECT:
                        command.set_response(cached_value, True)
                    return

                if response.status_code >= 400:
                    if not self.__handle_unsuccessful_response(
                        chosen_node,
                        node_index,
                        command,
                        request,
                        response,
                        url,
                        session_info,
                        should_retry,
                    ):
                        db_missing_header = response.headers.get("Database-Missing", None)
                        if db_missing_header is not None:
                            raise DatabaseDoesNotExistException(db_missing_header)
                        self.__throw_failed_to_contact_all_nodes(command, request)
                    return  # we either handled this already in the unsuccessful response or we are throwing
                self.__on_succeed_request_invoke(self.__database_name, url, response, request, attempt_num)
                response_dispose = command.process_response(self.__cache, response, url)
                self.__last_returned_response = datetime.datetime.utcnow()
            finally:
                if response_dispose == ResponseDisposeHandling.AUTOMATIC:
                    response.close()
                if len(refresh_tasks) > 0:
                    try:
                        wait(refresh_tasks, return_when=ALL_COMPLETED)
                    except:
                        raise

    def __refresh_if_needed(self, chosen_node: ServerNode, response: requests.Response) -> List[Future]:
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
                topology_task = Future()
                topology_task.set_result(False)

            if refresh_client_configuration:
                client_configuration = self._update_client_configuration_async(server_node)
            else:
                client_configuration = Future()
                client_configuration.set_result(None)

            return [topology_task, client_configuration]
        return []

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
        #       and therefore we dismish its cost by orders of magnitude just doing it
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
        if not self._disable_topology_updates:
            if cmd.selected_node_tag and not cmd.selected_node_tag.isspace():
                return self._node_selector.get_requested_node(cmd.selected_node_tag)

        if self.conventions.load_balance_behavior == LoadBalanceBehavior.USE_SESSION_CONTEXT:
            if session_info is not None and session_info.can_use_load_balance_behavior:
                return self._node_selector.get_node_by_session_id(session_info.session_id)

        if not cmd.is_read_request:
            return self._node_selector.get_preferred_node()

        if self.conventions.read_balance_behavior == ReadBalanceBehavior.NONE:
            return self._node_selector.get_preferred_node()
        elif self.conventions.read_balance_behavior == ReadBalanceBehavior.ROUND_ROBIN:
            return self._node_selector.get_node_by_session_id(session_info.session_id if session_info else 0)
        elif self.conventions.read_balance_behavior == ReadBalanceBehavior.FASTEST_NODE:
            return self._node_selector.get_fastest_node()
        raise RuntimeError()

    def __unlikely_execute(
        self, command: RavenCommand, topology_update: Union[None, Future[None]], session_info: SessionInfo
    ) -> None:
        self.__wait_for_topology_update(topology_update)

        current_index_and_node = self.choose_node_for_request(command, session_info)
        self.execute(
            current_index_and_node.current_node, current_index_and_node.current_index, command, True, session_info
        )

    def __wait_for_topology_update(self, topology_update: Future[None]) -> None:
        try:
            if topology_update is None or topology_update.exception():
                with self.__synchronized_lock:
                    if self._first_topology_update_task is None or topology_update == self._first_topology_update_task:
                        if self._last_known_urls is None:
                            # shouldn't happen
                            raise RuntimeError(
                                "No known topology and no previously known one, cannot proceed, likely a bug"
                            )
                        self._first_topology_update_task = self._first_topology_update(self._last_known_urls, None)
                    topology_update = self._first_topology_update_task
            topology_update.result()
        # todo: narrow the exception scope down
        except BaseException as e:
            with self.__synchronized_lock:
                if self._first_topology_update_task == topology_update:
                    self._first_topology_update_task = None  # next request will raise it

            raise e

    def __update_topology_callback(self) -> None:
        time = datetime.datetime.now()
        if (time - self.__last_returned_response.time) <= datetime.timedelta(minutes=5):
            return

        try:
            selector = self._node_selector
            if selector is None:
                return
            preferred_node = selector.get_preferred_node()
            server_node = preferred_node.current_node
        except Exception as e:
            self.logger.info("Couldn't get preferred node Topology from _updateTopologyTimer", exc_info=e)
            return

        update_parameters = UpdateTopologyParameters(server_node)
        update_parameters.timeout_in_ms = 0
        update_parameters.debug_tag = "timer-callback"

        try:
            self.update_topology_async(update_parameters)
        except Exception as e:
            self.logger.info("Couldn't update topology from __update_topology_timer", exc_info=e)

    def __set_request_headers(
        self, session_info: SessionInfo, cached_change_vector: Union[None, str], request: requests.Request
    ) -> None:
        if cached_change_vector is not None:
            request.headers[constants.Headers.IF_NONE_MATCH] = f'"{cached_change_vector}"'

        if not self._disable_client_configuration_updates:
            request.headers[constants.Headers.CLIENT_CONFIGURATION_ETAG] = f'"{self._client_configuration_etag}"'

        if session_info and session_info.last_cluster_transaction_index:
            request.headers[constants.Headers.LAST_KNOWN_CLUSTER_TRANSACTION_INDEX] = str(
                session_info.last_cluster_transaction_index
            )

        if not self._disable_topology_updates:
            request.headers[constants.Headers.TOPOLOGY_ETAG] = f'"{self._topology_etag}"'

        if not request.headers.get(constants.Headers.CLIENT_VERSION):
            request.headers[constants.Headers.CLIENT_VERSION] = RequestExecutor.CLIENT_VERSION

    def __get_from_cache(
        self, command: RavenCommand, use_cache: bool, url: str
    ) -> Tuple[HttpCache.ReleaseCacheItem, Optional[str], Optional[str]]:
        if (
            use_cache
            and command.can_cache
            and command.is_read_request
            and command.response_type == RavenCommandResponseType.OBJECT
        ):
            return self.__cache.get(url)

        return HttpCache.ReleaseCacheItem(), None, None

    @staticmethod
    def __try_get_server_version(response: requests.Response) -> Union[None, str]:
        server_version_header = response.headers.get(constants.Headers.SERVER_VERSION)

        if server_version_header is not None:
            return server_version_header

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
        return (
            self.conventions.read_balance_behavior == ReadBalanceBehavior.FASTEST_NODE
            and self._node_selector
            and self._node_selector.in_speed_test_phase
            and len(self._node_selector.topology.nodes) > 1
            and command.is_read_request()
            and command.response_type == RavenCommandResponseType.OBJECT
            and chosen_node is not None
            and not isinstance(command, Broadcast)
        )

    def __execute_on_all_to_figure_out_the_fastest(
        self, chosen_node: ServerNode, command: RavenCommand
    ) -> requests.Response:
        number_failed_tasks = [0]  # mutable integer
        preferred_task: Future[RequestExecutor.IndexAndResponse] = None

        nodes = self._node_selector.topology.nodes
        tasks: List[Future[RequestExecutor.IndexAndResponse]] = [None] * len(nodes)

        for i in range(len(nodes)):
            task_number = i
            self.number_of_server_requests += 1

            def __supply_async(
                single_element_list_number_failed_tasks: List[int],
            ) -> (RequestExecutor.IndexAndResponse, int):
                try:
                    request, str_ref = self.__create_request(nodes[task_number], command)
                    self.__set_request_headers(None, None, request)
                    return self.IndexAndResponse(task_number, command.send(self.http_session, request))
                except Exception as e:
                    single_element_list_number_failed_tasks[0] += 1
                    tasks[task_number] = None
                    raise RuntimeError("Request execution failed", e)

            task = self._thread_pool_executor.submit(__supply_async, number_failed_tasks)

            if nodes[i].cluster_tag == chosen_node.cluster_tag:
                preferred_task = task
            else:
                task.add_done_callback(lambda result: result.response.close())

            tasks[i] = task

        while number_failed_tasks[0] < len(tasks):
            try:
                first_finished, slow = wait(list(filter(lambda t: t is not None, tasks)), return_when=FIRST_COMPLETED)
                fastest = first_finished.pop().result()
                fastest: RequestExecutor.IndexAndResponse
                self._node_selector.record_fastest(fastest.index, nodes[fastest.index])
                break
            except BaseException:
                for i in range(len(nodes)):
                    if tasks[i].exception() is not None:
                        number_failed_tasks[0] += 1
                        tasks[i] = None

        # we can reach here if number of failed task equal to the number of the nodes,
        # in which case we have nothing to do

        return preferred_task.result().response

    def __create_request(self, node: ServerNode, command: RavenCommand) -> requests.Request:
        request = command.create_request(node)
        if request.data and not isinstance(request.data, str):
            request.data = json.dumps(request.data, default=self.conventions.json_default_method)

        # todo: 1117 - 1133
        return request if request else None

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
        broadcast_tasks: Dict[Future[None], RequestExecutor.BroadcastState] = {}

        try:
            self.__send_to_all_nodes(broadcast_tasks, session_info, command)

            return self.__wait_for_broadcast_result(command, broadcast_tasks)
        finally:
            for task, broadcast_state in broadcast_tasks.items():
                if task:

                    def __exceptionally(self_task: Future):
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

    def __wait_for_broadcast_result(self, command: RavenCommand, tasks: Dict[Future[None], BroadcastState]) -> object:
        while tasks:
            error = None
            try:
                wait(tasks.keys(), return_when=FIRST_COMPLETED)
            except Exception as e:
                error = e

            completed = filter(Future.done, tasks.keys()).__next__()

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

    def __handle_unsuccessful_response(
        self,
        chosen_node: ServerNode,
        node_index: int,
        command: RavenCommand,
        request: requests.Request,
        response: requests.Response,
        url: str,
        session_info: SessionInfo,
        should_retry: bool,
    ) -> bool:
        if response.status_code == HTTPStatus.NOT_FOUND:
            self.__cache.set_not_found(url, False)  # todo : check if aggressively cached, don't just pass False
            if command.response_type == RavenCommandResponseType.EMPTY:
                return True
            elif command.response_type == RavenCommandResponseType.OBJECT:
                command.set_response(None, False)
            else:
                command.set_response_raw(response, None)
            return True

        elif response.status_code == HTTPStatus.FORBIDDEN:
            msg = self.__try_get_response_of_error(response)
            builder = ["Forbidden access to ", chosen_node.database, "@", chosen_node.url, ", "]
            if self.__certificate_path is None:
                builder.append("a certificate is required. ")
            else:
                builder.append("certificate does not have permission to access it or is unknown. ")

            builder.append(" Method: ")
            builder.append(request.method)
            builder.append(", Request: ")
            builder.append(request.url)
            builder.append(os.linesep)
            builder.append(msg)

            raise AuthorizationException("".join(builder))

        elif (
            response.status_code == HTTPStatus.GONE
        ):  # request not relevant for the chosen node - the database has been moved to a different one
            if not should_retry:
                return False

            if node_index is not None:
                self._node_selector.on_failed_request(node_index)

            if not command.failed_nodes is None:
                command.failed_nodes = {}

            if command.is_failed_with_node(chosen_node):
                command.failed_nodes[chosen_node] = UnsuccessfulRequestException(
                    f"Request to {request.url} ({request.method}) is not relevant for this node anymore."
                )

            index_and_node = self.choose_node_for_request(command, session_info)

            if index_and_node.current_node in command.failed_nodes:
                update_parameters = UpdateTopologyParameters(chosen_node)
                update_parameters.timeout_in_ms = 60000
                update_parameters.force_update = True
                update_parameters.debug_tag = "handle-unsuccessful-response"
                success = self.update_topology_async(update_parameters).result()
                if not success:
                    return False

                command.failed_nodes.clear()
                index_and_node = self.choose_node_for_request(command, session_info)
                self.execute(index_and_node.current_node, index_and_node.current_index, command, False, session_info)
                return True

        elif response.status_code in [
            HTTPStatus.GATEWAY_TIMEOUT,
            HTTPStatus.REQUEST_TIMEOUT,
            HTTPStatus.BAD_GATEWAY,
            HTTPStatus.SERVICE_UNAVAILABLE,
        ]:
            return self.__handle_server_down(
                url, chosen_node, node_index, command, request, response, None, session_info, should_retry
            )

        elif response.status_code == HTTPStatus.CONFLICT:
            raise NotImplementedError("Handle conflict")  # todo: handle conflict (exception dispatcher involved)

        elif response.status_code == 425:  # too early
            if not should_retry:
                return False

            if node_index is not None:
                self._node_selector.on_failed_request(node_index)

            if command.failed_nodes is None:
                command.failed_nodes = {}

            if not command.is_failed_with_node(chosen_node):
                command.failed_nodes[chosen_node] = UnsuccessfulRequestException(
                    f"Request to '{request.url}' ({request.method}) is processing and not yed available on that node."
                )

            next_node = self.choose_node_for_request(command, session_info)
            self.execute(next_node.current_node, next_node.current_index, command, True, session_info)

            if node_index is not None:
                self._node_selector.restore_node_index(node_index)

            return True
        else:
            command.on_response_failure(response)
            raise RuntimeError(
                json.loads(response.text).get("Message", "Missing message")
            )  # todo: Exception dispatcher

        return False

    def __try_get_response_of_error(self, response: requests.Response) -> str:
        try:
            return response.content.decode("utf-8")
        except Exception as e:
            return f"Could not read request: {e.args[0]}"

    def __send_to_all_nodes(
        self, tasks: Dict[Future, BroadcastState], session_info: SessionInfo, command: Union[RavenCommand, Broadcast]
    ):
        for index in range(len(self._node_selector.topology.nodes)):
            state = self.BroadcastState(
                command.prepare_to_broadcast(self.conventions), index, self._node_selector.topology.nodes[index], None
            )

            state.command.timeout = self.second_broadcast_attempt_timeout

            # todo: aggressive caching options

            def __run_async() -> None:
                # todo: aggressive caching options
                try:
                    request = self.execute(state.node, None, state.command, False, session_info, True)
                    state.request = request
                finally:
                    # todo: aggressive_caching
                    pass

            task = self._thread_pool_executor.submit(__run_async)
            tasks[task] = state

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

        return (
            False  # todo: command.failed_nodes[chosen_node] = self.__read_exception_from_server(request, response, e)
        )

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

    def __ensure_node_selector(self) -> None:
        if not self._disable_topology_updates:
            self.__wait_for_topology_update(self._first_topology_update_task)

        if self._node_selector is None:
            topology = Topology(self.topology_etag, self.topology_nodes)
            self._node_selector = NodeSelector(topology, self._thread_pool_executor)


class ClusterRequestExecutor(RequestExecutor):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()

    # todo: security, cryptography, https
    def __init__(
        self,
        conventions: DocumentConventions,
        certificate_path: Optional[str] = None,
        trust_store_path: Optional[str] = None,
        thread_pool_executor: Optional[ThreadPoolExecutor] = None,
        initial_urls: Optional[List[str]] = None,
    ):
        super(ClusterRequestExecutor, self).__init__(
            None, conventions, certificate_path, trust_store_path, thread_pool_executor, initial_urls
        )
        self.__cluster_topology_semaphore = Semaphore(1)

    @classmethod
    def create_for_single_node(
        cls,
        url: str,
        thread_pool_executor: ThreadPoolExecutor,
        conventions: Optional[DocumentConventions] = None,
        certificate_path: Optional[str] = None,
        trust_store_path: Optional[str] = None,
    ) -> ClusterRequestExecutor:
        initial_urls = [url]
        # todo: validate urls
        # todo: use default, static DocumentConventions
        executor = cls(
            (conventions if conventions else DocumentConventions()),
            certificate_path,
            trust_store_path,
            thread_pool_executor,
            initial_urls,
        )

        server_node = ServerNode(url)
        server_node.url = url

        topology = Topology(-1, [server_node])

        node_selector = NodeSelector(topology, thread_pool_executor)
        executor._node_selector = node_selector
        executor._topology_etag = -2
        executor._disable_client_configuration_updates = True
        executor._disable_topology_updates = True

        return executor

    @classmethod
    def create_without_database_name(
        cls,
        initial_urls: List[str],
        thread_pool_executor: ThreadPoolExecutor,
        conventions: DocumentConventions,
        certificate_path: Optional[str] = None,
        trust_store_path: Optional[str] = None,
    ) -> ClusterRequestExecutor:
        executor = cls(
            (conventions if conventions else DocumentConventions()),
            certificate_path,
            trust_store_path,
            thread_pool_executor,
            initial_urls,
        )
        executor._disable_client_configuration_updates = True
        executor._first_topology_update_task = executor._first_topology_update(initial_urls, None)
        return executor

    def update_topology_async(self, parameters: UpdateTopologyParameters) -> Future:
        if parameters is None:
            raise ValueError("Parameters cannot be None")

        if self._disposed:
            returned = Future()
            returned.set_result(False)
            return returned

        def __supply_async():
            lock_taken = self.__cluster_topology_semaphore.acquire(timeout=parameters.timeout_in_ms * 1000)
            if not lock_taken:
                return False
            try:
                if self._disposed:
                    return False

                command = GetClusterTopologyCommand(parameters.debug_tag)
                self.execute(parameters.node, None, command, False, None)

                results = command.result
                nodes = [
                    ServerNode(url=url, cluster_tag=cluster_tag)
                    for cluster_tag, url in results.topology.members.items()
                ]
                new_topology = Topology(results.etag, nodes)
                self._topology_etag = results.etag

                if self._node_selector is None:
                    self._node_selector = NodeSelector(new_topology, self._thread_pool_executor)

                    if self.conventions.read_balance_behavior == ReadBalanceBehavior.FASTEST_NODE:
                        self._node_selector.schedule_speed_test()

                elif self._node_selector.on_update_topology(new_topology, parameters.force_update):
                    self._dispose_all_failed_nodes_timers()

                    if self.conventions.read_balance_behavior == ReadBalanceBehavior.FASTEST_NODE:
                        self._node_selector.schedule_speed_test()

                self._on_topology_updated_invoke(new_topology)
            except BaseException as e:
                if not self._disposed:
                    raise e

            finally:
                self.__cluster_topology_semaphore.release()

            return True

        return self._thread_pool_executor.submit(__supply_async)

    def _throw_exceptions(self, details: str):
        raise RuntimeError(f"Failed to retrieve cluster topology from all known nodes {os.linesep}{details}")
