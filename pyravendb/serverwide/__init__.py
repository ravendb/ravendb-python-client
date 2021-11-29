from __future__ import annotations

import datetime
import enum
from typing import Union, TYPE_CHECKING, Optional, List, Dict, Set

import pyravendb.http.request_executor as req_ex
from pyravendb.documents.indexes import RollingIndex, IndexDefinition, AutoIndexDefinition, RollingIndexDeployment
from pyravendb.documents.indexes.analysis import AnalyzerDefinition
from pyravendb.documents.operations.backups import PeriodicBackupConfiguration
from pyravendb.documents.operations.configuration import ClientConfiguration, StudioConfiguration
from pyravendb.documents.operations.expiration import ExpirationConfiguration
from pyravendb.documents.operations.refresh import RefreshConfiguration
from pyravendb.documents.operations.replication import (
    ExternalReplication,
    PullReplicationAsSink,
    PullReplicationDefinition,
)
from pyravendb.documents.operations.revisions import RevisionsConfiguration, RevisionsCollectionConfiguration
from pyravendb.documents.operations.time_series import TimeSeriesConfiguration
from pyravendb.documents.queries.sorting import SorterDefinition
from pyravendb.http import Topology
from pyravendb.serverwide.operations import (
    GetBuildNumberOperation,
    ServerOperation,
    VoidServerOperation,
    ServerWideOperation,
    DatabasePromotionStatus,
)
from pyravendb.tools.utils import CaseInsensitiveDict

if TYPE_CHECKING:
    from pyravendb.documents import DocumentStore
    from pyravendb.documents.operations.etl import RavenConnectionString, RavenEtlConfiguration
    from pyravendb.documents.operations.etl.olap import OlapConnectionString, OlapEtlConfiguration
    from pyravendb.documents.operations.etl.sql import SqlConnectionString, SqlEtlConfiguration
    from pyravendb.documents.operations import OperationIdResult, Operation


class ConnectionStringType(enum.Enum):
    NONE = "NONE"
    RAVEN = "RAVEN"
    SQL = "SQL"
    OLAP = "OLAP"


class ServerOperationExecutor:
    def __init__(self, store: DocumentStore):
        if store is None:
            raise ValueError("Store cannot be None")
        request_executor = self.create_request_executor(store)

        if request_executor is None:
            raise ValueError("Request Executor cannot be None")
        self.__store = store
        self.__request_executor = request_executor
        self.__initial_request_executor = None
        self.__node_tag = None
        self.__cache = CaseInsensitiveDict()

        # todo: store.register events

        # todo: if node tag is null add after_close_listener

    def send(
        self,
        operation: Union[VoidServerOperation, ServerOperation],
    ):
        if isinstance(operation, VoidServerOperation):
            command = operation.get_command(self.__request_executor.conventions)
            self.__request_executor.execute_command(command)

        if isinstance(operation, ServerOperation):
            command = operation.get_command(self.__request_executor.conventions)
            self.__request_executor.execute_command(command)

            return command.result

    def send_async(self, operation: ServerOperation[OperationIdResult]) -> Operation:
        command = operation.get_command(self.__request_executor.conventions)

        self.__request_executor.execute_command(command)
        return ServerWideOperation(
            self.__request_executor,
            self.__request_executor.conventions,
            command.result.operation_id,
            command.selected_node_tag if command.selected_node_tag else command.result.operation_node_tag,
        )

    def close(self) -> None:
        if self.__node_tag is not None:
            return

        if self.__request_executor is not None:
            self.__request_executor.close()

        cache = self.__cache
        if cache is not None:
            for key, value in cache.items():
                request_executor = value._request_executor
                if request_executor is not None:
                    request_executor.close()

            cache.clear()

    def __get_topology(self, request_executor: req_ex.ClusterRequestExecutor) -> Topology:
        topology: Topology = None
        try:
            topology = request_executor.topology
            if topology is None:
                # a bit rude way to make sure that topology was refreshed
                # but it handles a case when first topology update failed

                operation = GetBuildNumberOperation()
                command = operation.get_command(request_executor.conventions)
                request_executor.execute_command(command)

                topology = request_executor.topology

        except:
            pass

        if topology is None:
            raise RuntimeError("Could not fetch the topology")

        return topology

    @staticmethod
    def create_request_executor(store: DocumentStore) -> req_ex.ClusterRequestExecutor:
        return (
            req_ex.ClusterRequestExecutor.create_for_single_node(
                store.urls[0], store.thread_pool_executor, store.conventions
            )
            if store.conventions.disable_topology_updates
            else req_ex.ClusterRequestExecutor.create_without_database_name(
                store.urls, store.thread_pool_executor, store.conventions
            )
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return


# todo: maybe move DatabaseRecord with it serverwide dependencies to the serverwide.database_record?
class DatabaseRecord:
    def __init__(self, database_name: Optional[str] = None):
        self.database_name = database_name
        self.disabled: bool = False
        self.encrypted: bool = False
        self.settings: Dict[str, str] = {}
        self.conflict_solver_config: Union[None, ConflictSolver] = None
        self.documents_compression: Union[None, DocumentsCompressionConfiguration] = None
        self.etag_for_backup: Union[None, int] = None
        self.deletion_in_progress: Union[None, Dict[str, DeletionInProgressStatus]] = None
        self.rolling_indexes: Union[None, Dict[str, RollingIndex]] = None
        self.topology: Union[None, DatabaseTopology] = None
        self.sorters: Dict[str, SorterDefinition] = {}
        self.analyzers: Dict[str, AnalyzerDefinition] = {}
        self.indexes: Union[None, Dict[str, IndexDefinition]] = {}
        self.auto_indexes: Union[None, Dict[str, AutoIndexDefinition]] = None
        self.revisions: Union[None, RevisionsConfiguration] = None
        self.time_series: Union[None, TimeSeriesConfiguration] = None
        self.expiration: Union[None, ExpirationConfiguration] = None
        self.periodic_backups: List[PeriodicBackupConfiguration] = []
        self.external_replications: List[ExternalReplication] = []
        self.sink_pull_replications: List[PullReplicationAsSink] = []
        self.hub_pull_replications: List[PullReplicationDefinition] = []
        self.raven_connection_strings: Dict[str, RavenConnectionString] = {}
        self.sql_connection_strings: Dict[str, SqlConnectionString] = {}
        self.olap_connection_strings: Dict[str, OlapConnectionString] = {}
        self.raven_etls: List[RavenEtlConfiguration] = []
        self.sql_etls: List[SqlEtlConfiguration] = []
        self.olap_etls: List[OlapEtlConfiguration] = []
        self.client: Union[None, ClientConfiguration] = None
        self.studio: Union[None, StudioConfiguration] = None
        self.truncated_cluster_transaction_commands_count: int = 0
        self.database_state: Union[None, DatabaseRecord.DatabaseStateStatus] = None
        self.lock_mode: Union[None, DatabaseRecord.DatabaseLockMode] = None
        self.indexes_history_story: Union[None, List[DatabaseRecord.IndexHistoryEntry]] = None
        self.revisions_for_conflicts: Union[None, RevisionsCollectionConfiguration] = None
        self.refresh: Union[None, RefreshConfiguration] = None
        self.unused_database_ids: Union[None, Set[str]] = None

    def to_json(self):
        return {
            # todo: replace with .to_json() 's
            "DatabaseName": self.database_name,
            "Disabled": self.disabled,
            "Encrypted": self.encrypted,
            "EtagForBackup": self.etag_for_backup if self.etag_for_backup else 0,
            "DeletionInProgress": self.deletion_in_progress,
            "RollingIndexes": self.rolling_indexes,
            "DatabaseState": self.database_state,
            "LockMode": self.lock_mode,
            "Topology": self.topology,
            "ConflictSolverConfig": self.conflict_solver_config,
            "DocumentCompression": self.documents_compression,
            "Sorters": self.sorters,
            "Analyzers": self.analyzers,
            "Indexes": self.indexes,
            "IndexesHistory": self.indexes_history_story,
            "AutoIndexes": self.auto_indexes,
            "Settings": self.settings,
            "Revisions": self.revisions,
            "TimeSeries": self.time_series,
            "RevisionsForConflicts": self.revisions_for_conflicts,
            "Expiration": self.expiration,
            "Refresh": self.refresh,
            "PeriodicBackups": self.periodic_backups,
            "ExternalReplications": self.external_replications,
            "SinkPullReplications": self.sink_pull_replications,
            "HubPullReplications": self.hub_pull_replications,
            "RavenConnectionStrings": self.raven_connection_strings,
            "SqlConnectionStrings": self.sql_connection_strings,
            "OlapConnectionStrings": self.olap_connection_strings,
            "RavenEtls": self.raven_etls,
            "SqlEtls": self.sql_etls,
            "OlapEtls": self.olap_etls,
            "Client": self.client,
            "Studio": self.studio,
            "TruncatedClusterTransactionCommand": self.truncated_cluster_transaction_commands_count,
        }

    class IndexHistoryEntry:
        def __init__(
            self,
            definition: Optional[IndexDefinition] = None,
            source: Optional[str] = None,
            created_at: Optional[datetime.datetime] = None,
            rolling_deployment: Optional[Dict[str, RollingIndexDeployment]] = None,
        ):
            self.definition = definition
            self.source = source
            self.created_at = created_at
            self.rolling_deployment = rolling_deployment

    class DatabaseLockMode(enum.Enum):
        UNLOCK = "UNLOCK"
        PREVENT_DELETES_IGNORE = "PREVENT_DELETES_IGNORE"
        PREVENT_DELETES_ERROR = "PREVENT_DELETES_ERROR"

        def __str__(self):
            return self.value

    class DatabaseStateStatus(enum.Enum):
        NORMAL = "NORMAL"
        RESTORE_IN_PROGRESS = "RESTORE_IN_PROGRESS"

        def __str__(self):
            return self.value


class DeletionInProgressStatus(enum.Enum):
    NO = "NO"
    SOFT_DELETE = "SOFT_DELETE"
    HARD_DELETE = "HARD_DELETE"

    def __str__(self):
        return self.value


class ConflictSolver:
    def __init__(
        self,
        resolve_by_collection: Optional[Dict[str, ScriptResolver]] = None,
        resolve_to_latest: Optional[bool] = None,
    ):
        self.resolve_by_collection = resolve_by_collection
        self.resolve_to_latest = resolve_to_latest


class ScriptResolver:
    def __init__(self, script: Optional[str] = None, last_modified_time: Optional[datetime.datetime] = None):
        self.script = script
        self.last_modified_time = last_modified_time


class DocumentsCompressionConfiguration:
    def __init__(self, compress_revisions: Optional[bool] = None, connections: Optional[List[str]] = None):
        self.compress_revisions = compress_revisions
        self.connections = connections


class DatabaseTopology:
    def __init__(
        self,
        members: Optional[List[str]] = None,
        promotables: Optional[List[str]] = None,
        rehabs: Optional[List[str]] = None,
        predefined_mentors: Optional[Dict[str, str]] = None,
        demotion_reasons: Optional[Dict[str, str]] = None,
        promotables_status: Optional[Dict[str, DatabasePromotionStatus]] = None,
        replication_factor: Optional[int] = None,
        dynamic_nodes_distribution: Optional[bool] = None,
        stamp: Optional[LeaderStamp] = None,
        database_topology_id_base_64: Optional[str] = None,
        priority_order: Optional[List[str]] = None,
    ):
        self.members = members
        self.promotables = promotables
        self.rehabs = rehabs
        self.predefined_mentors = predefined_mentors
        self.demotion_reasons = demotion_reasons
        self.promotables_status = promotables_status
        self.replication_factor = replication_factor
        self.dynamic_nodes_distribution = dynamic_nodes_distribution
        self.stamp = stamp
        self.database_topology_id_base_64 = database_topology_id_base_64
        self.priority_order = priority_order

    # todo: make sure it won't be parsed to json while using json.dumps - research if json parses properties
    @property
    def all_nodes(self) -> List[str]:
        return [*self.members, *self.promotables, *self.rehabs]

    @staticmethod
    def from_json(json_dict: dict) -> DatabaseTopology:
        return DatabaseTopology(
            json_dict["Members"],
            json_dict["Promotables"],
            json_dict["Rehabs"],
            json_dict.get("PredefinedMentors", None),
            json_dict["DemotionReasons"],
            json_dict["PromotablesStatus"],
            json_dict["ReplicationFactor"],
            json_dict["DynamicNodesDistribution"],
            LeaderStamp.from_json(json_dict["Stamp"]),
            json_dict["DatabaseTopologyIdBase64"],
            json_dict["PriorityOrder"],
        )


class LeaderStamp:
    def __init__(self, index: Optional[int] = None, term: Optional[int] = None, leaders_ticks: Optional[int] = None):
        self.index = index
        self.term = term
        self.leaders_ticks = leaders_ticks

    @staticmethod
    def from_json(json_dict: Dict) -> LeaderStamp:
        return LeaderStamp(json_dict["Index"], json_dict["Term"], json_dict["LeadersTicks"])
