from __future__ import annotations
import datetime
import enum
from typing import Optional, Dict, Union, List, Set, TYPE_CHECKING

from pyravendb.documents.indexes import IndexDefinition, RollingIndexDeployment, RollingIndex, AutoIndexDefinition
from pyravendb.documents.indexes.analysis import AnalyzerDefinition
from pyravendb.documents.operations.backups import PeriodicBackupConfiguration
from pyravendb.documents.operations.configuration import ClientConfiguration, StudioConfiguration
from pyravendb.documents.operations.etl import RavenConnectionString, RavenEtlConfiguration
from pyravendb.documents.operations.etl.olap import OlapConnectionString, OlapEtlConfiguration
from pyravendb.documents.operations.etl.sql import SqlConnectionString, SqlEtlConfiguration
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

if TYPE_CHECKING:
    from pyravendb.serverwide import (
        ConflictSolver,
        DocumentsCompressionConfiguration,
        DeletionInProgressStatus,
        DatabaseTopology,
    )


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

    @staticmethod
    def from_json(json_dict: dict) -> DatabaseRecord:
        record = DatabaseRecord(json_dict.get("DatabaseName", None))
        record.disabled = json_dict.get("Disabled", None)
        record.encrypted = json_dict.get("Encrypted", None)
        record.etag_for_backup = json_dict.get("EtagForBackup", None)
        record.deletion_in_progress = json_dict.get("DeletionInProgress", None)
        record.rolling_indexes = json_dict.get("RollingIndexes", None)
        record.database_state = json_dict.get("DatabaseState", None)
        record.lock_mode = json_dict.get("LockMode", None)
        record.topology = json_dict.get("Topology", None)
        record.conflict_solver_config = json_dict.get("ConflictSolverConfig", None)
        record.documents_compression = json_dict.get("DocumentCompression", None)
        record.sorters = json_dict.get("Sorters", None)
        record.analyzers = json_dict.get("Analyzers", None)
        record.indexes = json_dict.get("Indexes", None)
        record.indexes_history_story = json_dict.get("IndexesHistory", None)
        record.auto_indexes = json_dict.get("AutoIndexes", None)
        record.settings = json_dict.get("Settings", None)
        record.revisions = json_dict.get("Revisions", None)
        record.time_series = json_dict.get("TimeSeries", None)
        record.revisions_for_conflicts = json_dict.get("RevisionsForConflicts", None)
        record.expiration = json_dict.get("Expiration", None)
        record.refresh = json_dict.get("Refresh", None)
        record.periodic_backups = json_dict.get("PeriodicBackups", None)
        record.external_replications = json_dict.get("ExternalReplications", None)
        record.sink_pull_replications = json_dict.get("SinkPullReplications", None)
        record.hub_pull_replications = json_dict.get("HubPullReplications", None)
        record.raven_connection_strings = json_dict.get("RavenConnectionStrings", None)
        record.sql_connection_strings = json_dict.get("SqlConnectionStrings", None)
        record.olap_connection_strings = json_dict.get("OlapConnectionStrings", None)
        record.raven_etls = json_dict.get("RavenEtls", None)
        record.sql_etls = json_dict.get("SqlEtls", None)
        record.olap_etls = json_dict.get("OlapEtls", None)
        record.client = json_dict.get("Client", None)
        record.studio = json_dict.get("Studio", None)
        record.truncated_cluster_transaction_commands_count = json_dict.get("TruncatedClusterTransactionCommand", None)
        return record

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


class DatabaseRecordWithEtag(DatabaseRecord):
    def __init__(self, database_name: str, etag: int):
        super().__init__(database_name)
        self.__etag = etag

    @staticmethod
    def from_json(json_dict: dict):
        record = DatabaseRecord.from_json(json_dict)
        record_with_etag = DatabaseRecordWithEtag(record.database_name, json_dict["Etag"])
        record_with_etag.__dict__.update(record.__dict__)
        return record_with_etag
