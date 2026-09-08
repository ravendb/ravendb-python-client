from __future__ import annotations

import enum
from typing import Any, Dict, Iterable, List, Optional, Set, Type, TypeVar


class DatabaseItemType(enum.Enum):
    """
    What an export or import operates on. The server takes a combination of these, so
    the smuggler options carry them as a set rather than a single value.
    """

    NONE = "None"
    DOCUMENTS = "Documents"
    REVISION_DOCUMENTS = "RevisionDocuments"
    INDEXES = "Indexes"
    IDENTITIES = "Identities"
    TOMBSTONES = "Tombstones"
    LEGACY_ATTACHMENTS = "LegacyAttachments"
    CONFLICTS = "Conflicts"
    COMPARE_EXCHANGE = "CompareExchange"
    LEGACY_DOCUMENT_DELETIONS = "LegacyDocumentDeletions"
    LEGACY_ATTACHMENT_DELETIONS = "LegacyAttachmentDeletions"
    DATABASE_RECORD = "DatabaseRecord"
    UNKNOWN = "Unknown"
    # Kept for callers written against older servers; 7.x replaced it with COUNTER_GROUPS.
    COUNTERS = "Counters"
    ATTACHMENTS = "Attachments"
    COUNTER_GROUPS = "CounterGroups"
    SUBSCRIPTIONS = "Subscriptions"
    COMPARE_EXCHANGE_TOMBSTONES = "CompareExchangeTombstones"
    TIME_SERIES = "TimeSeries"
    REPLICATION_HUB_CERTIFICATES = "ReplicationHubCertificates"
    TIME_SERIES_DELETED_RANGES = "TimeSeriesDeletedRanges"

    def __str__(self) -> str:
        return self.value


class DatabaseRecordItemType(enum.Enum):
    """Which parts of the database record an export or import operates on."""

    NONE = "None"
    CONFLICT_SOLVER_CONFIG = "ConflictSolverConfig"
    SETTINGS = "Settings"
    REVISIONS = "Revisions"
    EXPIRATION = "Expiration"
    PERIODIC_BACKUPS = "PeriodicBackups"
    EXTERNAL_REPLICATIONS = "ExternalReplications"
    RAVEN_CONNECTION_STRINGS = "RavenConnectionStrings"
    SQL_CONNECTION_STRINGS = "SqlConnectionStrings"
    RAVEN_ETLS = "RavenEtls"
    SQL_ETLS = "SqlEtls"
    CLIENT = "Client"
    SORTERS = "Sorters"
    SINK_PULL_REPLICATIONS = "SinkPullReplications"
    HUB_PULL_REPLICATIONS = "HubPullReplications"
    TIME_SERIES = "TimeSeries"
    DOCUMENTS_COMPRESSION = "DocumentsCompression"
    ANALYZERS = "Analyzers"
    LOCK_MODE = "LockMode"
    OLAP_CONNECTION_STRINGS = "OlapConnectionStrings"
    OLAP_ETLS = "OlapEtls"
    ELASTIC_SEARCH_CONNECTION_STRINGS = "ElasticSearchConnectionStrings"
    ELASTIC_SEARCH_ETLS = "ElasticSearchEtls"
    POSTGRE_SQL_INTEGRATION = "PostgreSQLIntegration"
    QUEUE_CONNECTION_STRINGS = "QueueConnectionStrings"
    QUEUE_ETLS = "QueueEtls"
    INDEXES_HISTORY = "IndexesHistory"
    REFRESH = "Refresh"
    QUEUE_SINKS = "QueueSinks"
    DATA_ARCHIVAL = "DataArchival"
    SNOWFLAKE_CONNECTION_STRINGS = "SnowflakeConnectionStrings"
    SNOWFLAKE_ETLS = "SnowflakeEtls"
    EMBEDDINGS_GENERATIONS = "EmbeddingsGenerations"
    AI_CONNECTION_STRINGS = "AiConnectionStrings"
    GEN_AI_ETLS = "GenAiEtls"
    AI_AGENTS = "AiAgents"
    REMOTE_ATTACHMENTS = "RemoteAttachments"
    SCHEMA_VALIDATION = "SchemaValidation"
    CDC_SINKS = "CdcSinks"

    def __str__(self) -> str:
        return self.value


class ExportCompressionAlgorithm(enum.Enum):
    ZSTD = "Zstd"
    GZIP = "Gzip"

    def __str__(self) -> str:
        return self.value


_TFlag = TypeVar("_TFlag", DatabaseItemType, DatabaseRecordItemType)


def flags_to_string(members: Optional[Iterable[_TFlag]], enum_type: Type[_TFlag]) -> str:
    """
    Renders a set of members the way the server reads a [Flags] enum: a comma-separated
    list of names, or "None" when nothing is selected.
    """
    selected = set(members or ())
    selected.discard(enum_type.NONE)
    if not selected:
        return enum_type.NONE.value

    # Declaration order keeps the output stable and matches the bit order the server uses.
    return ", ".join(member.value for member in enum_type if member in selected)


def flags_from_string(value: Optional[str], enum_type: Type[_TFlag]) -> Set[_TFlag]:
    """The inverse of flags_to_string."""
    if not value:
        return set()

    members = {enum_type(part.strip()) for part in value.split(",") if part.strip()}
    members.discard(enum_type.NONE)
    return members


DEFAULT_OPERATE_ON_TYPES: Set[DatabaseItemType] = {
    DatabaseItemType.INDEXES,
    DatabaseItemType.DOCUMENTS,
    DatabaseItemType.REVISION_DOCUMENTS,
    DatabaseItemType.CONFLICTS,
    DatabaseItemType.DATABASE_RECORD,
    DatabaseItemType.REPLICATION_HUB_CERTIFICATES,
    DatabaseItemType.IDENTITIES,
    DatabaseItemType.COMPARE_EXCHANGE,
    DatabaseItemType.ATTACHMENTS,
    DatabaseItemType.COUNTER_GROUPS,
    DatabaseItemType.SUBSCRIPTIONS,
    DatabaseItemType.TIME_SERIES,
    DatabaseItemType.TIME_SERIES_DELETED_RANGES,
}

DEFAULT_OPERATE_ON_DATABASE_RECORD_TYPES: Set[DatabaseRecordItemType] = {
    DatabaseRecordItemType.CLIENT,
    DatabaseRecordItemType.CONFLICT_SOLVER_CONFIG,
    DatabaseRecordItemType.EXPIRATION,
    DatabaseRecordItemType.EXTERNAL_REPLICATIONS,
    DatabaseRecordItemType.PERIODIC_BACKUPS,
    DatabaseRecordItemType.RAVEN_CONNECTION_STRINGS,
    DatabaseRecordItemType.RAVEN_ETLS,
    DatabaseRecordItemType.REVISIONS,
    DatabaseRecordItemType.SETTINGS,
    DatabaseRecordItemType.SQL_CONNECTION_STRINGS,
    DatabaseRecordItemType.SORTERS,
    DatabaseRecordItemType.SQL_ETLS,
    DatabaseRecordItemType.HUB_PULL_REPLICATIONS,
    DatabaseRecordItemType.SINK_PULL_REPLICATIONS,
    DatabaseRecordItemType.TIME_SERIES,
    DatabaseRecordItemType.DOCUMENTS_COMPRESSION,
    DatabaseRecordItemType.ANALYZERS,
    DatabaseRecordItemType.LOCK_MODE,
    DatabaseRecordItemType.OLAP_CONNECTION_STRINGS,
    DatabaseRecordItemType.OLAP_ETLS,
    DatabaseRecordItemType.ELASTIC_SEARCH_CONNECTION_STRINGS,
    DatabaseRecordItemType.ELASTIC_SEARCH_ETLS,
    DatabaseRecordItemType.POSTGRE_SQL_INTEGRATION,
    DatabaseRecordItemType.QUEUE_CONNECTION_STRINGS,
    DatabaseRecordItemType.QUEUE_ETLS,
    DatabaseRecordItemType.INDEXES_HISTORY,
    DatabaseRecordItemType.REFRESH,
    DatabaseRecordItemType.DATA_ARCHIVAL,
    DatabaseRecordItemType.QUEUE_SINKS,
    DatabaseRecordItemType.SNOWFLAKE_ETLS,
    DatabaseRecordItemType.SNOWFLAKE_CONNECTION_STRINGS,
    DatabaseRecordItemType.EMBEDDINGS_GENERATIONS,
    DatabaseRecordItemType.AI_CONNECTION_STRINGS,
    DatabaseRecordItemType.GEN_AI_ETLS,
    DatabaseRecordItemType.AI_AGENTS,
    DatabaseRecordItemType.REMOTE_ATTACHMENTS,
    DatabaseRecordItemType.SCHEMA_VALIDATION,
    DatabaseRecordItemType.CDC_SINKS,
}

DEFAULT_MAX_STEPS_FOR_TRANSFORM_SCRIPT = 10 * 1000


class DatabaseSmugglerOptions:
    """What an export or import covers, and how it transforms what it moves."""

    def __init__(
        self,
        operate_on_types: Set[DatabaseItemType] = None,
        operate_on_database_record_types: Set[DatabaseRecordItemType] = None,
        include_expired: bool = True,
        include_artificial: bool = False,
        include_archived: bool = True,
        remove_analyzers: bool = False,
        transform_script: str = None,
        max_steps_for_transform_script: int = DEFAULT_MAX_STEPS_FOR_TRANSFORM_SCRIPT,
        encryption_key: str = None,
        collections: List[str] = None,
        max_read_ops_per_second: Optional[int] = None,
        skip_corrupted_data: bool = False,
    ):
        self.operate_on_types = set(operate_on_types if operate_on_types is not None else DEFAULT_OPERATE_ON_TYPES)
        self.operate_on_database_record_types = set(
            operate_on_database_record_types
            if operate_on_database_record_types is not None
            else DEFAULT_OPERATE_ON_DATABASE_RECORD_TYPES
        )
        self.include_expired = include_expired
        self.include_artificial = include_artificial
        self.include_archived = include_archived
        self.remove_analyzers = remove_analyzers
        self.transform_script = transform_script
        self.max_steps_for_transform_script = max_steps_for_transform_script
        self.encryption_key = encryption_key
        # Empty means every collection.
        self.collections = collections if collections is not None else []
        self.max_read_ops_per_second = max_read_ops_per_second
        # Lets an export continue past corrupted data, reporting it in the result instead
        # of stopping. Useful when the database has lost its compression dictionaries.
        self.skip_corrupted_data = skip_corrupted_data

    def to_json(self) -> Dict[str, Any]:
        return {
            "OperateOnTypes": flags_to_string(self.operate_on_types, DatabaseItemType),
            "OperateOnDatabaseRecordTypes": flags_to_string(
                self.operate_on_database_record_types, DatabaseRecordItemType
            ),
            "IncludeExpired": self.include_expired,
            "IncludeArtificial": self.include_artificial,
            "IncludeArchived": self.include_archived,
            "RemoveAnalyzers": self.remove_analyzers,
            "TransformScript": self.transform_script,
            "MaxStepsForTransformScript": self.max_steps_for_transform_script,
            "EncryptionKey": self.encryption_key,
            "Collections": self.collections,
            "MaxReadOpsPerSecond": self.max_read_ops_per_second,
            "SkipCorruptedData": self.skip_corrupted_data,
        }

    def _fill_from_json(self, json_dict: Dict[str, Any]) -> None:
        self.operate_on_types = flags_from_string(json_dict.get("OperateOnTypes"), DatabaseItemType)
        self.operate_on_database_record_types = flags_from_string(
            json_dict.get("OperateOnDatabaseRecordTypes"), DatabaseRecordItemType
        )
        self.include_expired = json_dict.get("IncludeExpired", True)
        self.include_artificial = json_dict.get("IncludeArtificial", False)
        self.include_archived = json_dict.get("IncludeArchived", True)
        self.remove_analyzers = json_dict.get("RemoveAnalyzers", False)
        self.transform_script = json_dict.get("TransformScript")
        self.max_steps_for_transform_script = json_dict.get(
            "MaxStepsForTransformScript", DEFAULT_MAX_STEPS_FOR_TRANSFORM_SCRIPT
        )
        self.encryption_key = json_dict.get("EncryptionKey")
        self.collections = json_dict.get("Collections") or []
        self.max_read_ops_per_second = json_dict.get("MaxReadOpsPerSecond")
        self.skip_corrupted_data = json_dict.get("SkipCorruptedData", False)

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> DatabaseSmugglerOptions:
        options = cls()
        options._fill_from_json(json_dict)
        return options


class DatabaseSmugglerExportOptions(DatabaseSmugglerOptions):
    def __init__(self, compression_algorithm: ExportCompressionAlgorithm = None, **kwargs):
        super().__init__(**kwargs)
        # None leaves the choice to the server's own default.
        self.compression_algorithm = compression_algorithm

    def to_json(self) -> Dict[str, Any]:
        json_dict = super().to_json()
        if self.compression_algorithm is not None:
            json_dict["CompressionAlgorithm"] = self.compression_algorithm.value
        return json_dict

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> DatabaseSmugglerExportOptions:
        options = cls()
        options._fill_from_json(json_dict)
        compression_algorithm = json_dict.get("CompressionAlgorithm")
        options.compression_algorithm = (
            ExportCompressionAlgorithm(compression_algorithm) if compression_algorithm else None
        )
        return options


class DatabaseSmugglerImportOptions(DatabaseSmugglerOptions):
    def __init__(self, skip_revision_creation: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.skip_revision_creation = skip_revision_creation

    @classmethod
    def from_options(cls, options: DatabaseSmugglerOptions) -> DatabaseSmugglerImportOptions:
        """Carries the shared options over from an export, the way export-to-database does."""
        return cls(
            operate_on_types=set(options.operate_on_types),
            include_expired=options.include_expired,
            include_artificial=options.include_artificial,
            include_archived=options.include_archived,
            max_steps_for_transform_script=options.max_steps_for_transform_script,
            remove_analyzers=options.remove_analyzers,
            transform_script=options.transform_script,
        )

    def to_json(self) -> Dict[str, Any]:
        json_dict = super().to_json()
        json_dict["SkipRevisionCreation"] = self.skip_revision_creation
        return json_dict

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> DatabaseSmugglerImportOptions:
        options = cls()
        options._fill_from_json(json_dict)
        options.skip_revision_creation = json_dict.get("SkipRevisionCreation", False)
        return options
