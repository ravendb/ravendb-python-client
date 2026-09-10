from __future__ import annotations

import enum
from typing import Optional

from ravendb.exceptions.raven_exceptions import RavenException


class LimitType(enum.Enum):
    """
    Which licensed capability a server refused. Carried by LicenseLimitException when the
    server names one.
    """

    INVALID_LICENSE = "InvalidLicense"  # Invalid License
    FORBIDDEN_HOST = "ForbiddenHost"  # Forbidden Host
    DYNAMIC_NODE_DISTRIBUTION = "DynamicNodeDistribution"  # Dynamic Nodes Distribution
    CLUSTER_SIZE = "ClusterSize"  # Cluster Size
    SNAPSHOT_BACKUP = "SnapshotBackup"  # Snapshot Backup
    CLOUD_BACKUP = "CloudBackup"  # Cloud Backup
    ENCRYPTION = "Encryption"
    DOCUMENTS_COMPRESSION = "DocumentsCompression"  # Documents Compression
    ROLLING_INDEXES = "RollingIndexes"  # Rolling Indexes
    EXTERNAL_REPLICATION = "ExternalReplication"  # External Replication
    RAVEN_ETL = "RavenEtl"  # Raven ETL
    SQL_ETL = "SqlEtl"  # SQL ETL
    OLAP_ETL = "OlapEtl"  # OLAP ETL
    ELASTIC_SEARCH_ETL = "ElasticSearchEtl"  # ElasticSearch ETL
    QUEUE_ETL = "QueueEtl"  # Queue ETL
    SNOWFLAKE_ETL = "SnowflakeEtl"  # Snowflake ETL
    EMBEDDINGS_GENERATION = "EmbeddingsGeneration"  # Embeddings Generation
    GEN_AI = "GenAi"  # Gen AI
    AI_AGENT = "AiAgent"  # AI Agent
    AI_ASSISTANT = "AiAssistant"  # AI Assistant
    QUILL = "Quill"
    CORES = "Cores"  # Cores Limit
    SNMP = "Snmp"  # SNMP
    POSTGRE_SQL_INTEGRATION = "PostgreSqlIntegration"  # PostgreSql Integration
    POWER_B_I = "PowerBI"  # Power BI
    DELAYED_EXTERNAL_REPLICATION = "DelayedExternalReplication"  # Delayed External Replication
    HIGHLY_AVAILABLE_TASKS = "HighlyAvailableTasks"  # Highly Available Tasks
    PULL_REPLICATION_AS_HUB = "PullReplicationAsHub"  # Pull Replication As Hub
    PULL_REPLICATION_AS_SINK = "PullReplicationAsSink"  # Pull Replication As Sink
    TIME_SERIES_ROLLUPS_AND_RETENTION = "TimeSeriesRollupsAndRetention"  # Time Series Rollups and Retention
    ENCRYPTED_BACKUP = "EncryptedBackup"  # Encrypted Backup
    ADDITIONAL_ASSEMBLIES_FROM_NU_GET = "AdditionalAssembliesFromNuGet"  # Additional Assemblies from NuGet
    MONITORING_ENDPOINTS = "MonitoringEndpoints"  # Monitoring Endpoints
    READ_ONLY_CERTIFICATES = "ReadOnlyCertificates"  # Read-only Certificates
    CONCURRENT_SUBSCRIPTIONS = "ConcurrentSubscriptions"  # Concurrent Subscriptions
    TCP_DATA_COMPRESSION = "TcpDataCompression"  # TCP Data Compression
    SERVER_WIDE_BACKUPS = "ServerWideBackups"  # Server Wide Backups
    SERVER_WIDE_EXTERNAL_REPLICATIONS = "ServerWideExternalReplications"  # Server Wide External Replications
    SERVER_WIDE_CUSTOM_SORTERS = "ServerWideCustomSorters"  # Server Wide Custom Sorters
    SERVER_WIDE_ANALYZERS = "ServerWideAnalyzers"  # Server Wide Analyzers
    SERVER_WIDE_CONNECTION_STRINGS = "ServerWideConnectionStrings"  # Server Wide Connection Strings
    INDEX_CLEANUP = "IndexCleanup"  # Index Cleanup
    PERIODIC_BACKUP = "PeriodicBackup"  # Periodic Backup
    CLIENT_CONFIGURATION = "ClientConfiguration"  # Client Configuration
    STUDIO_CONFIGURATION = "StudioConfiguration"  # Studio Configuration
    QUEUE_SINK = "QueueSink"  # Queue Sink
    CDC_SINK = "CdcSink"  # CDC Sink
    DATA_ARCHIVAL = "DataArchival"  # Data Archival
    SHARDING = "Sharding"
    SUBSCRIPTIONS = "Subscriptions"
    REVISIONS_CONFIGURATION = "RevisionsConfiguration"  # Revisions Configuration
    EXPIRATION = "Expiration"
    REFRESH = "Refresh"
    INDEXES = "Indexes"
    CUSTOM_SORTERS = "CustomSorters"  # Custom Sorters
    CUSTOM_ANALYZERS = "CustomAnalyzers"  # Custom Analyzers
    REMOTE_ATTACHMENTS = "RemoteAttachments"  # Remote Attachments
    SCHEMA_VALIDATION = "SchemaValidation"  # Schema Validation
    SSO = "Sso"  # SSO

    def __str__(self) -> str:
        return self.value


class LicenseLimitException(RavenException):
    """
    Raised when the server refuses an operation because the license does not cover it.

    The server answers HTTP 402 and names the feature in the message; when it also sends a
    machine-readable limit, it lands in :attr:`limit_type`.
    """

    def __init__(self, message: str = None, limit_type: Optional[LimitType] = None):
        super().__init__(message)
        self.limit_type = limit_type
