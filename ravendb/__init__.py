from ravendb.documents.commands.batches import (
    BatchOptions,
    DeleteAttachmentCommandData,
    PatchCommandData,
    PutAttachmentCommandData,
    CommandData,
    CopyAttachmentCommandData,
    MoveAttachmentCommandData,
    BatchPatchCommandData,
    CountersBatchCommandData,
    PutCompareExchangeCommandData,
    DeleteCompareExchangeCommandData,
)
from ravendb.documents.commands.crud import DeleteDocumentCommand, PutDocumentCommand
from ravendb.documents.indexes.analysis.definitions import AnalyzerDefinition
from ravendb.documents.indexes.definitions import (
    IndexDeploymentMode,
    IndexDefinition,
    AbstractCommonApiForIndexes,
    AdditionalAssembly,
    IndexFieldOptions,
    RollingIndex,
    RollingIndexDeployment,
    RollingIndexState,
    IndexSourceType,
    AutoIndexDefinition,
    AutoIndexFieldOptions,
)
from ravendb.documents.indexes.index_creation import AbstractIndexDefinitionBuilder, AbstractIndexCreationTask
from ravendb.documents.indexes.spatial.configuration import AutoSpatialOptions
from ravendb.documents.operations.attachments import (
    DeleteAttachmentOperation,
    PutAttachmentOperation,
    GetAttachmentOperation,
    AttachmentRequest,
)
from ravendb.documents.operations.backups.settings import (
    BackupConfiguration,
    AmazonSettings,
    AzureSettings,
    FtpSettings,
    GlacierSettings,
    LocalSettings,
    PeriodicBackupConfiguration,
    S3Settings,
    BackupSettings,
    BackupStatus,
    GoogleCloudSettings,
)
from ravendb.documents.operations.batch import BatchOperation
from ravendb.documents.operations.compare_exchange.compare_exchange import (
    CompareExchangeValue,
    CompareExchangeSessionValue,
    CompareExchangeValueState,
)
from ravendb.documents.operations.compare_exchange.compare_exchange_value_result_parser import (
    CompareExchangeValueResultParser,
)
from ravendb.documents.operations.compare_exchange.operations import (
    PutCompareExchangeValueOperation,
    GetCompareExchangeValueOperation,
    CompareExchangeResult,
    GetCompareExchangeValuesOperation,
    DeleteCompareExchangeValueOperation,
)
from ravendb.documents.operations.configuration import (
    GetServerWideClientConfigurationOperation,
    PutServerWideClientConfigurationOperation,
    ClientConfiguration,
    GetClientConfigurationOperation,
    PutClientConfigurationOperation,
    StudioConfiguration,
    StudioEnvironment,
)
from ravendb.documents.operations.connection_strings import ConnectionString
from ravendb.documents.operations.etl.configuration import EtlConfiguration, RavenEtlConfiguration
from ravendb.documents.operations.etl.olap import OlapEtlConfiguration
from ravendb.documents.operations.etl.sql import SqlEtlConfiguration
from ravendb.documents.operations.executor import MaintenanceOperationExecutor, SessionOperationExecutor
from ravendb.documents.operations.expiration.configuration import ExpirationConfiguration
from ravendb.documents.operations.indexes import (
    GetIndexNamesOperation,
    DisableIndexOperation,
    EnableIndexOperation,
    GetIndexingStatusOperation,
    GetIndexesStatisticsOperation,
    GetIndexStatisticsOperation,
    GetIndexesOperation,
    GetTermsOperation,
    IndexHasChangedOperation,
    PutIndexesOperation,
    StopIndexingOperation,
    StartIndexingOperation,
    StopIndexOperation,
    StartIndexOperation,
    DeleteIndexOperation,
    SetIndexesLockOperation,
    SetIndexesPriorityOperation,
    GetIndexOperation,
    GetIndexErrorsOperation,
    IndexingStatus,
)
from ravendb.documents.operations.lazy.lazy_operation import LazyOperation
from ravendb.documents.operations.misc import DeleteByQueryOperation, GetOperationStateOperation, QueryOperationOptions
from ravendb.documents.operations.patch import (
    PatchOperation,
    PatchByQueryOperation,
    PatchRequest,
    PatchResult,
    PatchStatus,
)
from ravendb.documents.operations.refresh.configuration import RefreshConfiguration
from ravendb.documents.operations.replication.definitions import (
    ExternalReplication,
    PullReplicationAsSink,
    PullReplicationDefinition,
    ReplicationNode,
    ExternalReplicationBase,
)
from ravendb.documents.operations.revisions.configuration import (
    RevisionsCollectionConfiguration,
    RevisionsConfiguration,
)
from ravendb.documents.operations.statistics import (
    GetCollectionStatisticsOperation,
    CollectionStatistics,
    GetStatisticsOperation,
    DatabaseStatistics,
    IndexInformation,
    GetDetailedStatisticsOperation,
    DetailedDatabaseStatistics,
)
from ravendb.documents.queries.explanation import ExplanationOptions, Explanations
from ravendb.documents.queries.facets.builders import RangeBuilder, FacetBuilder, FacetOperations
from ravendb.documents.queries.facets.definitions import (
    FacetAggregationField,
    Facet,
    RangeFacet,
    FacetBase,
    GenericRangeFacet,
)
from ravendb.documents.queries.facets.queries import (
    AggregationRawDocumentQuery,
    AggregationDocumentQuery,
    AggregationQueryBase,
)
from ravendb.documents.queries.group_by import GroupBy, GroupByMethod
from ravendb.documents.queries.highlighting import HighlightingOptions, QueryHighlightings
from ravendb.documents.queries.index_query import IndexQuery
from ravendb.documents.queries.misc import SearchOperator
from ravendb.documents.queries.more_like_this import (
    MoreLikeThisOperations,
    MoreLikeThisBase,
    MoreLikeThisBuilder,
    MoreLikeThisOptions,
)
from ravendb.documents.queries.query import QueryOperator, ProjectionBehavior, QueryData, QueryResult, QueryTimings
from ravendb.documents.queries.sorting import SorterDefinition
from ravendb.documents.queries.spatial import (
    SpatialCriteriaFactory,
    SpatialCriteria,
    CircleCriteria,
    DynamicSpatialField,
    WktCriteria,
    PointField,
)
from ravendb.documents.queries.suggestions import (
    SuggestionBuilder,
    SuggestionDocumentQuery,
    StringDistanceTypes,
    SuggestionOptions,
    SuggestionBase,
    SuggestionResult,
    SuggestionSortMode,
)
from ravendb.documents.session.cluster_transaction_operation import ClusterTransactionOperations
from ravendb.documents.session.document_info import DocumentInfo
from ravendb.documents.session.document_session import DocumentSession
from ravendb.documents.session.entity_to_json import EntityToJson
from ravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
from ravendb.documents.session.loaders.include import IncludeBuilder, IncludeBuilderBase, QueryIncludeBuilder
from ravendb.documents.session.loaders.loaders import (
    LoaderWithInclude,
    LazyMultiLoaderWithInclude,
    MultiLoaderWithInclude,
)
from ravendb.documents.session.misc import (
    CmpXchg,
    DocumentsChanges,
    ForceRevisionStrategy,
    MethodCall,
    OrderingType,
    JavaScriptMap,
    DocumentQueryCustomization,
    ResponseTimeInformation,
    TransactionMode,
    SessionOptions,
)
from ravendb.documents.session.operations.lazy import (
    LazySessionOperations,
    LazyAggregationQueryOperation,
    LazyLoadOperation,
    LazyQueryOperation,
    LazyStartsWithOperation,
    LazySuggestionQueryOperation,
    LazyConditionalLoadOperation,
)
from ravendb.documents.session.operations.load_operation import LoadOperation
from ravendb.documents.session.operations.operations import LoadStartingWithOperation, MultiGetOperation
from ravendb.documents.session.operations.query import QueryOperation
from ravendb.documents.session.query import (
    AbstractDocumentQuery,
    DocumentQuery,
    RawDocumentQuery,
    QueryStatistics,
    WhereParams,
)
from ravendb.documents.session.query_group_by import GroupByDocumentQuery, GroupByField
from ravendb.documents.session.utils.document_query import DocumentQueryHelper
from ravendb.documents.session.utils.includes_util import IncludesUtil
from ravendb.documents.store.definition import DocumentStore, DocumentStoreBase
from ravendb.documents.store.lazy import Lazy
from ravendb.documents.session.conditional_load import ConditionalLoadResult
from ravendb.documents.store.misc import IdTypeAndName
from ravendb.http.misc import AggressiveCacheOptions, Broadcast, LoadBalanceBehavior, ReadBalanceBehavior
from ravendb.http.raven_command import RavenCommand
from ravendb.http.request_executor import ClusterRequestExecutor, RequestExecutor
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import (
    ClusterTopology,
    CurrentIndexAndNode,
    CurrentIndexAndNodeAndEtag,
    RaftCommand,
    NodeSelector,
    Topology,
    UpdateTopologyParameters,
)

# StatusCode
# UriUtility
# ServerWide
# CompactSettings
from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
from ravendb.json.result import BatchCommandResult
from ravendb.serverwide.commands import GetDatabaseTopologyCommand, GetClusterTopologyCommand
from ravendb.serverwide.misc import DocumentsCompressionConfiguration, DeletionInProgressStatus

# IDatabaseTaskStatus
from ravendb.serverwide.operations.certificates import (
    CertificateMetadata,
    EditClientCertificateOperation,
    ReplaceClusterCertificateOperation,
    GetCertificateMetadataOperation,
    GetCertificatesMetadataOperation,
    CertificateDefinition,
    CertificateRawData,
    CreateClientCertificateOperation,
    DatabaseAccess,
    DeleteCertificateOperation,
    GetCertificateOperation,
    GetCertificatesOperation,
    GetCertificatesResponse,
    PutClientCertificateOperation,
    SecurityClearance,
)
from ravendb.serverwide.operations.common import (
    BuildNumber,
    GetBuildNumberOperation,
    GetDatabaseNamesOperation,
    GetServerWideOperationStateOperation,
    CreateDatabaseOperation,
    GetDatabaseRecordOperation,
)

from ravendb.documents.identity.hilo import (
    HiLoIdGenerator,
    MultiTypeHiLoGenerator,
    MultiDatabaseHiLoGenerator,
    HiLoResult,
    GenerateEntityIdOnTheClient,
)

# todo: Serverwide
# ReorderDatabaseMembersOperation
# ConfigureRevisionsForConflictsOperation
# UpdateDatabaseOperation
# GetServerWideBackupConfigurationOperation
# SetDatabaseDynamicDistributionOperation
# UpdateUnusedDatabasesOperation

# todo: Serverwide Operations
# Operations
# DeleteDatabasesOperation
# ServerWideOperationCompletionAwaiter
# GetLogsConfigurationResult
# GetLogsConfigurationOperation
# LogMode
# SetLogsConfigurationOperation
# DeleteServerWideBackupConfigurationOperation
# GetServerWideBackupConfigurationsOperation
# PutServerWideBackupConfigurationOperation
# ServerWideBackupConfiguration
# DatabaseSettings
# GetDatabaseSettingsOperation
# PutDatabaseSettingsOperation
# GetTcpInfoCommand
# AddClusterNodeCommand
# ServerWide
# ModifyConflictSolverOperation

# todo: Operations and Commands
# BulkInsertOperation
# CollectionDetails
# BackupTaskType
# DatabaseHealthCheckOperation
# DetailedCollectionStatistics
# GetDetailedCollectionStatisticsOperation
# OperationAbstractions
# CompactDatabaseOperation
# PutConnectionStringOperation
# DeleteSorterOperation
# PutSortersOperation
# CompareExchangeValueJsonConverter
# ICompareExchangeValue
# GetServerWideExternalReplicationsResponse
# GetNextOperationIdCommand
# KillOperationCommand
# NextIdentityForCommand
# SeedIdentityForCommand
# ExplainQueryCommand
# GetIdentitiesOperation
# OperationCompletionAwaiter
# DeleteIndexErrorsOperation
# ResetIndexOperation
# GetServerWideBackupConfigurationsResponse
# NextIdentityForOperation
# SeedIdentityForOperation
# IOperationProgress
# IOperationResult
# PullReplicationDefinitionAndCurrentConnections
# DetailedReplicationHubAccess
# GetReplicationHubAccessOperation
# PreventDeletionsMode
# PullReplicationMode
# RegisterReplicationHubAccessOperation
# ReplicationHubAccess
# ReplicationHubAccessResult
# ReplicationHubAccessResponse
# UnregisterReplicationHubAccessOperation
# UpdatePullReplicationAsSinkOperation
# GetConflictsCommand
# PutAttachmentCommandHelper
# SetupDocumentBase
# StreamResultResponse
# StreamResult
# GetRevisionOperation
# GetRevisionsCountOperation
# IEagerSessionOperations
# LazyClusterTransactionOperations
# LazyGetCompareExchangeValueOperation
# LazyGetCompareExchangeValuesOperation
# LazyRevisionOperation
# LazyRevisionOperations
# StreamOperation
# ConfigureRevisionsOperation
# GetRevisionsOperation
# RevisionsResult
# GetConnectionStringsOperation
# RemoveConnectionStringOperation
# SqlEtlTable
# OlapEtlFileFormat
# OlapEtlTable
# Transformation
# AddEtlOperation
# UpdateEtlOperation
# ResetEtlOperation
# DisableDatabaseToggleResult
# ConfigureExpirationOperation
# DeleteOngoingTaskOperation
# GetPullReplicationHubTasksInfoOperation
# OngoingTaskPullReplicationAsSink
# OngoingTaskPullReplicationAsHub
# OngoingTaskType
# RunningBackup
# NextBackup
# GetOngoingTaskInfoOperation
# ToggleOngoingTaskStateOperation
# ConfigureRefreshOperation
# ConfigureRefreshOperationResult
# ToggleDatabasesStateOperation
# StartTransactionsRecordingOperation
# StopTransactionsRecordingOperation

# todo: backup
# BackupEncryptionSettings
# BackupEncryptionSettings
# GetPeriodicBackupStatusOperation
# GetPeriodicBackupStatusOperationResult
# LastRaftIndex
# PeriodicBackupStatus
# RestoreBackupConfiguration
# RestoreBackupOperation
# StartBackupOperation
# StartBackupOperationResult
# UpdatePeriodicBackupOperation
# UpdatePeriodicBackupOperationResult
# UploadProgress
# UploadState
# CompressionLevel
# GetBackupConfigurationScript
# RestoreBackupConfigurationBase
# RestoreFromAzureConfiguration
# RestoreFromGoogleCloudConfiguration
# RestoreFromS3Configuration
# RestoreType
# RetentionPolicy

# todo: Indexes
# Enums
# IndexDefinitionHelper
# IndexStats
# Indexes
# IndexDefinitionBase
# AbstractCsharpIndexCreationTask
# AbstractCsharpMultiMapIndexCreationTask
# AbstractJavaScriptIndexCreationTask
# AbstractJavaScriptMultiMapIndexCreationTask
# AbstractRawJavaScriptIndexCreationTask
# AbstractCountersIndexCreationTask
# AbstractGenericCountersIndexCreationTask
# AbstractCsharpCountersIndexCreationTask
# AbstractMultiMapCountersIndexCreationTask
# AbstractRawJavaScriptCountersIndexCreationTask
# CountersIndexDefinition
# CountersIndexDefinitionBuilder
# AbstractGenericTimeSeriesIndexCreationTask
# AbstractMultiMapTimeSeriesIndexCreationTask
# AbstractCsharpTimeSeriesIndexCreationTask
# AbstractRawJavaScriptTimeSeriesIndexCreationTask
# AbstractTimeSeriesIndexCreationTask
# TimeSeriesIndexDefinition
# TimeSeriesIndexDefinitionBuilder

# todo: Store
# DocumentAbstractions

# todo: Subscriptions
# SubscriptionBatch
# DocumentSubscriptions
# SubscriptionWorker
# SubscriptionWorkerOptions
# SubscriptionCreationOptions
# Revision
# SubscriptionState
# SubscriptionCreationOptions
# UpdateSubscriptionResult
# SubscriptionOpeningStrategy
# SubscriptionUpdateOptions

# todo: Session
# IAbstractDocumentQueryImpl
# ILazyRevisionsOperations
# IAdvancedSessionOperations
# IDocumentQueryBuilder
# IDocumentQueryBaseSingle
# IEnumerableQuery
# IFilterDocumentQueryBase
# IGraphDocumentQuery
# IGroupByDocumentQuery
# IQueryBase
# QueryEvents
# QueryOptions
# StreamQueryStatistics
# SessionEvents
# ILazyClusterTransactionOperations
# ISessionDocumentAppendTimeSeriesBase
# ISessionDocumentDeleteTimeSeriesBase
# ISessionDocumentRollupTypedAppendTimeSeriesBase
# ISessionDocumentRollupTypedTimeSeries
# ISessionDocumentTimeSeries
# ISessionDocumentTypedAppendTimeSeriesBase
# ISessionDocumentTypedTimeSeries
# DocumentResultStream
# SessionDocumentRollupTypedTimeSeries
# SessionDocumentTimeSeries
# SessionDocumentTypedTimeSeries
# SessionTimeSeriesBase
# ICounterIncludeBuilder
# IAbstractTimeSeriesIncludeBuilder
# ICompareExchangeValueIncludeBuilder
# IDocumentIncludeBuilder
# IGenericIncludeBuilder
# IGenericRevisionIncludeBuilder
# IGenericTimeSeriesIncludeBuilder
# ISubscriptionIncludeBuilder
# ISubscriptionTimeSeriesIncludeBuilder
# TimeSeriesIncludeBuilder
# SubscriptionIncludeBuilder:
# ILazyLoaderWithInclude
# ITimeSeriesIncludeBuilder
# DocumentSessionAttachments
# DocumentSessionAttachmentsBase
# DocumentSessionRevisions
# DocumentSessionRevisionsBase
# IAttachmentsSessionOperations
# IRevisionsSessionOperations
# MetadataObject
# ISessionDocumentCounters
# CounterInternalTypes
# SessionDocumentCounters
# TimeSeriesEntry
# TimeSeriesValue
# TimeSeriesValuesHelper
# TypedTimeSeriesEntry
# TypedTimeSeriesRollupEntry
# TimeSeriesOperations

# todo: Batch
# StreamResult

# todo: Counters
# CounterBatch
# GetCountersOperation
# CounterBatchOperation
# CounterOperationType
# CounterOperation
# DocumentCountersOperation
# CounterDetail
# CountersDetail

# todo: TimeSeries
# AggregationType
# RawTimeSeriesTypes
# ConfigureRawTimeSeriesPolicyOperation
# ConfigureTimeSeriesOperation
# ConfigureTimeSeriesOperationResult
# ConfigureTimeSeriesPolicyOperation
# ConfigureTimeSeriesValueNamesOperation
# GetMultipleTimeSeriesOperation
# GetTimeSeriesOperation
# GetTimeSeriesStatisticsOperation
# RawTimeSeriesPolicy
# RemoveTimeSeriesPolicyOperation
# TimeSeriesBatchOperation
# TimeSeriesCollectionConfiguration
# TimeSeriesConfiguration
# TimeSeriesDetails
# TimeSeriesItemDetail
# TimeSeriesOperation
# TimeSeriesPolicy
# TimeSeriesRange
# TimeSeriesCountRange
# TimeSeriesRangeType
# TimeSeriesTimeRange
# TimeSeriesRangeResult
# TimeSeriesStatistics
# AbstractTimeSeriesRange

# todo: Auth
# AuthOptions

# todo: Types
# Callbacks
# Contracts
# Types

# todo: Queries
# WktField
# FacetSetup
# Facets
# HighlightingParameters
# Hightlightings
# ITimeSeriesQueryBuilder
# TimeSeriesAggregationResult
# TimeSeriesQueryBuilder
# TimeSeriesQueryResult
# TimeSeriesRangeAggregation
# TimeSeriesRawResult
# TypedTimeSeriesAggregationResult
# TypedTimeSeriesRangeAggregation
# TypedTimeSeriesRawResult

# todo: More Like This
# IMoreLikeThisBuilderBase
# MoreLikeThisStopWords

# todo: Suggestions
# ISuggestionOperations

# todo: Attachments
# Attachments

# todo: Analyzers
# DeleteAnalyzerOperation
# PutAnalyzersOperation

# todo: Changes
# IndexChange
# DatabaseChangesOptions
# DocumentChange
# TimeSeriesChange
# CounterChange
# IDatabaseChanges
# DatabaseChange
# OperationStatusChange
# IDatabaseChanges
# DatabaseChanges
# IConnectableChanges
# IChangesObservable
# ChangesObservable
# DatabaseConnectionState
# IChangesConnectionState

# todo: Smuggler
# DatabaseItemType
# DatabaseRecordItemType
# DatabaseSmuggler
# DatabaseSmugglerExportOptions
# IDatabaseSmugglerExportOptions
# DatabaseSmugglerImportOptions
# IDatabaseSmugglerImportOptions
# DatabaseSmugglerOptions
# IDatabaseSmugglerOptions

# todo: Certificates
# AddDatabaseNodeOperation
# PromoteDatabaseNodeOperation
# DeleteServerWideAnalyzerOperation
# PutServerWideAnalyzersOperation
# DocumentCompressionConfigurationResult
# UpdateDocumentsCompressionConfigurationOperation
# IServerWideTask
# DeleteServerWideTaskOperation
# SetDatabasesLockOperation
# ToggleServerWideTaskStateOperation
# GetServerWideExternalReplicationOperation
# PutServerWideExternalReplicationOperation
# ServerWideTaskResponse
# ServerWideExternalReplication
# DeleteServerWideSorterOperation
# PutServerWideSortersOperation
