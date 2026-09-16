from ravendb.documents.commands.batches import (
    BatchOptions,
    DeleteAttachmentCommandData,
    PatchCommandData,
    JsonPatchCommandData,
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
    ArchivedDataProcessingBehavior,
)
from ravendb.documents.indexes.abstract_index_creation_tasks import (
    AbstractIndexDefinitionBuilder,
    AbstractIndexCreationTask,
)
from ravendb.documents.indexes.spatial.configuration import AutoSpatialOptions
from ravendb.documents.operations.attachments import (
    DeleteAttachmentOperation,
    PutAttachmentOperation,
    GetAttachmentOperation,
    AttachmentRequest,
    ConfigureRemoteAttachmentsOperation,
    ConfigureRemoteAttachmentsOperationResult,
    GetRemoteAttachmentsConfigurationOperation,
    RemoteAttachmentsAzureSettings,
    RemoteAttachmentsConfiguration,
    RemoteAttachmentsDestinationConfiguration,
    RemoteAttachmentsS3Settings,
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
from ravendb.documents.operations.configuration.operations import (
    GetServerWideClientConfigurationOperation,
    PutServerWideClientConfigurationOperation,
    ClientConfiguration,
    GetClientConfigurationOperation,
    PutClientConfigurationOperation,
)
from ravendb.documents.operations.configuration.definitions import StudioConfiguration, StudioEnvironment

from ravendb.documents.operations.connection_strings import (
    ConnectionString,
    ConnectionStringUsage,
    ConnectionStringUsageKind,
)

# AI Operations
from ravendb.documents.ai import (
    AiOperations,
    AiConversation,
    AiConversationResult,
    ContentPart,
    TextPart,
    AiMessagePromptFields,
    AiMessagePromptTypes,
)
from ravendb.documents.operations.ai.agents import (
    AiAgentConfiguration,
    AiAgentConfigurationResult,
    AiAgentParameter,
    AiAgentParameterPolicy,
    AiAgentParameterValueType,
    AiAgentToolAction,
    AiAgentToolQuery,
    AiAgentToolSubAgent,
    AiAgentPersistenceConfiguration,
    AiAgentChatTrimmingConfiguration,
    AiAgentSummarizationByTokens,
    AiAgentTruncateChat,
    AiAgentHistoryConfiguration,
    RunConversationOperation,
    ConversationResult,
    AiAgentActionRequest,
    AiAgentActionRequestType,
    AiAgentActionResponse,
    AiAgentArtificialActionResponse,
    AiUsage,
    AiConversationCreationOptions,
    AiConversationParameter,
    AiConversationParameterOptions,
    GetAiAgentOperation,
    GetAiAgentsResponse,
    AddOrUpdateAiAgentOperation,
    DeleteAiAgentOperation,
    GetConversationMessagesOperation,
    GetConversationMessagesOptions,
    AiConversationDetailLevel,
    AiConversationMessage,
    AiConversationMessagesResult,
    AiMessageRole,
    AiToolCallResult,
)
from ravendb.documents.operations.ai import (
    ChunkingOptions,
    ChunkingMethod,
    EmbeddingPathConfiguration,
    EmbeddingsTransformation,
    EmbeddingsGenerationConfiguration,
    AddEmbeddingsGenerationOperation,
    UpdateEmbeddingsGenerationOperation,
)

from ravendb.documents.operations.etl.configuration import EtlConfiguration, RavenEtlConfiguration
from ravendb.documents.operations.etl.olap.connection import OlapEtlConfiguration
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
from ravendb.documents.operations.lazy.definition import LazyOperation
from ravendb.documents.operations.misc import DeleteByQueryOperation, GetOperationStateOperation, QueryOperationOptions
from ravendb.documents.conventions import SessionPatchBehavior
from ravendb.documents.operations.json_patch import (
    JsonPatchDocument,
    JsonPatchOperation,
    JsonPatchResult,
)
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
    ExternalReplicationBase,
    ReplicationNode,
    PullReplicationAsSink,
    PullReplicationDefinition,
    PullReplicationMode,
    PreventDeletionsMode,
    ReplicationHubAccess,
    DetailedReplicationHubAccess,
    ReplicationHubAccessResult,
    PullReplicationDefinitionAndCurrentConnections,
)
from ravendb.documents.operations.replication.pull_replication import (
    PutPullReplicationAsHubOperation,
    UpdatePullReplicationAsSinkOperation,
    UpdateExternalReplicationOperation,
    RegisterReplicationHubAccessOperation,
    UnregisterReplicationHubAccessOperation,
    GetReplicationHubAccessOperation,
    GetPullReplicationTasksInfoOperation,
)
from ravendb.documents.operations.ongoing_tasks import (
    GetOngoingTaskInfoOperation,
    OngoingTaskPullReplicationAsSink,
    OngoingTaskPullReplicationAsHub,
    OngoingTaskCdcSink,
    OngoingTaskQueueSink,
    OngoingTaskType,
)
from ravendb.documents.operations.queue_sink import (
    AddQueueSinkOperation,
    AddQueueSinkOperationResult,
    AzureServiceBusSinkSource,
    QueueSinkConfiguration,
    QueueSinkProcessState,
    QueueSinkScript,
    UpdateQueueSinkOperation,
    UpdateQueueSinkOperationResult,
)
from ravendb.documents.smuggler.common import (
    DatabaseItemType,
    DatabaseRecordItemType,
    DatabaseSmugglerExportOptions,
    DatabaseSmugglerImportOptions,
    DatabaseSmugglerOptions,
    ExportCompressionAlgorithm,
)
from ravendb.documents.smuggler.database_smuggler import DatabaseSmuggler
from ravendb.documents.operations.cdc_sink.schema import (
    CdcSinkSchemaRequest,
    CdcSinkSourceColumn,
    CdcSinkSourceForeignKey,
    CdcSinkSourceSchema,
    CdcSinkSourceTable,
    GetCdcSinkSchemaOperation,
)
from ravendb.documents.operations.cdc_sink.testing import (
    TestCdcSinkMappingOperation,
    TestCdcSinkMappingRequest,
    TestCdcSinkMappingResult,
    TestCdcSinkOperation,
    TestCdcSinkRowResult,
    TestCdcSinkRowSelector,
)
from ravendb.documents.smuggler.result import (
    Counts,
    CountsWithLastEtag,
    CountsWithLastEtagAndAttachments,
    CountsWithSkippedCountAndLastEtag,
    CountsWithSkippedCountAndLastEtagAndAttachments,
    DatabaseRecordProgress,
    SmugglerProgressBase,
    SmugglerResult,
)
from ravendb.documents.smuggler.database_smuggler import SmugglerOperation
from ravendb.exceptions.commercial import LicenseLimitException, LimitType
from ravendb.exceptions.raven_exceptions import QueryToolFailedException
from ravendb.documents.ai.ai_output_options import AiOutputOptions
from ravendb.documents.operations.cdc_sink import (
    AddCdcSinkOperation,
    AddCdcSinkOperationResult,
    CdcColumnMapping,
    CdcColumnType,
    CdcSinkConfiguration,
    CdcSinkEmbeddedTableConfig,
    CdcSinkLinkedTableConfig,
    CdcSinkOnDeleteConfig,
    CdcSinkPostgresSettings,
    CdcSinkProcessState,
    CdcSinkRelationType,
    CdcSinkTableConfig,
    CdcSinkTableLoadState,
    CdcSinkTaskState,
    UpdateCdcSinkOperation,
    UpdateCdcSinkOperationResult,
)
from ravendb.documents.operations.revisions import (
    RevisionsCollectionConfiguration,
    RevisionsConfiguration,
    RevisionsResult,
    ConfigureRevisionsOperation,
    ConfigureRevisionsOperationResult,
    GetRevisionsOperation,
    EnforceRevisionsConfigurationOperation,
    AdoptOrphanedRevisionsOperation,
    DeleteRevisionsOperation,
    RevertRevisionsByIdOperation,
    ConfigureRevisionsBinCleanerOperation,
    ConfigureRevisionsBinCleanerOperationResult,
    RevisionsBinConfiguration,
    RevisionsOperationParameters,
    RevisionsOperationContinuationParameters,
)
from ravendb.serverwide.operations.revisions import (
    ConfigureRevisionsForConflictsOperation,
    ConfigureRevisionsForConflictsResult,
)
from ravendb.documents.operations.statistics import (
    GetCollectionStatisticsOperation,
    CollectionStatistics,
    GetStatisticsOperation,
    DatabaseStatistics,
    IndexInformation,
    GetDetailedStatisticsOperation,
    DetailedDatabaseStatistics,
    GetEssentialStatisticsOperation,
    EssentialDatabaseStatistics,
    EssentialIndexInformation,
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
from ravendb.documents.queries.highlighting import HighlightingOptions, Highlightings, QueryHighlightings
from ravendb.documents.queries.index_query import IndexQuery
from ravendb.documents.queries.misc import SearchOperator
from ravendb.documents.queries.raven_document_query import RavenDocumentQuery
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
from ravendb.documents.session.document_session_operations.in_memory_document_session_operations import (
    InMemoryDocumentSessionOperations,
)
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
    OptimisticConcurrencyMode,
    OrderingType,
    NullsOrdering,
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
from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
from ravendb.json.result import BatchCommandResult
from ravendb.serverwide.commands import GetDatabaseTopologyCommand, GetClusterTopologyCommand
from ravendb.serverwide.misc import DocumentsCompressionConfiguration, DeletionInProgressStatus

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
    CertificateUsage,
    SsoIdentifier,
    SsoProvider,
)
from ravendb.serverwide.operations.connection_strings import (
    GetServerWideConnectionStringsOperation,
    GetServerWideConnectionStringsResult,
    PutServerWideConnectionStringOperation,
    PutServerWideConnectionStringResult,
    RemoveServerWideConnectionStringOperation,
    RemoveServerWideConnectionStringResult,
    ServerWideConnectionString,
    ServerWideConnectionStringUsage,
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

# The rest of the public surface, reachable straight from `ravendb` like everything above.
from ravendb.documents.bulk_insert_operation import (
    BulkInsertOperation,
    BulkInsertOptions,
)
from ravendb.documents.commands.batches import (
    CommandType,
    IndexBatchOptions,
    ReplicationBatchOptions,
)
from ravendb.documents.commands.crud import (
    ConditionalGetResult,
    PutResult,
)
from ravendb.documents.indexes.definitions import (
    AggregationOperation,
    AutoFieldIndexing,
    FieldIndexing,
    FieldStorage,
    FieldTermVector,
    GroupByArrayBehavior,
    IndexDefinitionBase,
    IndexDefinitionCompareDifferences,
    IndexErrors,
    IndexLockMode,
    IndexPriority,
    IndexRunningStatus,
    IndexState,
    IndexType,
    IndexingError,
    SearchEngineType,
    SortOptions,
)
from ravendb.documents.indexes.spatial.configuration import (
    AutoSpatialMethodType,
    SpatialFieldType,
    SpatialOptions,
    SpatialOptionsFactory,
    SpatialRelation,
    SpatialSearchStrategy,
    SpatialUnits,
)
from ravendb.documents.ai.ai_conversation import AiHandleErrorStrategy
from ravendb.documents.operations.ai.add_gen_ai_operation import AddGenAiOperation
from ravendb.documents.operations.ai.update_gen_ai_operation import UpdateGenAiOperation
from ravendb.documents.operations.ai.agents.ai_agent_configuration import AiAgentToolQueryOptions
from ravendb.documents.operations.backups.settings import (
    BackupEncryptionSettings,
    BackupType,
    GetBackupConfigurationScript,
    RetentionPolicy,
    CompressionLevel,
    EncryptionMode,
    S3StorageClass,
    SnapshotSettings,
)
from ravendb.documents.operations.compact import CompactDatabaseOperation
from ravendb.documents.operations.connection_string.get_connection_string_operation import (
    GetConnectionStringsOperation,
    GetConnectionStringsResult,
)
from ravendb.documents.operations.connection_string.put_connection_string_operation import (
    PutConnectionStringOperation,
    PutConnectionStringResult,
)
from ravendb.documents.operations.connection_string.remove_connection_string_operation import (
    RemoveConnectionStringOperation,
    RemoveConnectionStringResult,
)
from ravendb.documents.operations.counters import (
    CounterBatch,
    CounterBatchOperation,
    CounterDetail,
    CounterOperation,
    CounterOperationType,
    CountersDetail,
    DocumentCountersOperation,
    GetCountersOperation,
)
from ravendb.documents.operations.definitions import (
    IOperation,
    MaintenanceOperation,
    OperationExceptionResult,
    OperationIdResult,
    VoidMaintenanceOperation,
    VoidOperation,
)
from ravendb.documents.operations.etl.configuration import RavenConnectionString
from ravendb.documents.operations.etl.etl_operation_results import (
    AddEtlOperationResult,
    UpdateEtlOperationResult,
)
from ravendb.documents.operations.etl.olap.connection import OlapConnectionString
from ravendb.documents.operations.etl.queue.connection import (
    QueueBrokerType,
    QueueConnectionString,
)
from ravendb.documents.operations.etl.queue.amazon_sqs_connection_settings import (
    AmazonSqsConnectionSettings,
    AmazonSqsCredentials,
)
from ravendb.documents.operations.etl.queue.azure_queue_storage_connection_settings import (
    AzureQueueStorageConnectionSettings,
    EntraId,
    Passwordless,
)
from ravendb.documents.operations.etl.queue.azure_service_bus_connection_settings import (
    AzureServiceBusConnectionSettings,
    AzureServiceBusEntraId,
    AzureServiceBusPasswordless,
)
from ravendb.documents.operations.etl.queue.kafka_connection_settings import KafkaConnectionSettings
from ravendb.documents.operations.etl.queue.rabbit_mq_connection_settings import RabbitMqConnectionSettings
from ravendb.documents.operations.etl.sql import SqlConnectionString
from ravendb.documents.operations.expiration.operations import (
    ConfigureExpirationOperation,
    ConfigureExpirationOperationResult,
)
from ravendb.documents.operations.identities import (
    GetIdentitiesOperation,
    NextIdentityForOperation,
    SeedIdentityForOperation,
)
from ravendb.documents.operations.indexes import (
    DeleteIndexErrorsOperation,
    IndexStatus,
    ResetIndexOperation,
)
from ravendb.documents.operations.ongoing_tasks import (
    DeleteOngoingTaskOperation,
    NodeId,
    OngoingTask,
    OngoingTaskConnectionStatus,
    OngoingTaskEmbeddingsGeneration,
    OngoingTaskGenAi,
    OngoingTaskState,
    ToggleOngoingTaskStateOperation,
)
from ravendb.documents.operations.operation import Operation
from ravendb.documents.operations.refresh.configuration import (
    ConfigureRefreshOperation,
    ConfigureRefreshOperationResult,
)
from ravendb.documents.operations.replication.definitions import ReplicationType
from ravendb.documents.operations.revisions import RevisionIncludeResult
from ravendb.documents.operations.schema_validation import (
    ConfigureSchemaValidationOperation,
    ConfigureSchemaValidationOperationResult,
    GetSchemaValidationConfiguration,
    SchemaDefinition,
    SchemaValidationConfiguration,
    StartSchemaValidationOperation,
    ValidateSchemaProgress,
    ValidateSchemaResult,
)
from ravendb.documents.operations.server_misc import (
    DisableDatabaseToggleResult,
    ToggleDatabasesStateOperation,
)
from ravendb.documents.operations.sorters import (
    DeleteSorterOperation,
    PutSortersOperation,
)
from ravendb.documents.operations.statistics import (
    CollectionDetails,
    DetailedCollectionStatistics,
    GetDetailedCollectionStatisticsOperation,
)
from ravendb.documents.operations.time_series import (
    ConfigureRawTimeSeriesPolicyOperation,
    ConfigureTimeSeriesOperation,
    ConfigureTimeSeriesOperationResult,
    ConfigureTimeSeriesPolicyOperation,
    ConfigureTimeSeriesValueNamesOperation,
    GetMultipleTimeSeriesOperation,
    GetTimeSeriesOperation,
    GetTimeSeriesStatisticsOperation,
    RawTimeSeriesPolicy,
    RemoveTimeSeriesPolicyOperation,
    TimeSeriesBatchOperation,
    TimeSeriesCollectionConfiguration,
    TimeSeriesConfiguration,
    TimeSeriesDetails,
    TimeSeriesItemDetail,
    TimeSeriesOperation,
    TimeSeriesPolicy,
    TimeSeriesRangeResult,
    TimeSeriesStatistics,
)
from ravendb.exceptions.raven_exceptions import (
    AiException,
    BadResponseException,
    ClientVersionMismatchException,
    ConcurrencyException,
    ConflictException,
    IndexCompactionInProgressException,
    InsufficientQuotaException,
    MissingAiAgentParameterException,
    PortInUseException,
    RateLimitException,
    RavenException,
    RefusedToAnswerException,
    ReplicationHubNotFoundException,
    SchemaValidationException,
    TooManyRequestsException,
    TooManyTokensException,
    UnsuccessfulAiRequestException,
)
from ravendb.http.misc import (
    AggressiveCacheMode,
    ResponseDisposeHandling,
)
from ravendb.serverwide.misc import CompactSettings
from ravendb.serverwide.operations.analyzers import (
    DeleteServerWideAnalyzerOperation,
    PutServerWideAnalyzersOperation,
)
from ravendb.serverwide.operations.common import (
    AddDatabaseNodeOperation,
    DatabasePromotionStatus,
    DatabasePutResult,
    DatabaseSettings,
    DeleteDatabaseOperation,
    DeleteDatabaseResult,
    ModifyOngoingTaskResult,
    PromoteDatabaseNodeOperation,
    ReorderDatabaseMembersOperation,
    ServerOperation,
    ServerWideOperation,
    VoidServerOperation,
)
from ravendb.serverwide.operations.configuration import (
    DeleteServerWideTaskOperation,
    GetDatabaseSettingsOperation,
    GetServerWideBackupConfigurationOperation,
    GetServerWideBackupConfigurationsOperation,
    PutDatabaseSettingsOperation,
    PutServerWideBackupConfigurationOperation,
    ServerWideBackupConfiguration,
)
from ravendb.serverwide.operations.documents_compression import (
    DocumentCompressionConfigurationResult,
    UpdateDocumentsCompressionConfigurationOperation,
)
from ravendb.serverwide.operations.logs import (
    AdminLogsConfiguration,
    AuditLogsConfiguration,
    GetLogsConfigurationOperation,
    GetLogsConfigurationResult,
    LogFilter,
    LogFilterAction,
    LogLevel,
    LogsConfiguration,
    MicrosoftLogsConfiguration,
    SetLogsConfigurationOperation,
)
from ravendb.serverwide.operations.ongoing_tasks import (
    IServerWideTask,
    ServerWideTaskResponse,
    SetDatabasesLockOperation,
)
from ravendb.serverwide.operations.sorters import (
    DeleteServerWideSorterOperation,
    PutServerWideSortersOperation,
)
from ravendb.changes.database_changes import DatabaseChanges
from ravendb.changes.observers import (
    ActionObserver,
    Observable,
)
from ravendb.changes.types import (
    CounterChange,
    CounterChangeTypes,
    DatabaseChange,
    DocumentChange,
    DocumentChangeType,
    IndexChange,
    IndexChangeTypes,
    OperationStatusChange,
    TimeSeriesChange,
    TimeSeriesChangeTypes,
    TopologyChange,
)
from ravendb.documents.commands.stream import (
    StreamResult,
    StreamResultResponse,
)
from ravendb.documents.commands.subscriptions import UpdateSubscriptionResult
from ravendb.documents.indexes.abstract_index_creation_tasks import AbstractJavaScriptIndexCreationTask
from ravendb.documents.indexes.counters import (
    AbstractCountersIndexCreationTask,
    AbstractGenericCountersIndexCreationTask,
    CountersIndexDefinition,
    CountersIndexDefinitionBuilder,
)
from ravendb.documents.indexes.stats import IndexStats
from ravendb.documents.indexes.time_series import (
    AbstractGenericTimeSeriesIndexCreationTask,
    AbstractMultiMapTimeSeriesIndexCreationTask,
    AbstractTimeSeriesIndexCreationTask,
    TimeSeriesIndexDefinition,
    TimeSeriesIndexDefinitionBuilder,
)
from ravendb.documents.operations.etl.transformation import Transformation
from ravendb.documents.operations.lazy.revisions import (
    LazyRevisionOperation,
    LazyRevisionOperations,
)
from ravendb.documents.queries.facets.definitions import FacetSetup
from ravendb.documents.queries.more_like_this import MoreLikeThisStopWords
from ravendb.documents.queries.spatial import WktField
from ravendb.documents.queries.time_series import (
    TimeSeriesAggregationResult,
    TimeSeriesQueryBuilder,
    TimeSeriesQueryResult,
    TimeSeriesRangeAggregation,
    TimeSeriesRawResult,
    TypedTimeSeriesAggregationResult,
    TypedTimeSeriesRangeAggregation,
    TypedTimeSeriesRawResult,
)
from ravendb.documents.session.cluster_transaction_operation import LazyClusterTransactionOperations
from ravendb.documents.session.document_session import (
    SessionDocumentCounters,
    SessionDocumentRollupTypedTimeSeries,
    SessionDocumentTimeSeries,
    SessionDocumentTypedTimeSeries,
    SessionTimeSeriesBase,
)
from ravendb.documents.session.document_session_revisions import (
    DocumentSessionRevisions,
    DocumentSessionRevisionsBase,
)
from ravendb.documents.session.loaders.include import (
    SubscriptionIncludeBuilder,
    TimeSeriesIncludeBuilder,
)
from ravendb.documents.session.operations.lazy import (
    LazyGetCompareExchangeValueOperation,
    LazyGetCompareExchangeValuesOperation,
)
from ravendb.documents.session.operations.operations import (
    GetRevisionOperation,
    GetRevisionsCountOperation,
)
from ravendb.documents.session.operations.stream import StreamOperation
from ravendb.documents.session.stream_statistics import StreamQueryStatistics
from ravendb.documents.session.time_series import (
    AbstractTimeSeriesRange,
    TimeSeriesCountRange,
    TimeSeriesEntry,
    TimeSeriesRange,
    TimeSeriesRangeType,
    TimeSeriesTimeRange,
    TypedTimeSeriesEntry,
    TypedTimeSeriesRollupEntry,
)
from ravendb.documents.subscriptions.document_subscriptions import DocumentSubscriptions
from ravendb.documents.subscriptions.options import (
    SubscriptionCreationOptions,
    SubscriptionOpeningStrategy,
    SubscriptionUpdateOptions,
    SubscriptionWorkerOptions,
)
from ravendb.documents.subscriptions.revision import Revision
from ravendb.documents.subscriptions.state import SubscriptionState
from ravendb.documents.subscriptions.worker import (
    SubscriptionBatch,
    SubscriptionWorker,
)
from ravendb.documents.time_series import TimeSeriesOperations

# todo: Serverwide
# UpdateDatabaseOperation
# SetDatabaseDynamicDistributionOperation
# UpdateUnusedDatabasesOperation

# todo: Serverwide Operations
# DeleteDatabasesOperation
# DeleteServerWideBackupConfigurationOperation
# AddClusterNodeCommand
# ModifyConflictSolverOperation

# todo: Operations and Commands
# BackupTaskType
# DatabaseHealthCheckOperation
# GetServerWideExternalReplicationsResponse
# GetConflictsCommand
# SqlEtlTable
# OlapEtlFileFormat
# OlapEtlTable
# AddEtlOperation
# UpdateEtlOperation
# ResetEtlOperation
# RunningBackup
# NextBackup
# StartTransactionsRecordingOperation
# StopTransactionsRecordingOperation

# todo: backup
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
# RestoreBackupConfigurationBase
# RestoreFromAzureConfiguration
# RestoreFromGoogleCloudConfiguration
# RestoreFromS3Configuration
# RestoreType

# todo: Indexes
# AbstractJavaScriptMultiMapIndexCreationTask
# AbstractRawJavaScriptIndexCreationTask
# AbstractMultiMapCountersIndexCreationTask
# AbstractRawJavaScriptCountersIndexCreationTask
# AbstractRawJavaScriptTimeSeriesIndexCreationTask

# todo: TimeSeries
# AggregationType

# todo: Analyzers
# DeleteAnalyzerOperation
# PutAnalyzersOperation

# todo: Server-wide tasks
# ToggleServerWideTaskStateOperation
# GetServerWideExternalReplicationOperation
# PutServerWideExternalReplicationOperation
# ServerWideExternalReplication
