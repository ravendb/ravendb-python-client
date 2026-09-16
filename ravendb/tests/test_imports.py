from ravendb.tests.test_base import TestBase


# noinspection PyUnresolvedReferences
class TestImports(TestBase):
    def setUp(self):
        super(TestImports, self).setUp()

    def test_imports_at_top_level(self):
        from ravendb import AggressiveCacheOptions
        from ravendb import ClusterRequestExecutor
        from ravendb import ClusterTopology
        from ravendb import CurrentIndexAndNode
        from ravendb import CurrentIndexAndNodeAndEtag
        from ravendb import Broadcast
        from ravendb import RaftCommand
        from ravendb import NodeSelector
        from ravendb import LoadBalanceBehavior
        from ravendb import RavenCommand
        from ravendb import ReadBalanceBehavior
        from ravendb import RequestExecutor
        from ravendb import ServerNode
        from ravendb import Topology
        from ravendb import UpdateTopologyParameters
        from ravendb import ConnectionString
        from ravendb import DocumentsCompressionConfiguration
        from ravendb import DeletionInProgressStatus
        from ravendb import BuildNumber
        from ravendb import GetBuildNumberOperation

        from ravendb import ReorderDatabaseMembersOperation
        from ravendb import ConfigureRevisionsForConflictsOperation
        from ravendb import ConfigureRevisionsForConflictsResult

        # from ravendb import UpdateDatabaseOperation
        from ravendb import GetServerWideBackupConfigurationOperation

        # from ravendb import SetDatabaseDynamicDistributionOperation
        # from ravendb import UpdateUnusedDatabasesOperation
        # from ravendb import DeleteDatabasesOperation
        from ravendb import GetDatabaseNamesOperation
        from ravendb import GetServerWideOperationStateOperation

        from ravendb import CertificateMetadata
        from ravendb import EditClientCertificateOperation
        from ravendb import ReplaceClusterCertificateOperation
        from ravendb import GetCertificateMetadataOperation
        from ravendb import GetCertificatesMetadataOperation
        from ravendb import GetServerWideClientConfigurationOperation
        from ravendb import PutServerWideClientConfigurationOperation

        from ravendb import GetLogsConfigurationResult
        from ravendb import GetLogsConfigurationOperation
        from ravendb import SetLogsConfigurationOperation

        # from ravendb import DeleteServerWideBackupConfigurationOperation
        from ravendb import GetServerWideClientConfigurationOperation

        from ravendb import GetServerWideBackupConfigurationsOperation
        from ravendb import PutServerWideBackupConfigurationOperation
        from ravendb import ServerWideBackupConfiguration
        from ravendb import DatabaseSettings
        from ravendb import GetDatabaseSettingsOperation
        from ravendb import PutDatabaseSettingsOperation
        from ravendb import GetDatabaseTopologyCommand
        from ravendb import GetClusterTopologyCommand

        # from ravendb import AddClusterNodeCommand
        from ravendb import CreateDatabaseOperation

        # from ravendb import ModifyConflictSolverOperation
        from ravendb import ConnectionString

        from ravendb import BulkInsertOperation
        from ravendb import CollectionDetails
        from ravendb import BackupConfiguration

        # from ravendb import BackupTaskType
        # from ravendb import DatabaseHealthCheckOperation
        from ravendb import DetailedCollectionStatistics
        from ravendb import GetDetailedCollectionStatisticsOperation
        from ravendb import CompactDatabaseOperation
        from ravendb import PutConnectionStringOperation
        from ravendb import PatchOperation

        from ravendb import DeleteSorterOperation
        from ravendb import PutSortersOperation
        from ravendb import PatchByQueryOperation
        from ravendb import PutCompareExchangeValueOperation
        from ravendb import GetCompareExchangeValueOperation
        from ravendb import CompareExchangeResult
        from ravendb import CompareExchangeValue
        from ravendb import CompareExchangeValueResultParser
        from ravendb import GetCompareExchangeValuesOperation
        from ravendb import DeleteCompareExchangeValueOperation
        from ravendb import CompareExchangeSessionValue

        from ravendb import CompareExchangeValueState
        from ravendb import DeleteByQueryOperation
        from ravendb import GetCollectionStatisticsOperation
        from ravendb import CollectionStatistics

        # from ravendb import GetServerWideExternalReplicationsResponse
        from ravendb import DeleteDocumentCommand

        from ravendb import GetIdentitiesOperation
        from ravendb import GetStatisticsOperation
        from ravendb import DatabaseStatistics
        from ravendb import GetOperationStateOperation
        from ravendb import IndexInformation
        from ravendb import MaintenanceOperationExecutor

        from ravendb import ClientConfiguration
        from ravendb import GetClientConfigurationOperation
        from ravendb import PutClientConfigurationOperation
        from ravendb import PutDocumentCommand
        from ravendb import GetIndexNamesOperation

        from ravendb import DeleteIndexErrorsOperation
        from ravendb import DisableIndexOperation
        from ravendb import EnableIndexOperation
        from ravendb import GetIndexingStatusOperation
        from ravendb import GetIndexesStatisticsOperation
        from ravendb import GetIndexStatisticsOperation
        from ravendb import GetIndexesOperation
        from ravendb import GetTermsOperation
        from ravendb import IndexHasChangedOperation
        from ravendb import PutIndexesOperation
        from ravendb import StopIndexingOperation
        from ravendb import StartIndexingOperation
        from ravendb import StopIndexOperation
        from ravendb import StartIndexOperation

        from ravendb import ResetIndexOperation
        from ravendb import DeleteIndexOperation

        from ravendb import NextIdentityForOperation
        from ravendb import SeedIdentityForOperation
        from ravendb import UpdateExternalReplicationOperation
        from ravendb import PullReplicationDefinitionAndCurrentConnections
        from ravendb import PutPullReplicationAsHubOperation
        from ravendb import DetailedReplicationHubAccess
        from ravendb import GetReplicationHubAccessOperation
        from ravendb import PreventDeletionsMode
        from ravendb import PullReplicationMode
        from ravendb import RegisterReplicationHubAccessOperation
        from ravendb import ReplicationHubAccess
        from ravendb import ReplicationHubAccessResult
        from ravendb import UnregisterReplicationHubAccessOperation
        from ravendb import UpdatePullReplicationAsSinkOperation
        from ravendb import GetPullReplicationTasksInfoOperation

        # from ravendb import GetConflictsCommand
        from ravendb import SetIndexesLockOperation
        from ravendb import SetIndexesPriorityOperation
        from ravendb import PatchRequest
        from ravendb import GetDetailedStatisticsOperation
        from ravendb import BatchOptions
        from ravendb import DeleteAttachmentCommandData
        from ravendb import PatchCommandData
        from ravendb import PutAttachmentCommandData

        from ravendb import CommandData
        from ravendb import GetDatabaseRecordOperation

        from ravendb import StreamResultResponse
        from ravendb import StreamResult
        from ravendb import BatchOperation

        from ravendb import GetRevisionOperation
        from ravendb import GetRevisionsCountOperation
        from ravendb import Lazy

        from ravendb import LazyAggregationQueryOperation
        from ravendb import LazyLoadOperation
        from ravendb import LazyQueryOperation
        from ravendb import LazySessionOperations
        from ravendb import LazyStartsWithOperation
        from ravendb import LazySuggestionQueryOperation

        from ravendb import LazyClusterTransactionOperations
        from ravendb import LazyGetCompareExchangeValueOperation
        from ravendb import LazyGetCompareExchangeValuesOperation
        from ravendb import LazyConditionalLoadOperation

        from ravendb import LazyRevisionOperation
        from ravendb import LazyRevisionOperations
        from ravendb import LoadOperation
        from ravendb import LoadStartingWithOperation
        from ravendb import MultiGetOperation
        from ravendb import QueryOperation

        from ravendb import StreamOperation
        from ravendb import DeleteAttachmentOperation
        from ravendb import PutAttachmentOperation
        from ravendb import PatchResult
        from ravendb import PatchStatus

        from ravendb import ConfigureRevisionsOperation
        from ravendb import ConfigureRevisionsOperationResult
        from ravendb import GetRevisionsOperation
        from ravendb import RevisionsResult
        from ravendb import EnforceRevisionsConfigurationOperation
        from ravendb import AdoptOrphanedRevisionsOperation
        from ravendb import DeleteRevisionsOperation
        from ravendb import RevertRevisionsByIdOperation
        from ravendb import ConfigureRevisionsBinCleanerOperation
        from ravendb import ConfigureRevisionsBinCleanerOperationResult
        from ravendb import RevisionsBinConfiguration
        from ravendb import RevisionsOperationParameters
        from ravendb import RevisionsOperationContinuationParameters
        from ravendb import RevisionsCollectionConfiguration
        from ravendb import RevisionsConfiguration
        from ravendb import DetailedDatabaseStatistics
        from ravendb import SessionOperationExecutor
        from ravendb import StudioConfiguration
        from ravendb import StudioEnvironment

        from ravendb import GetConnectionStringsOperation
        from ravendb import RemoveConnectionStringOperation
        from ravendb import EtlConfiguration
        from ravendb import RavenEtlConfiguration
        from ravendb import SqlEtlConfiguration

        # from ravendb import SqlEtlTable
        from ravendb import OlapEtlConfiguration

        # from ravendb import OlapEtlFileFormat
        # from ravendb import OlapEtlTable
        from ravendb import Transformation
        from ravendb import ExpirationConfiguration
        from ravendb import PullReplicationAsSink
        from ravendb import PullReplicationDefinition

        # from ravendb import AddEtlOperation
        # from ravendb import UpdateEtlOperation
        # from ravendb import ResetEtlOperation
        from ravendb import DisableDatabaseToggleResult
        from ravendb import ConfigureExpirationOperation
        from ravendb import DeleteOngoingTaskOperation
        from ravendb import OngoingTaskPullReplicationAsSink
        from ravendb import OngoingTaskPullReplicationAsHub

        from ravendb import OngoingTaskType

        # from ravendb import RunningBackup
        # from ravendb import NextBackup
        from ravendb import GetOngoingTaskInfoOperation
        from ravendb import ToggleOngoingTaskStateOperation
        from ravendb import ConfigureRefreshOperation
        from ravendb import RefreshConfiguration

        from ravendb import ConfigureRefreshOperationResult
        from ravendb import ToggleDatabasesStateOperation

        # from ravendb import StartTransactionsRecordingOperation
        # from ravendb import StopTransactionsRecordingOperation
        from ravendb import AmazonSettings
        from ravendb import AzureSettings

        from ravendb import BackupEncryptionSettings
        from ravendb import BackupEncryptionSettings
        from ravendb import FtpSettings
        from ravendb import GlacierSettings
        from ravendb import LocalSettings
        from ravendb import PeriodicBackupConfiguration
        from ravendb import S3Settings
        from ravendb import BackupSettings
        from ravendb import BackupStatus

        # from ravendb import GetPeriodicBackupStatusOperation
        # from ravendb import GetPeriodicBackupStatusOperationResult
        # from ravendb import LastRaftIndex
        # from ravendb import PeriodicBackupStatus
        # from ravendb import RestoreBackupConfiguration
        # from ravendb import RestoreBackupOperation
        # from ravendb import StartBackupOperation
        # from ravendb import StartBackupOperationResult
        # from ravendb import UpdatePeriodicBackupOperation
        # from ravendb import UpdatePeriodicBackupOperationResult
        # from ravendb import UploadProgress
        # from ravendb import UploadState
        from ravendb import CompressionLevel
        from ravendb import GetBackupConfigurationScript
        from ravendb import GoogleCloudSettings

        # from ravendb import RestoreBackupConfigurationBase
        # from ravendb import RestoreFromAzureConfiguration
        # from ravendb import RestoreFromGoogleCloudConfiguration
        # from ravendb import RestoreFromS3Configuration
        # from ravendb import RestoreType
        from ravendb import RetentionPolicy
        from ravendb import GetIndexOperation
        from ravendb import GetIndexErrorsOperation

        from ravendb import IndexDeploymentMode
        from ravendb import IndexDefinition
        from ravendb import AbstractCommonApiForIndexes
        from ravendb import AbstractIndexDefinitionBuilder

        from ravendb import AdditionalAssembly

        from ravendb import IndexFieldOptions

        from ravendb import IndexingStatus
        from ravendb import RollingIndex
        from ravendb import RollingIndexDeployment
        from ravendb import RollingIndexState

        from ravendb import IndexStats
        from ravendb import IndexSourceType

        from ravendb import IndexDefinitionBase
        from ravendb import AnalyzerDefinition

        from ravendb import AbstractJavaScriptIndexCreationTask

        # from ravendb import AbstractJavaScriptMultiMapIndexCreationTask
        # from ravendb import AbstractRawJavaScriptIndexCreationTask
        from ravendb import AutoIndexDefinition
        from ravendb import AutoIndexFieldOptions
        from ravendb import AutoSpatialOptions

        from ravendb import AbstractCountersIndexCreationTask
        from ravendb import AbstractGenericCountersIndexCreationTask

        # from ravendb import AbstractMultiMapCountersIndexCreationTask
        # from ravendb import AbstractRawJavaScriptCountersIndexCreationTask
        from ravendb import CountersIndexDefinition
        from ravendb import CountersIndexDefinitionBuilder
        from ravendb import AbstractGenericTimeSeriesIndexCreationTask
        from ravendb import AbstractMultiMapTimeSeriesIndexCreationTask

        # from ravendb import AbstractRawJavaScriptTimeSeriesIndexCreationTask
        from ravendb import AbstractTimeSeriesIndexCreationTask
        from ravendb import TimeSeriesIndexDefinition
        from ravendb import TimeSeriesIndexDefinitionBuilder
        from ravendb import ExternalReplication
        from ravendb import ReplicationNode
        from ravendb import ExternalReplicationBase

        from ravendb import DocumentStore
        from ravendb import DocumentStoreBase
        from ravendb import IdTypeAndName

        from ravendb import SubscriptionBatch
        from ravendb import DocumentSubscriptions
        from ravendb import SubscriptionWorker
        from ravendb import SubscriptionWorkerOptions
        from ravendb import SubscriptionCreationOptions
        from ravendb import Revision
        from ravendb import SubscriptionState
        from ravendb import SubscriptionCreationOptions
        from ravendb import UpdateSubscriptionResult
        from ravendb import SubscriptionOpeningStrategy
        from ravendb import SubscriptionUpdateOptions
        from ravendb import AbstractDocumentQuery
        from ravendb import CmpXchg
        from ravendb import DocumentInfo
        from ravendb import DocumentQuery
        from ravendb import DocumentQueryHelper

        from ravendb import DocumentsChanges
        from ravendb import DocumentSession
        from ravendb import EntityToJson
        from ravendb import ForceRevisionStrategy
        from ravendb import GroupByDocumentQuery
        from ravendb import GroupByField

        from ravendb import IncludesUtil
        from ravendb import InMemoryDocumentSessionOperations

        from ravendb import MethodCall
        from ravendb import OrderingType

        from ravendb import QueryStatistics

        from ravendb import StreamQueryStatistics
        from ravendb import RawDocumentQuery

        from ravendb import WhereParams

        from ravendb import MetadataAsDictionary

        from ravendb import SessionDocumentRollupTypedTimeSeries
        from ravendb import SessionDocumentTimeSeries
        from ravendb import SessionDocumentTypedTimeSeries
        from ravendb import SessionTimeSeriesBase
        from ravendb import TimeSeriesIncludeBuilder
        from ravendb import SubscriptionIncludeBuilder
        from ravendb import LoaderWithInclude

        from ravendb import LazyMultiLoaderWithInclude
        from ravendb import MultiLoaderWithInclude
        from ravendb import DocumentQueryCustomization

        from ravendb import DocumentSessionRevisions
        from ravendb import DocumentSessionRevisionsBase
        from ravendb import ResponseTimeInformation

        from ravendb import TransactionMode
        from ravendb import ConditionalLoadResult

        from ravendb import ClusterTransactionOperations

        from ravendb import IncludeBuilder
        from ravendb import IncludeBuilderBase

        from ravendb import QueryIncludeBuilder
        from ravendb import QueryIncludeBuilder
        from ravendb import BatchCommandResult

        from ravendb import SessionDocumentCounters
        from ravendb import TimeSeriesEntry
        from ravendb import TypedTimeSeriesEntry
        from ravendb import TypedTimeSeriesRollupEntry
        from ravendb import TimeSeriesOperations
        from ravendb import StreamResult
        from ravendb import SessionOptions
        from ravendb import CommandData
        from ravendb import CopyAttachmentCommandData
        from ravendb import DeleteAttachmentCommandData
        from ravendb import MoveAttachmentCommandData
        from ravendb import PutAttachmentCommandData
        from ravendb import BatchPatchCommandData
        from ravendb import CountersBatchCommandData
        from ravendb import PatchCommandData
        from ravendb import PutCompareExchangeCommandData
        from ravendb import DeleteCompareExchangeCommandData
        from ravendb import Lazy

        from ravendb import CounterBatch
        from ravendb import GetCountersOperation
        from ravendb import CounterBatchOperation
        from ravendb import CounterOperationType
        from ravendb import CounterOperation
        from ravendb import DocumentCountersOperation
        from ravendb import CounterDetail
        from ravendb import CountersDetail

        # from ravendb import AggregationType
        from ravendb import ConfigureRawTimeSeriesPolicyOperation
        from ravendb import ConfigureTimeSeriesOperation
        from ravendb import ConfigureTimeSeriesOperationResult
        from ravendb import ConfigureTimeSeriesPolicyOperation
        from ravendb import ConfigureTimeSeriesValueNamesOperation
        from ravendb import GetMultipleTimeSeriesOperation
        from ravendb import GetTimeSeriesOperation
        from ravendb import GetTimeSeriesStatisticsOperation
        from ravendb import RawTimeSeriesPolicy
        from ravendb import RemoveTimeSeriesPolicyOperation
        from ravendb import TimeSeriesBatchOperation
        from ravendb import TimeSeriesCollectionConfiguration
        from ravendb import TimeSeriesConfiguration
        from ravendb import TimeSeriesDetails
        from ravendb import TimeSeriesItemDetail
        from ravendb import TimeSeriesOperation
        from ravendb import TimeSeriesPolicy
        from ravendb import TimeSeriesRange
        from ravendb import TimeSeriesCountRange
        from ravendb import TimeSeriesRangeType
        from ravendb import TimeSeriesTimeRange
        from ravendb import TimeSeriesRangeResult
        from ravendb import TimeSeriesStatistics
        from ravendb import AbstractTimeSeriesRange
        from ravendb import IndexQuery
        from ravendb import GroupBy
        from ravendb import QueryOperator
        from ravendb import SearchOperator

        from ravendb import GroupByMethod
        from ravendb import ProjectionBehavior
        from ravendb import SpatialCriteriaFactory
        from ravendb import SpatialCriteria
        from ravendb import CircleCriteria
        from ravendb import DynamicSpatialField
        from ravendb import WktCriteria
        from ravendb import PointField

        from ravendb import WktField
        from ravendb import RangeBuilder
        from ravendb import FacetBuilder
        from ravendb import FacetAggregationField
        from ravendb import Facet
        from ravendb import RangeFacet
        from ravendb import FacetBase

        from ravendb import FacetSetup
        from ravendb import AggregationRawDocumentQuery
        from ravendb import QueryData
        from ravendb import QueryOperationOptions
        from ravendb import QueryResult
        from ravendb import HighlightingOptions

        from ravendb import QueryTimings
        from ravendb import AggregationDocumentQuery
        from ravendb import AggregationQueryBase
        from ravendb import GenericRangeFacet
        from ravendb import FacetBuilder
        from ravendb import FacetOperations
        from ravendb import ExplanationOptions
        from ravendb import Explanations
        from ravendb import QueryHighlightings
        from ravendb import SorterDefinition

        from ravendb import TimeSeriesAggregationResult
        from ravendb import TimeSeriesQueryBuilder
        from ravendb import TimeSeriesQueryResult
        from ravendb import TimeSeriesRangeAggregation
        from ravendb import TimeSeriesRawResult
        from ravendb import TypedTimeSeriesAggregationResult
        from ravendb import TypedTimeSeriesRangeAggregation
        from ravendb import TypedTimeSeriesRawResult
        from ravendb import MoreLikeThisOperations
        from ravendb import MoreLikeThisBase
        from ravendb import MoreLikeThisBuilder
        from ravendb import MoreLikeThisOptions

        from ravendb import MoreLikeThisStopWords
        from ravendb import SuggestionBuilder
        from ravendb import SuggestionDocumentQuery

        from ravendb import StringDistanceTypes
        from ravendb import SuggestionBuilder
        from ravendb import SuggestionDocumentQuery
        from ravendb import SuggestionOptions
        from ravendb import SuggestionBase
        from ravendb import SuggestionResult
        from ravendb import SuggestionSortMode

        from ravendb import GetAttachmentOperation
        from ravendb import AttachmentRequest

        # from ravendb import DeleteAnalyzerOperation
        # from ravendb import PutAnalyzersOperation
        from ravendb import IndexChange
        from ravendb import DocumentChange
        from ravendb import TimeSeriesChange
        from ravendb import CounterChange
        from ravendb import DatabaseChange
        from ravendb import OperationStatusChange
        from ravendb import DatabaseChanges
        from ravendb import DatabaseItemType
        from ravendb import DatabaseRecordItemType
        from ravendb import DatabaseSmuggler
        from ravendb import DatabaseSmugglerExportOptions
        from ravendb import DatabaseSmugglerImportOptions
        from ravendb import DatabaseSmugglerOptions
        from ravendb import CertificateDefinition
        from ravendb import CertificateRawData
        from ravendb import CreateClientCertificateOperation
        from ravendb import DatabaseAccess
        from ravendb import DeleteCertificateOperation
        from ravendb import GetCertificateOperation
        from ravendb import GetCertificatesOperation
        from ravendb import GetCertificatesResponse
        from ravendb import PutClientCertificateOperation
        from ravendb import SecurityClearance

        from ravendb import AddDatabaseNodeOperation
        from ravendb import PromoteDatabaseNodeOperation
        from ravendb import DeleteServerWideAnalyzerOperation
        from ravendb import PutServerWideAnalyzersOperation
        from ravendb import DocumentCompressionConfigurationResult
        from ravendb import UpdateDocumentsCompressionConfigurationOperation
        from ravendb import IServerWideTask
        from ravendb import DeleteServerWideTaskOperation
        from ravendb import SetDatabasesLockOperation

        # from ravendb import ToggleServerWideTaskStateOperation
        # from ravendb import GetServerWideExternalReplicationOperation
        # from ravendb import PutServerWideExternalReplicationOperation
        from ravendb import ServerWideTaskResponse

        # from ravendb import ServerWideExternalReplication
        from ravendb import DeleteServerWideSorterOperation
        from ravendb import PutServerWideSortersOperation
        from ravendb import DocumentStore

        return

    def test_the_rest_of_the_public_surface_imports(self):
        # Everything else ravendb/__init__.py binds. Dropping an export fails here.
        from ravendb import AbstractIndexCreationTask
        from ravendb import ActionObserver
        from ravendb import AddCdcSinkOperation
        from ravendb import AddCdcSinkOperationResult
        from ravendb import AddEmbeddingsGenerationOperation
        from ravendb import AddEtlOperationResult
        from ravendb import AddGenAiOperation
        from ravendb import AddOrUpdateAiAgentOperation
        from ravendb import AddQueueSinkOperation
        from ravendb import AddQueueSinkOperationResult
        from ravendb import AdminLogsConfiguration
        from ravendb import AggregationOperation
        from ravendb import AggressiveCacheMode
        from ravendb import AiAgentActionRequest
        from ravendb import AiAgentActionRequestType
        from ravendb import AiAgentActionResponse
        from ravendb import AiAgentArtificialActionResponse
        from ravendb import AiAgentChatTrimmingConfiguration
        from ravendb import AiAgentConfiguration
        from ravendb import AiAgentConfigurationResult
        from ravendb import AiAgentHistoryConfiguration
        from ravendb import AiAgentParameter
        from ravendb import AiAgentParameterPolicy
        from ravendb import AiAgentParameterValueType
        from ravendb import AiAgentPersistenceConfiguration
        from ravendb import AiAgentSummarizationByTokens
        from ravendb import AiAgentToolAction
        from ravendb import AiAgentToolQuery
        from ravendb import AiAgentToolQueryOptions
        from ravendb import AiAgentToolSubAgent
        from ravendb import AiAgentTruncateChat
        from ravendb import AiConversation
        from ravendb import AiConversationCreationOptions
        from ravendb import AiConversationDetailLevel
        from ravendb import AiConversationMessage
        from ravendb import AiConversationMessagesResult
        from ravendb import AiConversationParameter
        from ravendb import AiConversationParameterOptions
        from ravendb import AiConversationResult
        from ravendb import AiException
        from ravendb import AiHandleErrorStrategy
        from ravendb import AiMessagePromptFields
        from ravendb import AiMessagePromptTypes
        from ravendb import AiMessageRole
        from ravendb import AiOperations
        from ravendb import AiOutputOptions
        from ravendb import AiToolCallResult
        from ravendb import AiUsage
        from ravendb import AmazonSqsConnectionSettings
        from ravendb import AmazonSqsCredentials
        from ravendb import AuditLogsConfiguration
        from ravendb import AutoFieldIndexing
        from ravendb import AutoSpatialMethodType
        from ravendb import AzureQueueStorageConnectionSettings
        from ravendb import AzureServiceBusConnectionSettings
        from ravendb import AzureServiceBusEntraId
        from ravendb import AzureServiceBusPasswordless
        from ravendb import AzureServiceBusSinkSource
        from ravendb import BackupType
        from ravendb import BadResponseException
        from ravendb import BulkInsertOptions
        from ravendb import CdcColumnMapping
        from ravendb import CdcColumnType
        from ravendb import CdcSinkConfiguration
        from ravendb import CdcSinkEmbeddedTableConfig
        from ravendb import CdcSinkLinkedTableConfig
        from ravendb import CdcSinkOnDeleteConfig
        from ravendb import CdcSinkPostgresSettings
        from ravendb import CdcSinkProcessState
        from ravendb import CdcSinkRelationType
        from ravendb import CdcSinkSchemaRequest
        from ravendb import CdcSinkSourceColumn
        from ravendb import CdcSinkSourceForeignKey
        from ravendb import CdcSinkSourceSchema
        from ravendb import CdcSinkSourceTable
        from ravendb import CdcSinkTableConfig
        from ravendb import CdcSinkTableLoadState
        from ravendb import CdcSinkTaskState
        from ravendb import CertificateUsage
        from ravendb import ChunkingMethod
        from ravendb import ChunkingOptions
        from ravendb import ClientVersionMismatchException
        from ravendb import CommandType
        from ravendb import CompactSettings
        from ravendb import ConcurrencyException
        from ravendb import ConditionalGetResult
        from ravendb import ConfigureExpirationOperationResult
        from ravendb import ConfigureRemoteAttachmentsOperation
        from ravendb import ConfigureRemoteAttachmentsOperationResult
        from ravendb import ConfigureSchemaValidationOperation
        from ravendb import ConfigureSchemaValidationOperationResult
        from ravendb import ConflictException
        from ravendb import ConnectionStringUsage
        from ravendb import ConnectionStringUsageKind
        from ravendb import ContentPart
        from ravendb import ConversationResult
        from ravendb import CounterChangeTypes
        from ravendb import Counts
        from ravendb import CountsWithLastEtag
        from ravendb import CountsWithLastEtagAndAttachments
        from ravendb import CountsWithSkippedCountAndLastEtag
        from ravendb import CountsWithSkippedCountAndLastEtagAndAttachments
        from ravendb import DatabasePromotionStatus
        from ravendb import DatabasePutResult
        from ravendb import DatabaseRecordProgress
        from ravendb import DeleteAiAgentOperation
        from ravendb import DeleteDatabaseOperation
        from ravendb import DeleteDatabaseResult
        from ravendb import DocumentChangeType
        from ravendb import EmbeddingPathConfiguration
        from ravendb import EmbeddingsGenerationConfiguration
        from ravendb import EmbeddingsTransformation
        from ravendb import EncryptionMode
        from ravendb import EntraId
        from ravendb import ExportCompressionAlgorithm
        from ravendb import FieldIndexing
        from ravendb import FieldStorage
        from ravendb import FieldTermVector
        from ravendb import GenerateEntityIdOnTheClient
        from ravendb import GetAiAgentOperation
        from ravendb import GetAiAgentsResponse
        from ravendb import GetCdcSinkSchemaOperation
        from ravendb import GetConnectionStringsResult
        from ravendb import GetConversationMessagesOperation
        from ravendb import GetConversationMessagesOptions
        from ravendb import GetRemoteAttachmentsConfigurationOperation
        from ravendb import GetSchemaValidationConfiguration
        from ravendb import GetServerWideConnectionStringsOperation
        from ravendb import GetServerWideConnectionStringsResult
        from ravendb import GroupByArrayBehavior
        from ravendb import HiLoIdGenerator
        from ravendb import HiLoResult
        from ravendb import Highlightings
        from ravendb import IOperation
        from ravendb import IndexBatchOptions
        from ravendb import IndexChangeTypes
        from ravendb import IndexCompactionInProgressException
        from ravendb import IndexDefinitionCompareDifferences
        from ravendb import IndexErrors
        from ravendb import IndexLockMode
        from ravendb import IndexPriority
        from ravendb import IndexRunningStatus
        from ravendb import IndexState
        from ravendb import IndexStatus
        from ravendb import IndexType
        from ravendb import IndexingError
        from ravendb import InsufficientQuotaException
        from ravendb import JavaScriptMap
        from ravendb import JsonPatchCommandData
        from ravendb import JsonPatchDocument
        from ravendb import JsonPatchOperation
        from ravendb import JsonPatchResult
        from ravendb import KafkaConnectionSettings
        from ravendb import LazyOperation
        from ravendb import LicenseLimitException
        from ravendb import LimitType
        from ravendb import LogFilter
        from ravendb import LogFilterAction
        from ravendb import LogLevel
        from ravendb import LogsConfiguration
        from ravendb import MaintenanceOperation
        from ravendb import MicrosoftLogsConfiguration
        from ravendb import MissingAiAgentParameterException
        from ravendb import ModifyOngoingTaskResult
        from ravendb import MultiDatabaseHiLoGenerator
        from ravendb import MultiTypeHiLoGenerator
        from ravendb import NodeId
        from ravendb import NullsOrdering
        from ravendb import Observable
        from ravendb import OlapConnectionString
        from ravendb import OngoingTask
        from ravendb import OngoingTaskCdcSink
        from ravendb import OngoingTaskConnectionStatus
        from ravendb import OngoingTaskEmbeddingsGeneration
        from ravendb import OngoingTaskGenAi
        from ravendb import OngoingTaskQueueSink
        from ravendb import OngoingTaskState
        from ravendb import Operation
        from ravendb import OperationExceptionResult
        from ravendb import OperationIdResult
        from ravendb import OptimisticConcurrencyMode
        from ravendb import Passwordless
        from ravendb import PortInUseException
        from ravendb import PutConnectionStringResult
        from ravendb import PutResult
        from ravendb import PutServerWideConnectionStringOperation
        from ravendb import PutServerWideConnectionStringResult
        from ravendb import QueryToolFailedException
        from ravendb import QueueBrokerType
        from ravendb import QueueConnectionString
        from ravendb import QueueSinkConfiguration
        from ravendb import QueueSinkProcessState
        from ravendb import QueueSinkScript
        from ravendb import RabbitMqConnectionSettings
        from ravendb import RateLimitException
        from ravendb import RavenConnectionString
        from ravendb import RavenDocumentQuery
        from ravendb import RavenException
        from ravendb import RefusedToAnswerException
        from ravendb import RemoteAttachmentsAzureSettings
        from ravendb import RemoteAttachmentsConfiguration
        from ravendb import RemoteAttachmentsDestinationConfiguration
        from ravendb import RemoteAttachmentsS3Settings
        from ravendb import RemoveConnectionStringResult
        from ravendb import RemoveServerWideConnectionStringOperation
        from ravendb import RemoveServerWideConnectionStringResult
        from ravendb import ReplicationBatchOptions
        from ravendb import ReplicationHubNotFoundException
        from ravendb import ReplicationType
        from ravendb import ResponseDisposeHandling
        from ravendb import RevisionIncludeResult
        from ravendb import RunConversationOperation
        from ravendb import S3StorageClass
        from ravendb import SchemaDefinition
        from ravendb import SchemaValidationConfiguration
        from ravendb import SchemaValidationException
        from ravendb import SearchEngineType
        from ravendb import ServerOperation
        from ravendb import ServerWideConnectionString
        from ravendb import ServerWideConnectionStringUsage
        from ravendb import ServerWideOperation
        from ravendb import SessionPatchBehavior
        from ravendb import SmugglerOperation
        from ravendb import SmugglerProgressBase
        from ravendb import SmugglerResult
        from ravendb import SnapshotSettings
        from ravendb import SortOptions
        from ravendb import SpatialFieldType
        from ravendb import SpatialOptions
        from ravendb import SpatialOptionsFactory
        from ravendb import SpatialRelation
        from ravendb import SpatialSearchStrategy
        from ravendb import SpatialUnits
        from ravendb import SqlConnectionString
        from ravendb import SsoIdentifier
        from ravendb import SsoProvider
        from ravendb import StartSchemaValidationOperation
        from ravendb import TestCdcSinkMappingOperation
        from ravendb import TestCdcSinkMappingRequest
        from ravendb import TestCdcSinkMappingResult
        from ravendb import TestCdcSinkOperation
        from ravendb import TestCdcSinkRowResult
        from ravendb import TestCdcSinkRowSelector
        from ravendb import TextPart
        from ravendb import TimeSeriesChangeTypes
        from ravendb import TooManyRequestsException
        from ravendb import TooManyTokensException
        from ravendb import TopologyChange
        from ravendb import UnsuccessfulAiRequestException
        from ravendb import UpdateCdcSinkOperation
        from ravendb import UpdateCdcSinkOperationResult
        from ravendb import UpdateEmbeddingsGenerationOperation
        from ravendb import UpdateEtlOperationResult
        from ravendb import UpdateGenAiOperation
        from ravendb import UpdateQueueSinkOperation
        from ravendb import UpdateQueueSinkOperationResult
        from ravendb import ValidateSchemaProgress
        from ravendb import ValidateSchemaResult
        from ravendb import VoidMaintenanceOperation
        from ravendb import VoidOperation
        from ravendb import VoidServerOperation
