import enum
import sys

from ravendb.documents.indexes.vector.embedding import VectorEmbeddingType

int_max = 0x7FFFFFFF
int_min = -int_max - 1
min_normal = sys.float_info.min
json_serialize_method_name = "to_json"
nan_value = float("nan")


class _CompanyInformation:
    COMPANY_OID = "1.3.6.1.4.1.45751"


class Json:
    class Fields:
        TYPE = "$type"
        VALUES = "$values"


class QueryString:
    NODE_TAG = "nodeTag"
    SHARD_NUMBER = "shardNumber"


class Headers:
    REQUEST_TIME = "Request-Time"
    SERVER_STARTUP_TIME = "Server-Startup-Time"
    REFRESH_TOPOLOGY = "Refresh-Topology"
    TOPOLOGY_ETAG = "Topology-Etag"
    CLUSTER_TOPOLOGY_ETAG = "Cluster-Topology-Etag"
    CLIENT_CONFIGURATION_ETAG = "Client-Configuration-Etag"
    LAST_KNOWN_CLUSTER_TRANSACTION_INDEX = "Known-Raft-Index"
    DATABASE_CLUSTER_TRANSACTION_ID = "Database-Cluster-Tx-Id"
    REFRESH_CLIENT_CONFIGURATION = "Refresh-Client-Configuration"
    ETAG = "ETag"
    CLIENT_VERSION = "Raven-Client-Version"
    SERVER_VERSION = "Raven-Server-Version"
    STUDIO_VERSION = "Raven-Studio-Version"
    IF_MATCH = "If-Match"
    IF_NONE_MATCH = "If-None-Match"
    TRANSFER_ENCODING = "Transfer-Encoding"
    CONTENT_ENCODING = "Content-Encoding"
    ACCEPT_ENCODING = "Accept-Encoding"
    CONTENT_DISPOSITION = "Content-Disposition"
    CONTENT_TYPE = "Content-Type"
    CONTENT_LENGTH = "Content-Length"
    ORIGIN = "Origin"
    INCREMENTAL_TIME_SERIES_PREFIX = "INC:"
    SHARDED = "Sharded"
    ATTACHMENT_HASH = "Attachment-Hash"
    DATABASE_MISSING = "Database-Missing"

    class Encodings:
        GZIP = "gzip"
        BROTLI = "br"
        DEFLATE = "deflate"
        ZSTD = "zstd"


class Platform:
    class Windows:
        MAX_PATH = 0x7FFF
        RESERVED_FILE_NAMES = [
            "con",
            "prn",
            "aux",
            "nul",
            "com1",
            "com2",
            "com3",
            "com4",
            "com5",
            "com6",
            "com7",
            "com8",
            "com9",
            "lpt1",
            "lpt2",
            "lpt3",
            "lpt4",
            "lpt5",
            "lpt6",
            "lpt7",
            "lpt8",
            "lpt9",
            "clock$",
        ]

    class Linux:
        MAX_PATH = 4096
        MAX_FILE_NAME_LENGTH = 230


class Certificates:
    PREFIX = "certificates/"
    MAX_NUMBER_OF_CERTS_WITH_SAME_HASH = 5
    SERVER_AUTHENTICATION_OID = "1.3.6.1.5.5.7.3.1"
    CLIENT_AUTHENTICATION_OID = "1.3.6.1.5.5.7.3.2"
    SERVER_CERT_EXTENSION_OID = _CompanyInformation.COMPANY_OID + ".2.1"


class Network:
    ANY_IP = "0.0.0.0"
    ZERO_VALUE = 0
    DEFAULT_SECURED_RAVEN_DB_HTTP_PORT = 443
    DEFAULT_SECURED_RAVEN_DB_TCP_PORT = 38888


class DatabaseSettings:
    STUDIO_ID = "DatabasesSettings/Studio"


class Configuration:
    class Indexes:
        INDEXING_STATIC_SEARCH_ENGINE_TYPE = "Indexing.Static.SearchEngineType"

    CLIENT_ID = "Configuration/Client"
    STUDIO_ID = "Configuration/Studio"


class Counters:
    ALL = "@all_counters"


class TimeSeries:
    SELECT_FIELD_NAME = "timeseries"
    QUERY_FUNCTION = "__timeSeriesQueryFunction"

    ALL = "@all_timeseries"


class Documents:
    PREFIX = "db/"
    MAX_DATABASE_NAME_LENGTH = 128

    class SubscriptionChangeVectorSpecialStates(enum.Enum):
        DO_NOT_CHANGE = "DoNotChange"
        LAST_DOCUMENT = "LastDocument"
        BEGINNING_OF_TIME = "BeginningOfTime"

    class Metadata:
        EDGES = "@edges"
        COLLECTION = "@collection"
        PROJECTION = "@projection"
        KEY = "@metadata"
        ID = "@id"
        CONFLICT = "@conflict"
        ID_PROPERTY = "Id"
        FLAGS = "@flags"
        ATTACHMENTS = "@attachments"
        COUNTERS = "@counters"
        TIME_SERIES = "@timeseries"
        TIME_SERIES_NAMED_VALUES = "@timeseries-named-values"
        REVISION_COUNTERS = "@counters-snapshot"
        REVISION_TIME_SERIES = "@timeseries-snapshot"
        LEGACY_ATTACHMENT_METADATA = "@legacy-attachment-metadata"
        INDEX_SCORE = "@index-score"
        SPATIAL_RESULT = "@spatial"
        LAST_MODIFIED = "@last-modified"
        RAVEN_PYTHON_TYPE = "Raven-Python-Type"
        CHANGE_VECTOR = "@change-vector"
        EXPIRES = "@expires"
        REFRESH = "@refresh"
        ARCHIVE_AT = "@archive-at"
        ARCHIVED = "@archived"
        HAS_VALUE = "HasValue"
        ETAG = "@etag"
        QUANTIZATION = "@quantization"
        GEN_AI_HASHES = "@gen-ai-hashes"

        class Sharding:
            SHARD_NUMBER = "@shard-number"

            class Querying:
                ORDER_BY_FIELDS = "@order-by-fields"
                SUGGESTIONS_POPULARITY_FIELDS = "@suggestions-popularity"
                RESULT_DATA_HASH = "@data-hash"

            class Subscription:
                NON_PERSISTENT_FLAGS = "@non-persistent-flags"

    class Collections:
        ALL_DOCUMENTS_COLLECTION = "@all_docs"
        EMPTY_COLLECTION = "@empty"
        EMBEDDINGS_CACHE_COLLECTION = "@embeddings-cache"
        AI_AGENT_CONVERSATIONS_COLLECTION = "@conversations"
        AI_AGGENT_CONVERSATION_HISTORY_COLLECTION = "@conversations-history"
        NESTED_OBJECT_TYPES = "@nested-object-types"

    class Ai:
        AI_AGENT_ID_PREFIX = "Conversations"

    class Indexing:
        SIDE_BY_SIDE_INDEX_NAME_PREFIX = "ReplacementOf/"

        class Fields:
            COUNT_FIELD_NAME = "Count"
            DOCUMENT_ID_FIELD_NAME = "id()"
            DOCUMENT_ID_METHOD_NAME = "id"
            SOURCE_DOCUMENT_ID_FIELD_NAME = "sourceDocId()"
            REDUCE_KEY_HASH_FIELD_NAME = "hash(key())"
            REDUCE_KEY_VALUE_FIELD_NAME = "key()"
            VALUE_FIELD_NAME = "value()"
            ALL_FIELDS = "__all_fields"
            ALL_STORED_FIELDS = "__all_stored_fields"
            SPATIAL_SHAPE_FIELD_NAME = "spatial(shape)"
            RANGE_FIELD_SUFFIX = "_Range"
            RANGE_FIELD_SUFFIX_LONG = "_L" + RANGE_FIELD_SUFFIX
            RANGE_FIELD_SUFFIX_DOUBLE = "_D" + RANGE_FIELD_SUFFIX
            TIME_FIELD_SUFFIX = "_Time"
            NULL_VALUE = "NULL_VALUE"
            EMPTY_STRING = "EMPTY_STRING"

            class JavaScript:
                VALUE_PROPERTY_NAME = "$value"
                OPTIONS_PROPERTY_NAME = "$options"
                NAME_PROPERTY_NAME = "$name"
                SPATIAL_PROPERTY_NAME = "$spatial"
                BOOST_PROPERTY_NAME = "$boost"
                VECTOR_PROPERTY_NAME = "$vector"
                LOAD_VECTOR_PROPERTY_NAME = "$loadvector"

        class Spatial:
            DEFAULT_DISTANCE_ERROR_PCT = 0.025
            EARTH_MEAN_RADIUS_KM = 6371.0087714
            MILES_TO_KM = 1.60934

        class Analyzers:
            DEFAULT = "LowerCaseKeywordAnalyzer"
            DEFAULT_EXACT = "KeywordAnalyzer"
            DEFAULT_SEARCH = "RavenStandardAnalyzer"

    class Querying:
        class Facet:
            ALL_RESULTS = "@AllResults"

        class Fields:
            POWER_BI_JSON_FIELD_NAME = "json()"

        class Sharding:
            SHARD_CONTEXT_PARAMETER_NAME = "__shardContext"
            SHARD_CONTEXT_DOCUMENT_IDS = "DocumentIds"
            SHARD_CONTEXT_PREFIXES = "Prefixes"

        class Terms:
            LEFT_NULL_VALUE_OF_BETWEEN_QUERY = "*"
            RIGHT_NULL_VALUE_OF_BETWEEN_QUERY = "NULL"

    class PeriodicBackup:
        FULL_BACKUP_EXTENSTION = ".ravendb-full-backup"
        SNAPSHOT_EXTENSTION = ".ravendb-snapshot"
        ENCRYPTED_FULL_BACKUP_EXTENSTION = ".ravendb-encrypted-full-backup"
        ENCRYPTED_SNAPSHOT_EXTENSTION = ".ravendb-encrypted-snapshot"
        INCREMENTAL_BACKUP_EXTENSTION = ".ravendb-incremental-backup"
        ENCRYPTED_INCREMENTAL_BACKUP_EXTENSTION = ".ravendb-encrypted-incremental-backup"

        class Folders:
            INDEXES = "Indexes"
            DOCUMENTS = "Documents"
            CONFIGURATION = "Configuration"

    class Blob:
        DOCUMENT = "@raven-data"
        SIZE = "@raven-blob-size"


class Identities:
    DEFAULT_SEPARATOR = "/"


class Smuggler:
    IMPORT_OPTIONS = "importOptions"
    CSV_IMPORT_OPTIONS = "csvImportOptions"


class Operations:
    INVALID_OPERATION_ID = -1


class CompareExchange:
    RVN_ATOMIC_PREFIX = "rvn-atomic/"
    OBJECT_FIELD_NAME = "Object"


class Monitoring:
    class Snmp:
        DATABASES_MAPPING_KEY = "monitoring/snmp/databases/mapping"
        SNMP_ROOT_ID = _CompanyInformation.COMPANY_OID + ".1.1"


class Fields:
    DOCUMENT_CHANGE_VECTOR = None
    DESTINATION_DOCUMENT_CHANGE_VECTOR = None


class Obsolete:
    pass


class DatabaseRecord:
    class SupportedFeatures:
        THROW_REVISION_KEY_TOO_BIG_FIX = "ThrowRevisionKeyTooBigFix"


class VectorSearch:
    AI_TASK_METHOD_NAME = "ai.task"
    EMBEDDING_PREFIX = "embedding."

    EMBEDDING_FOR_DOCUMENT = EMBEDDING_PREFIX + "forDoc"
    EMBEDDING_FOR_RAW = EMBEDDING_PREFIX + "Raw"
    EMBEDDING_TEXT = EMBEDDING_PREFIX + "text"
    EMBEDDING_TEXT_INT_8 = EMBEDDING_PREFIX + "text_i8"
    EMBEDDING_TEXT_INT_1 = EMBEDDING_PREFIX + "text_i1"
    EMBEDDING_SINGLE = EMBEDDING_PREFIX + "f32"
    EMBEDDING_SINGLE_INT8 = EMBEDDING_PREFIX + "f32_i8"
    EMBEDDING_SINGLE_INT1 = EMBEDDING_PREFIX + "f32_i1"
    EMBEDDING_INT8 = EMBEDDING_PREFIX + "i8"
    EMBEDDING_INT1 = EMBEDDING_PREFIX + "i1"

    @staticmethod
    def configuration_to_method_name(source: VectorEmbeddingType, dest: VectorEmbeddingType):
        mapping = {
            (VectorEmbeddingType.SINGLE, VectorEmbeddingType.SINGLE): "",
            (VectorEmbeddingType.SINGLE, VectorEmbeddingType.INT8): VectorSearch.EMBEDDING_SINGLE_INT8,
            (VectorEmbeddingType.SINGLE, VectorEmbeddingType.BINARY): VectorSearch.EMBEDDING_SINGLE_INT1,
            (VectorEmbeddingType.TEXT, VectorEmbeddingType.SINGLE): VectorSearch.EMBEDDING_TEXT,
            (VectorEmbeddingType.TEXT, VectorEmbeddingType.INT8): VectorSearch.EMBEDDING_TEXT_INT_8,
            (VectorEmbeddingType.TEXT, VectorEmbeddingType.BINARY): VectorSearch.EMBEDDING_TEXT_INT_1,
            (VectorEmbeddingType.INT8, VectorEmbeddingType.INT8): VectorSearch.EMBEDDING_INT8,
            (VectorEmbeddingType.BINARY, VectorEmbeddingType.BINARY): VectorSearch.EMBEDDING_INT1,
        }
        if (source, dest) not in mapping:
            raise ValueError(
                f"Invalid embedding configuration. SourceEmbedding: {source.value}, DestinationEmbedding: {dest.value}"
            )
        return mapping[(source, dest)]

    DEFAULT_EMBEDDING_TYPE = VectorEmbeddingType.SINGLE
    DEFAULT_IS_EXACT = False
