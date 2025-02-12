import sys

from ravendb.documents.indexes.vector.embedding import VectorEmbeddingType

int_max = 0x7FFFFFFF
int_min = -int_max - 1
min_normal = sys.float_info.min
json_serialize_method_name = "to_json"
nan_value = float("nan")


class Documents:
    class Metadata:
        COLLECTION = "@collection"
        CONFLICT = "@conflict"
        PROJECTION = "@projection"
        METADATA = "@metadata"
        KEY = "@metadata"
        ID = "@id"
        FLAGS = "@flags"
        ATTACHMENTS = "@attachments"
        INDEX_SCORE = "@index-score"
        LAST_MODIFIED = "@last-modified"
        CHANGE_VECTOR = "@change-vector"
        EXPIRES = "@expires"
        REFRESH = "@refresh"
        ALL_DOCUMENTS_COLLECTION = "@all_docs"
        EMPTY_COLLECTION = "@empty"
        NESTED_OBJECT_TYPES = "@nested-object-types"
        COUNTERS = "@counters"
        TIME_SERIES = "@timeseries"
        REVISION_COUNTERS = "@counters-snapshot"
        REVISION_TIME_SERIES = "@timeseries-snapshot"
        RAVEN_PYTHON_TYPE = "Raven-Python-Type"

    class Indexing:
        SIDE_BY_SIDE_INDEX_NAME_PREFIX = "ReplacementOf/"

        class Fields:
            DOCUMENT_ID_FIELD_NAME = "id()"
            SOURCE_DOCUMENT_ID_FIELD_NAME = "sourceDocId()"
            REDUCE_KEY_HASH_FIELD_NAME = "hash(key())"
            REDUCE_KEY_KEY_VALUE_FIELD_NAME = "key()"
            VALUE_FIELD_NAME = "value()"
            ALL_FIELDS = "__all_fields"
            SPATIAL_SHAPE_FIELD_NAME = "spatial(shape)"

            class JavaScript:
                VECTOR_PROPERTY_NAME = "$vector"

        class Spatial:
            DEFAULT_DISTANCE_ERROR_PCT = 0.025


class CompareExchange:
    OBJECT_FIELD_NAME = "Object"


class Counters:
    ALL = "@all_counters"


class Headers:
    REQUEST_TIME = "Raven-Request-Time"
    REFRESH_TOPOLOGY = "Refresh-Topology"
    TOPOLOGY_ETAG = "Topology-Etag"
    LAST_KNOWN_CLUSTER_TRANSACTION_INDEX = "Known-Raft-Index"
    CLIENT_CONFIGURATION_ETAG = "Client-Configuration-Etag"
    REFRESH_CLIENT_CONFIGURATION = "Refresh-Client-Configuration"
    CLIENT_VERSION = "Raven-Client-Version"
    SERVER_VERSION = "Raven-Server-Version"
    ETAG = "ETag"
    IF_NONE_MATCH = "If-None-Match"
    TRANSFER_ENCODING = "Transfer-Encoding"
    CONTENT_ENCODING = "Content-Encoding"
    CONTENT_LENGTH = "Content-Length"


class TimeSeries:
    SELECT_FIELD_NAME = "timeseries"
    QUERY_FUNCTION = "__timeSeriesQueryFunction"

    ALL = "@all_timeseries"


class VectorSearch:
    EMBEDDING_PREFIX = "embedding."
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
