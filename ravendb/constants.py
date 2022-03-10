int_max = 0x7FFFFFF
json_serialize_method_name = "to_json"


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
