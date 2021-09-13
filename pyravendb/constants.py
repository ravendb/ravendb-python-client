class Documents:
    class Metadata:
        COLLECTION = "@collection"
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
        ALL_DOCUMENTS_COLLECTION = "@all-docs"
        EMPTY_COLLECTION = "@empty"
        NESTED_OBJECT_TYPES = "@nested-object-types"
        COUNTERS = "@counters"
        TIME_SERIES = "@timeseries"
        REVISION_COUNTERS = "@counters-snapshot"
        REVISION_TIME_SERIES = "@timeseries-snapshot"
        RAVEN_PYTHON_TYPE = "Raven-Python-Type"

    class Indexing:
        SIDE_BY_SIDE_INDEX_NAME_PREFIX = "ReplacementOf/"
