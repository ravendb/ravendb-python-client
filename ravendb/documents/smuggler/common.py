import enum


class DatabaseItemType(enum.Enum):
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
    COUNTERS = "Counters"
    ATTACHMENTS = "Attachments"
    COUNTER_GROUPS = "CounterGroups"
    SUBSCRIPTIONS = "Subscriptions"
    COMPARE_EXCHANGE_TOMBSTONES = "CompareExchangeTombstones"
    TIME_SERIES = "TimeSeries"
    REPLICATION_HUB_CERTIFICATES = "ReplicationHubCertificates"
