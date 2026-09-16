from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from ravendb.tools.utils import Utils


class Counts:
    """How much of one item type an export or import got through."""

    def __init__(
        self,
        start_time: Optional[datetime] = None,
        processed: bool = False,
        read_count: int = 0,
        skipped: bool = False,
        errored_count: int = 0,
        size_in_bytes: int = 0,
    ):
        self.start_time = start_time
        self.processed = processed
        self.read_count = read_count
        self.skipped = skipped
        self.errored_count = errored_count
        self.size_in_bytes = size_in_bytes

    def to_json(self) -> Dict[str, Any]:
        return {
            "StartTime": Utils.datetime_to_string(self.start_time) if self.start_time else None,
            "Processed": self.processed,
            "ReadCount": self.read_count,
            "Skipped": self.skipped,
            "ErroredCount": self.errored_count,
            "SizeInBytes": self.size_in_bytes,
        }

    def _fill_from_json(self, json_dict: Dict[str, Any]) -> None:
        start_time = json_dict.get("StartTime")
        self.start_time = Utils.string_to_datetime(start_time) if start_time else None
        self.processed = json_dict.get("Processed", False)
        self.read_count = json_dict.get("ReadCount", 0)
        self.skipped = json_dict.get("Skipped", False)
        self.errored_count = json_dict.get("ErroredCount", 0)
        self.size_in_bytes = json_dict.get("SizeInBytes", 0)

    @classmethod
    def from_json(cls, json_dict: Optional[Dict[str, Any]]):
        counts = cls()
        if json_dict:
            counts._fill_from_json(json_dict)
        return counts

    def __repr__(self) -> str:
        text = f"Read: {self.read_count:,}."
        if self.errored_count:
            text += f" Errored: {self.errored_count:,}."
        if self.size_in_bytes:
            text += f" Size: {self.size_in_bytes:,} bytes."
        return text


class CountsWithLastEtag(Counts):
    def __init__(self, last_etag: int = 0, **kwargs):
        super().__init__(**kwargs)
        self.last_etag = last_etag

    def to_json(self) -> Dict[str, Any]:
        json_dict = super().to_json()
        json_dict["LastEtag"] = self.last_etag
        return json_dict

    def _fill_from_json(self, json_dict: Dict[str, Any]) -> None:
        super()._fill_from_json(json_dict)
        self.last_etag = json_dict.get("LastEtag", 0)


class CountsWithLastEtagAndAttachments(CountsWithLastEtag):
    def __init__(self, attachments: Optional[Counts] = None, **kwargs):
        super().__init__(**kwargs)
        self.attachments = attachments if attachments is not None else Counts()

    def to_json(self) -> Dict[str, Any]:
        json_dict = super().to_json()
        json_dict["Attachments"] = self.attachments.to_json()
        return json_dict

    def _fill_from_json(self, json_dict: Dict[str, Any]) -> None:
        super()._fill_from_json(json_dict)
        self.attachments = Counts.from_json(json_dict.get("Attachments"))

    def __repr__(self) -> str:
        if self.attachments and self.attachments.read_count:
            return f"{super().__repr__()} Attachments: {self.attachments!r}"
        return super().__repr__()


class CountsWithSkippedCountAndLastEtag(CountsWithLastEtag):
    def __init__(self, skipped_count: int = 0, **kwargs):
        super().__init__(**kwargs)
        self.skipped_count = skipped_count

    def to_json(self) -> Dict[str, Any]:
        json_dict = super().to_json()
        json_dict["SkippedCount"] = self.skipped_count
        return json_dict

    def _fill_from_json(self, json_dict: Dict[str, Any]) -> None:
        super()._fill_from_json(json_dict)
        self.skipped_count = json_dict.get("SkippedCount", 0)

    def __repr__(self) -> str:
        return f"Skipped: {self.skipped_count:,}. {super().__repr__()}"


class CountsWithSkippedCountAndLastEtagAndAttachments(CountsWithLastEtagAndAttachments):
    def __init__(self, skipped_count: int = 0, **kwargs):
        super().__init__(**kwargs)
        self.skipped_count = skipped_count

    def to_json(self) -> Dict[str, Any]:
        json_dict = super().to_json()
        json_dict["SkippedCount"] = self.skipped_count
        return json_dict

    def _fill_from_json(self, json_dict: Dict[str, Any]) -> None:
        super()._fill_from_json(json_dict)
        self.skipped_count = json_dict.get("SkippedCount", 0)

    def __repr__(self) -> str:
        if self.skipped_count:
            return f"Skipped: {self.skipped_count:,}. {super().__repr__()}"
        return super().__repr__()


class DatabaseRecordProgress(Counts):
    """
    Which parts of the database record an import wrote. The server sends only the flags it
    actually set, so anything it leaves out reads as False.
    """

    def __init__(
        self,
        sorters_updated: bool = False,
        analyzers_updated: bool = False,
        sink_pull_replications_updated: bool = False,
        hub_pull_replications_updated: bool = False,
        raven_etls_updated: bool = False,
        sql_etls_updated: bool = False,
        snowflake_etls_updated: bool = False,
        embeddings_generations_updated: bool = False,
        gen_ai_tasks_updated: bool = False,
        external_replications_updated: bool = False,
        periodic_backups_updated: bool = False,
        conflict_solver_config_updated: bool = False,
        schema_validation_config_updated: bool = False,
        time_series_configuration_updated: bool = False,
        documents_compression_configuration_updated: bool = False,
        revisions_configuration_updated: bool = False,
        expiration_configuration_updated: bool = False,
        refresh_configuration_updated: bool = False,
        data_archival_configuration_updated: bool = False,
        remote_attachments_configuration_updated: bool = False,
        raven_connection_strings_updated: bool = False,
        sql_connection_strings_updated: bool = False,
        snowflake_connection_strings_updated: bool = False,
        ai_connection_strings_updated: bool = False,
        ai_agents_updated: bool = False,
        client_configuration_updated: bool = False,
        unused_database_ids_updated: bool = False,
        lock_mode_updated: bool = False,
        olap_etls_updated: bool = False,
        olap_connection_strings_updated: bool = False,
        elastic_search_etls_updated: bool = False,
        elastic_search_connection_strings_updated: bool = False,
        postre_s_q_l_configuration_updated: bool = False,
        queue_etls_updated: bool = False,
        queue_connection_strings_updated: bool = False,
        queue_sinks_updated: bool = False,
        cdc_sinks_updated: bool = False,
        indexes_history_updated: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sorters_updated = sorters_updated
        self.analyzers_updated = analyzers_updated
        self.sink_pull_replications_updated = sink_pull_replications_updated
        self.hub_pull_replications_updated = hub_pull_replications_updated
        self.raven_etls_updated = raven_etls_updated
        self.sql_etls_updated = sql_etls_updated
        self.snowflake_etls_updated = snowflake_etls_updated
        self.embeddings_generations_updated = embeddings_generations_updated
        self.gen_ai_tasks_updated = gen_ai_tasks_updated
        self.external_replications_updated = external_replications_updated
        self.periodic_backups_updated = periodic_backups_updated
        self.conflict_solver_config_updated = conflict_solver_config_updated
        self.schema_validation_config_updated = schema_validation_config_updated
        self.time_series_configuration_updated = time_series_configuration_updated
        self.documents_compression_configuration_updated = documents_compression_configuration_updated
        self.revisions_configuration_updated = revisions_configuration_updated
        self.expiration_configuration_updated = expiration_configuration_updated
        self.refresh_configuration_updated = refresh_configuration_updated
        self.data_archival_configuration_updated = data_archival_configuration_updated
        self.remote_attachments_configuration_updated = remote_attachments_configuration_updated
        self.raven_connection_strings_updated = raven_connection_strings_updated
        self.sql_connection_strings_updated = sql_connection_strings_updated
        self.snowflake_connection_strings_updated = snowflake_connection_strings_updated
        self.ai_connection_strings_updated = ai_connection_strings_updated
        self.ai_agents_updated = ai_agents_updated
        self.client_configuration_updated = client_configuration_updated
        self.unused_database_ids_updated = unused_database_ids_updated
        self.lock_mode_updated = lock_mode_updated
        self.olap_etls_updated = olap_etls_updated
        self.olap_connection_strings_updated = olap_connection_strings_updated
        self.elastic_search_etls_updated = elastic_search_etls_updated
        self.elastic_search_connection_strings_updated = elastic_search_connection_strings_updated
        self.postre_s_q_l_configuration_updated = postre_s_q_l_configuration_updated
        self.queue_etls_updated = queue_etls_updated
        self.queue_connection_strings_updated = queue_connection_strings_updated
        self.queue_sinks_updated = queue_sinks_updated
        self.cdc_sinks_updated = cdc_sinks_updated
        self.indexes_history_updated = indexes_history_updated

    def to_json(self) -> Dict[str, Any]:
        json_dict = super().to_json()
        if self.sorters_updated:
            json_dict["SortersUpdated"] = True
        if self.analyzers_updated:
            json_dict["AnalyzersUpdated"] = True
        if self.sink_pull_replications_updated:
            json_dict["SinkPullReplicationsUpdated"] = True
        if self.hub_pull_replications_updated:
            json_dict["HubPullReplicationsUpdated"] = True
        if self.raven_etls_updated:
            json_dict["RavenEtlsUpdated"] = True
        if self.sql_etls_updated:
            json_dict["SqlEtlsUpdated"] = True
        if self.snowflake_etls_updated:
            json_dict["SnowflakeEtlsUpdated"] = True
        if self.embeddings_generations_updated:
            json_dict["EmbeddingsGenerationsUpdated"] = True
        if self.gen_ai_tasks_updated:
            json_dict["GenAiTasksUpdated"] = True
        if self.external_replications_updated:
            json_dict["ExternalReplicationsUpdated"] = True
        if self.periodic_backups_updated:
            json_dict["PeriodicBackupsUpdated"] = True
        if self.conflict_solver_config_updated:
            json_dict["ConflictSolverConfigUpdated"] = True
        if self.schema_validation_config_updated:
            json_dict["SchemaValidationConfigUpdated"] = True
        if self.time_series_configuration_updated:
            json_dict["TimeSeriesConfigurationUpdated"] = True
        if self.documents_compression_configuration_updated:
            json_dict["DocumentsCompressionConfigurationUpdated"] = True
        if self.revisions_configuration_updated:
            json_dict["RevisionsConfigurationUpdated"] = True
        if self.expiration_configuration_updated:
            json_dict["ExpirationConfigurationUpdated"] = True
        if self.refresh_configuration_updated:
            json_dict["RefreshConfigurationUpdated"] = True
        if self.data_archival_configuration_updated:
            json_dict["DataArchivalConfigurationUpdated"] = True
        if self.remote_attachments_configuration_updated:
            json_dict["RemoteAttachmentsConfigurationUpdated"] = True
        if self.raven_connection_strings_updated:
            json_dict["RavenConnectionStringsUpdated"] = True
        if self.sql_connection_strings_updated:
            json_dict["SqlConnectionStringsUpdated"] = True
        if self.snowflake_connection_strings_updated:
            json_dict["SnowflakeConnectionStringsUpdated"] = True
        if self.ai_connection_strings_updated:
            json_dict["AiConnectionStringsUpdated"] = True
        if self.ai_agents_updated:
            json_dict["AiAgentsUpdated"] = True
        if self.client_configuration_updated:
            json_dict["ClientConfigurationUpdated"] = True
        if self.unused_database_ids_updated:
            json_dict["UnusedDatabaseIdsUpdated"] = True
        if self.lock_mode_updated:
            json_dict["LockModeUpdated"] = True
        if self.olap_etls_updated:
            json_dict["OlapEtlsUpdated"] = True
        if self.olap_connection_strings_updated:
            json_dict["OlapConnectionStringsUpdated"] = True
        if self.elastic_search_etls_updated:
            json_dict["ElasticSearchEtlsUpdated"] = True
        if self.elastic_search_connection_strings_updated:
            json_dict["ElasticSearchConnectionStringsUpdated"] = True
        if self.postre_s_q_l_configuration_updated:
            json_dict["PostreSQLConfigurationUpdated"] = True
        if self.queue_etls_updated:
            json_dict["QueueEtlsUpdated"] = True
        if self.queue_connection_strings_updated:
            json_dict["QueueConnectionStringsUpdated"] = True
        if self.queue_sinks_updated:
            json_dict["QueueSinksUpdated"] = True
        if self.cdc_sinks_updated:
            json_dict["CdcSinksUpdated"] = True
        if self.indexes_history_updated:
            json_dict["IndexesHistoryUpdated"] = True
        return json_dict

    def _fill_from_json(self, json_dict: Dict[str, Any]) -> None:
        super()._fill_from_json(json_dict)
        self.sorters_updated = json_dict.get("SortersUpdated", False)
        self.analyzers_updated = json_dict.get("AnalyzersUpdated", False)
        self.sink_pull_replications_updated = json_dict.get("SinkPullReplicationsUpdated", False)
        self.hub_pull_replications_updated = json_dict.get("HubPullReplicationsUpdated", False)
        self.raven_etls_updated = json_dict.get("RavenEtlsUpdated", False)
        self.sql_etls_updated = json_dict.get("SqlEtlsUpdated", False)
        self.snowflake_etls_updated = json_dict.get("SnowflakeEtlsUpdated", False)
        self.embeddings_generations_updated = json_dict.get("EmbeddingsGenerationsUpdated", False)
        self.gen_ai_tasks_updated = json_dict.get("GenAiTasksUpdated", False)
        self.external_replications_updated = json_dict.get("ExternalReplicationsUpdated", False)
        self.periodic_backups_updated = json_dict.get("PeriodicBackupsUpdated", False)
        self.conflict_solver_config_updated = json_dict.get("ConflictSolverConfigUpdated", False)
        self.schema_validation_config_updated = json_dict.get("SchemaValidationConfigUpdated", False)
        self.time_series_configuration_updated = json_dict.get("TimeSeriesConfigurationUpdated", False)
        self.documents_compression_configuration_updated = json_dict.get(
            "DocumentsCompressionConfigurationUpdated", False
        )
        self.revisions_configuration_updated = json_dict.get("RevisionsConfigurationUpdated", False)
        self.expiration_configuration_updated = json_dict.get("ExpirationConfigurationUpdated", False)
        self.refresh_configuration_updated = json_dict.get("RefreshConfigurationUpdated", False)
        self.data_archival_configuration_updated = json_dict.get("DataArchivalConfigurationUpdated", False)
        self.remote_attachments_configuration_updated = json_dict.get("RemoteAttachmentsConfigurationUpdated", False)
        self.raven_connection_strings_updated = json_dict.get("RavenConnectionStringsUpdated", False)
        self.sql_connection_strings_updated = json_dict.get("SqlConnectionStringsUpdated", False)
        self.snowflake_connection_strings_updated = json_dict.get("SnowflakeConnectionStringsUpdated", False)
        self.ai_connection_strings_updated = json_dict.get("AiConnectionStringsUpdated", False)
        self.ai_agents_updated = json_dict.get("AiAgentsUpdated", False)
        self.client_configuration_updated = json_dict.get("ClientConfigurationUpdated", False)
        self.unused_database_ids_updated = json_dict.get("UnusedDatabaseIdsUpdated", False)
        self.lock_mode_updated = json_dict.get("LockModeUpdated", False)
        self.olap_etls_updated = json_dict.get("OlapEtlsUpdated", False)
        self.olap_connection_strings_updated = json_dict.get("OlapConnectionStringsUpdated", False)
        self.elastic_search_etls_updated = json_dict.get("ElasticSearchEtlsUpdated", False)
        self.elastic_search_connection_strings_updated = json_dict.get("ElasticSearchConnectionStringsUpdated", False)
        self.postre_s_q_l_configuration_updated = json_dict.get("PostreSQLConfigurationUpdated", False)
        self.queue_etls_updated = json_dict.get("QueueEtlsUpdated", False)
        self.queue_connection_strings_updated = json_dict.get("QueueConnectionStringsUpdated", False)
        self.queue_sinks_updated = json_dict.get("QueueSinksUpdated", False)
        self.cdc_sinks_updated = json_dict.get("CdcSinksUpdated", False)
        self.indexes_history_updated = json_dict.get("IndexesHistoryUpdated", False)

    @property
    def updated(self) -> List[str]:
        """The parts that were written, for when you just want to see the list."""
        return sorted(name for name, value in vars(self).items() if name.endswith("_updated") and value is True)


class SmugglerProgressBase:
    """Per-item-type counts, shared by a smuggler operation's progress and its result."""

    _SECTIONS = (
        ("database_record", "DatabaseRecord", DatabaseRecordProgress),
        ("documents", "Documents", CountsWithSkippedCountAndLastEtagAndAttachments),
        ("revision_documents", "RevisionDocuments", CountsWithSkippedCountAndLastEtagAndAttachments),
        ("tombstones", "Tombstones", CountsWithLastEtag),
        ("conflicts", "Conflicts", CountsWithLastEtag),
        ("identities", "Identities", CountsWithLastEtag),
        ("indexes", "Indexes", Counts),
        ("compare_exchange", "CompareExchange", CountsWithLastEtag),
        ("subscriptions", "Subscriptions", Counts),
        ("counters", "Counters", CountsWithSkippedCountAndLastEtag),
        ("compare_exchange_tombstones", "CompareExchangeTombstones", Counts),
        ("time_series", "TimeSeries", CountsWithSkippedCountAndLastEtag),
        ("replication_hub_certificates", "ReplicationHubCertificates", Counts),
        ("time_series_deleted_ranges", "TimeSeriesDeletedRanges", CountsWithSkippedCountAndLastEtag),
    )

    def __init__(self):
        for attribute, _, counts_type in self._SECTIONS:
            setattr(self, attribute, counts_type())

    def to_json(self) -> Dict[str, Any]:
        return {key: getattr(self, attribute).to_json() for attribute, key, _ in self._SECTIONS}

    def _fill_from_json(self, json_dict: Dict[str, Any]) -> None:
        for attribute, key, counts_type in self._SECTIONS:
            setattr(self, attribute, counts_type.from_json(json_dict.get(key)))

    @classmethod
    def from_json(cls, json_dict: Optional[Dict[str, Any]]):
        progress = cls()
        if json_dict:
            progress._fill_from_json(json_dict)
        return progress


class SmugglerResult(SmugglerProgressBase):
    """
    What a smuggler export or import actually moved. Read it off a finished operation:

        operation = store.smuggler.import_data(options, "dump.ravendbdump")
        result = operation.wait_for_completion()
        print(result.documents.read_count)
    """

    def __init__(self):
        super().__init__()
        self.messages: List[str] = []
        self.elapsed: Optional[str] = None
        self.message: Optional[str] = None

    def to_json(self) -> Dict[str, Any]:
        json_dict = super().to_json()
        json_dict["Messages"] = self.messages
        json_dict["Elapsed"] = self.elapsed
        json_dict["Message"] = self.message
        return json_dict

    def _fill_from_json(self, json_dict: Dict[str, Any]) -> None:
        super()._fill_from_json(json_dict)
        self.messages = json_dict.get("Messages") or []
        self.elapsed = json_dict.get("Elapsed")
        self.message = json_dict.get("Message")

    def __repr__(self) -> str:
        return f"Documents: {self.documents!r} Revisions: {self.revision_documents!r} Indexes: {self.indexes!r}"
