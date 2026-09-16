from __future__ import annotations

import enum
from typing import Any, Dict, List, Optional


class CdcColumnType(enum.Enum):
    """Controls how a CDC Sink column is stored in the target RavenDB document."""

    # Document property with standard type conversion. JSON/JSONB columns land as plain
    # strings unless explicitly marked as JSON.
    DEFAULT = "Default"
    # Parse the string value as its native JSON type: objects, arrays, strings, numbers,
    # booleans and null.
    JSON = "Json"
    # Store as a RavenDB attachment instead of a document property.
    ATTACHMENT = "Attachment"

    def __str__(self) -> str:
        return self.value


class CdcSinkRelationType(enum.Enum):
    """How an embedded table's rows are stored inside the parent document."""

    # One-to-many, as a JSON array.
    ARRAY = "Array"
    # One-to-many, as a JSON object keyed by primary key value(s). Composite keys are "pk1,pk2".
    MAP = "Map"
    # Many-to-one, as a single value/object.
    VALUE = "Value"

    def __str__(self) -> str:
        return self.value


class CdcColumnMapping:
    """Maps a single SQL column to a RavenDB document property or attachment."""

    def __init__(
        self,
        column: str = None,
        name: str = None,
        type_: CdcColumnType = CdcColumnType.DEFAULT,
    ):
        self.column = column
        # Document property name for Default and Json, attachment name for Attachment.
        self.name = name
        self.type_ = type_

    def to_json(self) -> Dict[str, Any]:
        json_dict = {
            "Column": self.column,
            "Name": self.name,
        }
        # The server treats an absent Type as Default, so it is only written when it differs.
        if self.type_ is not None and self.type_ != CdcColumnType.DEFAULT:
            json_dict["Type"] = self.type_.value
        return json_dict

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> CdcColumnMapping:
        type_ = json_dict.get("Type")
        return cls(
            column=json_dict.get("Column"),
            name=json_dict.get("Name"),
            type_=CdcColumnType(type_) if type_ else CdcColumnType.DEFAULT,
        )


class CdcSinkOnDeleteConfig:
    """
    Controls how DELETE events are handled for a CDC Sink table, root or embedded.
    Leave it unset to have deletes processed normally: root documents are deleted and
    embedded items are removed from their parent.
    """

    def __init__(self, patch: str = None, ignore_deletes: bool = False):
        # JavaScript patch that runs when a DELETE event arrives, before the delete is
        # applied. For root tables `this` is the document, for embedded tables the parent;
        # `$row` is the raw CDC row of the DELETE event.
        self.patch = patch
        # When True the DELETE is not applied - the patch (if any) still runs first.
        self.ignore_deletes = ignore_deletes

    def to_json(self) -> Dict[str, Any]:
        return {
            "Patch": self.patch,
            "IgnoreDeletes": self.ignore_deletes,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> CdcSinkOnDeleteConfig:
        return cls(
            patch=json_dict.get("Patch"),
            ignore_deletes=json_dict.get("IgnoreDeletes", False),
        )


class CdcSinkPostgresSettings:
    """
    PostgreSQL-specific settings. Optional on creation - the server auto-fills generated
    names when omitted - and immutable once set.
    """

    def __init__(self, publication_name: str = None, slot_name: str = None):
        # Publication used for logical replication, auto-filled as rvn_cdc_p_{guid}.
        self.publication_name = publication_name
        # Logical replication slot, auto-filled as rvn_cdc_s_{guid}.
        self.slot_name = slot_name

    def to_json(self) -> Dict[str, Any]:
        return {
            "PublicationName": self.publication_name,
            "SlotName": self.slot_name,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> CdcSinkPostgresSettings:
        return cls(
            publication_name=json_dict.get("PublicationName"),
            slot_name=json_dict.get("SlotName"),
        )


class CdcSinkLinkedTableConfig:
    """A table referenced by document ID link rather than embedded."""

    def __init__(
        self,
        source_table_schema: str = None,
        source_table_name: str = None,
        property_name: str = None,
        join_columns: List[str] = None,
        linked_collection_name: str = None,
    ):
        self.source_table_schema = source_table_schema
        self.source_table_name = source_table_name
        # Property name in the document, e.g. "Customer".
        self.property_name = property_name
        # Foreign key columns used to resolve the link.
        self.join_columns = join_columns or []
        # Target collection used for document ID generation, e.g. "Customers" -> "Customers/ALFKI".
        self.linked_collection_name = linked_collection_name

    def to_json(self) -> Dict[str, Any]:
        return {
            "SourceTableSchema": self.source_table_schema,
            "SourceTableName": self.source_table_name,
            "PropertyName": self.property_name,
            "JoinColumns": self.join_columns,
            "LinkedCollectionName": self.linked_collection_name,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> CdcSinkLinkedTableConfig:
        return cls(
            source_table_schema=json_dict.get("SourceTableSchema"),
            source_table_name=json_dict.get("SourceTableName"),
            property_name=json_dict.get("PropertyName"),
            join_columns=json_dict.get("JoinColumns"),
            linked_collection_name=json_dict.get("LinkedCollectionName"),
        )


class CdcSinkEmbeddedTableConfig:
    """A table stored as a nested object, array or map inside its parent's documents."""

    def __init__(
        self,
        source_table_schema: str = None,
        source_table_name: str = None,
        property_name: str = None,
        columns: List[CdcColumnMapping] = None,
        primary_key_columns: List[str] = None,
        join_columns: List[str] = None,
        type_: CdcSinkRelationType = CdcSinkRelationType.ARRAY,
        patch: str = None,
        on_delete: CdcSinkOnDeleteConfig = None,
        case_sensitive_keys: bool = False,
        embedded_tables: List[CdcSinkEmbeddedTableConfig] = None,
        linked_tables: List[CdcSinkLinkedTableConfig] = None,
    ):
        self.source_table_schema = source_table_schema
        self.source_table_name = source_table_name
        # Property name in the parent document, e.g. "Lines".
        self.property_name = property_name
        self.columns = columns or []
        # Used to match items within arrays and maps on update and delete.
        self.primary_key_columns = primary_key_columns or []
        # Foreign key columns joining this table to its parent.
        self.join_columns = join_columns or []
        self.type_ = type_
        # JavaScript patch that runs on the parent document after the embedded operation
        # has been applied. `this` is the parent, `$row` the raw CDC row, `$old` the item
        # as it was before this event (None for inserts).
        self.patch = patch
        self.on_delete = on_delete
        # When False (default) string primary key values and map keys compare
        # case-insensitively.
        self.case_sensitive_keys = case_sensitive_keys
        # Deep nesting. Requires the nested table to carry a denormalized FK to the root.
        self.embedded_tables = embedded_tables or []
        self.linked_tables = linked_tables or []

    def to_json(self) -> Dict[str, Any]:
        return {
            "SourceTableSchema": self.source_table_schema,
            "SourceTableName": self.source_table_name,
            "PropertyName": self.property_name,
            "Columns": [column.to_json() for column in self.columns],
            "PrimaryKeyColumns": self.primary_key_columns,
            "JoinColumns": self.join_columns,
            "Type": self.type_.value if self.type_ else None,
            "Patch": self.patch,
            "OnDelete": self.on_delete.to_json() if self.on_delete else None,
            "CaseSensitiveKeys": self.case_sensitive_keys,
            "EmbeddedTables": [embedded.to_json() for embedded in self.embedded_tables],
            "LinkedTables": [linked.to_json() for linked in self.linked_tables],
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> CdcSinkEmbeddedTableConfig:
        type_ = json_dict.get("Type")
        on_delete = json_dict.get("OnDelete")
        return cls(
            source_table_schema=json_dict.get("SourceTableSchema"),
            source_table_name=json_dict.get("SourceTableName"),
            property_name=json_dict.get("PropertyName"),
            columns=[CdcColumnMapping.from_json(column) for column in json_dict.get("Columns") or []],
            primary_key_columns=json_dict.get("PrimaryKeyColumns"),
            join_columns=json_dict.get("JoinColumns"),
            type_=CdcSinkRelationType(type_) if type_ else CdcSinkRelationType.ARRAY,
            patch=json_dict.get("Patch"),
            on_delete=CdcSinkOnDeleteConfig.from_json(on_delete) if on_delete else None,
            case_sensitive_keys=json_dict.get("CaseSensitiveKeys", False),
            embedded_tables=[cls.from_json(embedded) for embedded in json_dict.get("EmbeddedTables") or []],
            linked_tables=[
                CdcSinkLinkedTableConfig.from_json(linked) for linked in json_dict.get("LinkedTables") or []
            ],
        )


class CdcSinkTableConfig:
    """Maps a source SQL table to a RavenDB collection."""

    def __init__(
        self,
        collection_name: str = None,
        source_table_schema: str = None,
        source_table_name: str = None,
        columns: List[CdcColumnMapping] = None,
        primary_key_columns: List[str] = None,
        patch: str = None,
        on_delete: CdcSinkOnDeleteConfig = None,
        disabled: bool = False,
        embedded_tables: List[CdcSinkEmbeddedTableConfig] = None,
        linked_tables: List[CdcSinkLinkedTableConfig] = None,
    ):
        self.collection_name = collection_name
        self.source_table_schema = source_table_schema
        self.source_table_name = source_table_name
        self.columns = columns or []
        # Used for document ID generation.
        self.primary_key_columns = primary_key_columns or []
        # JavaScript patch that runs after column mapping and embedded operations.
        # `this` is the mapped document, `$row` the raw CDC row, `$old` the document as
        # stored before this event (None for inserts).
        self.patch = patch
        self.on_delete = on_delete
        # When True the table is skipped entirely: no initial load and no change capture.
        self.disabled = disabled
        self.embedded_tables = embedded_tables or []
        self.linked_tables = linked_tables or []

    def to_json(self) -> Dict[str, Any]:
        return {
            "CollectionName": self.collection_name,
            "SourceTableSchema": self.source_table_schema,
            "SourceTableName": self.source_table_name,
            "Columns": [column.to_json() for column in self.columns],
            "PrimaryKeyColumns": self.primary_key_columns,
            "Patch": self.patch,
            "OnDelete": self.on_delete.to_json() if self.on_delete else None,
            "Disabled": self.disabled,
            "EmbeddedTables": [embedded.to_json() for embedded in self.embedded_tables],
            "LinkedTables": [linked.to_json() for linked in self.linked_tables],
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> CdcSinkTableConfig:
        on_delete = json_dict.get("OnDelete")
        return cls(
            collection_name=json_dict.get("CollectionName"),
            source_table_schema=json_dict.get("SourceTableSchema"),
            source_table_name=json_dict.get("SourceTableName"),
            columns=[CdcColumnMapping.from_json(column) for column in json_dict.get("Columns") or []],
            primary_key_columns=json_dict.get("PrimaryKeyColumns"),
            patch=json_dict.get("Patch"),
            on_delete=CdcSinkOnDeleteConfig.from_json(on_delete) if on_delete else None,
            disabled=json_dict.get("Disabled", False),
            embedded_tables=[
                CdcSinkEmbeddedTableConfig.from_json(embedded) for embedded in json_dict.get("EmbeddedTables") or []
            ],
            linked_tables=[
                CdcSinkLinkedTableConfig.from_json(linked) for linked in json_dict.get("LinkedTables") or []
            ],
        )


class CdcSinkConfiguration:
    """A CDC Sink task: an SQL source streamed into RavenDB collections."""

    def __init__(
        self,
        name: str = None,
        connection_string_name: str = None,
        tables: List[CdcSinkTableConfig] = None,
        task_id: int = 0,
        disabled: bool = False,
        mentor_node: str = None,
        pin_to_mentor_node: bool = False,
        postgres: CdcSinkPostgresSettings = None,
        skip_initial_load: bool = False,
    ):
        self.name = name
        self.connection_string_name = connection_string_name
        self.tables = tables or []
        self.task_id = task_id
        self.disabled = disabled
        self.mentor_node = mentor_node
        self.pin_to_mentor_node = pin_to_mentor_node
        # PostgreSQL only. None for SQL Server, auto-filled on creation if omitted.
        self.postgres = postgres
        # Skip the initial full-table load and start streaming changes straight away.
        # For a target database that is already populated, e.g. from a prior migration.
        self.skip_initial_load = skip_initial_load

    def to_json(self) -> Dict[str, Any]:
        return {
            "Name": self.name,
            "TaskId": self.task_id,
            "Disabled": self.disabled,
            "ConnectionStringName": self.connection_string_name,
            "MentorNode": self.mentor_node,
            "PinToMentorNode": self.pin_to_mentor_node,
            "Tables": [table.to_json() for table in self.tables],
            "Postgres": self.postgres.to_json() if self.postgres else None,
            "SkipInitialLoad": self.skip_initial_load,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> CdcSinkConfiguration:
        postgres = json_dict.get("Postgres")
        return cls(
            name=json_dict.get("Name"),
            connection_string_name=json_dict.get("ConnectionStringName"),
            tables=[CdcSinkTableConfig.from_json(table) for table in json_dict.get("Tables") or []],
            task_id=json_dict.get("TaskId", 0),
            disabled=json_dict.get("Disabled", False),
            mentor_node=json_dict.get("MentorNode"),
            pin_to_mentor_node=json_dict.get("PinToMentorNode", False),
            postgres=CdcSinkPostgresSettings.from_json(postgres) if postgres else None,
            skip_initial_load=json_dict.get("SkipInitialLoad", False),
        )


class CdcSinkProcessState:
    """Which node a CDC Sink task last ran on."""

    def __init__(self, node_tag: str = None, configuration_name: str = None):
        self.node_tag = node_tag
        self.configuration_name = configuration_name

    def to_json(self) -> Dict[str, Any]:
        return {
            "ConfigurationName": self.configuration_name,
            "NodeTag": self.node_tag,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> CdcSinkProcessState:
        return cls(
            node_tag=json_dict.get("NodeTag"),
            configuration_name=json_dict.get("ConfigurationName"),
        )

    @staticmethod
    def generate_item_name(database_name: str, configuration_name: str) -> str:
        return f"values/{database_name}/cdcsink/{configuration_name}"


class CdcSinkTableLoadState:
    """Per-table initial-load progress inside a CDC Sink task state document."""

    def __init__(
        self,
        initial_load_completed: bool = False,
        last_key_values: List[str] = None,
        key_columns: List[str] = None,
    ):
        self.initial_load_completed = initial_load_completed
        # The last primary key values loaded, in primary key column order, so an
        # interrupted initial load can resume.
        self.last_key_values = last_key_values
        self.key_columns = key_columns

    def to_json(self) -> Dict[str, Any]:
        return {
            "InitialLoadCompleted": self.initial_load_completed,
            "LastKeyValues": self.last_key_values,
            "KeyColumns": self.key_columns,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> CdcSinkTableLoadState:
        return cls(
            initial_load_completed=json_dict.get("InitialLoadCompleted", False),
            last_key_values=json_dict.get("LastKeyValues"),
            key_columns=json_dict.get("KeyColumns"),
        )


class CdcSinkTaskState:
    """
    The state document of a CDC Sink task, stored in the @cdc-states collection.
    Tracks the last processed LSN and per-table initial load progress.
    """

    COLLECTION_NAME = "@cdc-states"

    def __init__(
        self,
        configuration_name: str = None,
        last_lsn: str = None,
        tables: Dict[str, CdcSinkTableLoadState] = None,
    ):
        self.configuration_name = configuration_name
        # The last successfully processed Log Sequence Number, used to resume streaming.
        self.last_lsn = last_lsn
        # Keyed by "schema.tableName".
        self.tables = tables or {}

    def to_json(self) -> Dict[str, Any]:
        return {
            "ConfigurationName": self.configuration_name,
            "LastLsn": self.last_lsn,
            "Tables": {name: state.to_json() for name, state in self.tables.items()},
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> CdcSinkTaskState:
        return cls(
            configuration_name=json_dict.get("ConfigurationName"),
            last_lsn=json_dict.get("LastLsn"),
            tables={
                name: CdcSinkTableLoadState.from_json(state) for name, state in (json_dict.get("Tables") or {}).items()
            },
        )

    @classmethod
    def get_document_id(cls, configuration_name: str) -> str:
        # Configuration names compare case-insensitively, but the document ID keeps the
        # casing it was created with.
        return f"{cls.COLLECTION_NAME}/{configuration_name}"


class AddCdcSinkOperationResult:
    def __init__(self, raft_command_index: Optional[int] = None, task_id: Optional[int] = None):
        self.raft_command_index = raft_command_index
        self.task_id = task_id

    def to_json(self) -> Dict[str, Any]:
        return {"RaftCommandIndex": self.raft_command_index, "TaskId": self.task_id}

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AddCdcSinkOperationResult:
        return cls(
            raft_command_index=json_dict.get("RaftCommandIndex"),
            task_id=json_dict.get("TaskId"),
        )


class UpdateCdcSinkOperationResult(AddCdcSinkOperationResult):
    # Same shape as the add result; kept as its own name so callers read what they got back.
    pass
