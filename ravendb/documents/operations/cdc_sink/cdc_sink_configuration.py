from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional


class CdcColumnType(Enum):
    DEFAULT = "Default"
    JSON = "Json"
    ATTACHMENT = "Attachment"


class CdcSinkRelationType(Enum):
    ARRAY = "Array"
    MAP = "Map"
    VALUE = "Value"


class CdcColumnMapping:
    def __init__(
        self,
        column: Optional[str] = None,
        name: Optional[str] = None,
        type: CdcColumnType = CdcColumnType.DEFAULT,
    ):
        self.column = column
        self.name = name
        self.type = type

    def to_json(self) -> Dict[str, Any]:
        json_dict = {"Column": self.column, "Name": self.name}
        if self.type != CdcColumnType.DEFAULT:
            json_dict["Type"] = self.type.value
        return json_dict

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "CdcColumnMapping":
        type_raw = json_dict.get("Type")
        return cls(
            column=json_dict.get("Column"),
            name=json_dict.get("Name"),
            type=CdcColumnType(type_raw) if type_raw is not None else CdcColumnType.DEFAULT,
        )


class CdcSinkOnDeleteConfig:
    def __init__(self, patch: Optional[str] = None, ignore_deletes: bool = False):
        self.patch = patch
        self.ignore_deletes = ignore_deletes

    def to_json(self) -> Dict[str, Any]:
        return {"Patch": self.patch, "IgnoreDeletes": self.ignore_deletes}

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "CdcSinkOnDeleteConfig":
        return cls(
            patch=json_dict.get("Patch"),
            ignore_deletes=json_dict.get("IgnoreDeletes", False),
        )


class CdcSinkPostgresSettings:
    def __init__(self, publication_name: Optional[str] = None, slot_name: Optional[str] = None):
        self.publication_name = publication_name
        self.slot_name = slot_name

    def to_json(self) -> Dict[str, Any]:
        return {"PublicationName": self.publication_name, "SlotName": self.slot_name}

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "CdcSinkPostgresSettings":
        return cls(
            publication_name=json_dict.get("PublicationName"),
            slot_name=json_dict.get("SlotName"),
        )


class CdcSinkLinkedTableConfig:
    def __init__(
        self,
        source_table_schema: Optional[str] = None,
        source_table_name: Optional[str] = None,
        property_name: Optional[str] = None,
        join_columns: Optional[List[str]] = None,
        linked_collection_name: Optional[str] = None,
    ):
        self.source_table_schema = source_table_schema
        self.source_table_name = source_table_name
        self.property_name = property_name
        self.join_columns = join_columns if join_columns is not None else []
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
    def from_json(cls, json_dict: Dict[str, Any]) -> "CdcSinkLinkedTableConfig":
        return cls(
            source_table_schema=json_dict.get("SourceTableSchema"),
            source_table_name=json_dict.get("SourceTableName"),
            property_name=json_dict.get("PropertyName"),
            join_columns=json_dict.get("JoinColumns") or [],
            linked_collection_name=json_dict.get("LinkedCollectionName"),
        )


class CdcSinkEmbeddedTableConfig:
    def __init__(
        self,
        source_table_schema: Optional[str] = None,
        source_table_name: Optional[str] = None,
        property_name: Optional[str] = None,
        columns: Optional[List[CdcColumnMapping]] = None,
        primary_key_columns: Optional[List[str]] = None,
        join_columns: Optional[List[str]] = None,
        type: CdcSinkRelationType = CdcSinkRelationType.ARRAY,
        patch: Optional[str] = None,
        on_delete: Optional[CdcSinkOnDeleteConfig] = None,
        case_sensitive_keys: bool = False,
        embedded_tables: Optional[List["CdcSinkEmbeddedTableConfig"]] = None,
        linked_tables: Optional[List[CdcSinkLinkedTableConfig]] = None,
    ):
        self.source_table_schema = source_table_schema
        self.source_table_name = source_table_name
        self.property_name = property_name
        self.columns = columns if columns is not None else []
        self.primary_key_columns = primary_key_columns if primary_key_columns is not None else []
        self.join_columns = join_columns if join_columns is not None else []
        self.type = type
        self.patch = patch
        self.on_delete = on_delete
        self.case_sensitive_keys = case_sensitive_keys
        self.embedded_tables = embedded_tables if embedded_tables is not None else []
        self.linked_tables = linked_tables if linked_tables is not None else []

    def to_json(self) -> Dict[str, Any]:
        return {
            "SourceTableSchema": self.source_table_schema,
            "SourceTableName": self.source_table_name,
            "PropertyName": self.property_name,
            "Columns": [column.to_json() for column in self.columns],
            "PrimaryKeyColumns": self.primary_key_columns,
            "JoinColumns": self.join_columns,
            "Type": self.type.value,
            "Patch": self.patch,
            "OnDelete": self.on_delete.to_json() if self.on_delete is not None else None,
            "CaseSensitiveKeys": self.case_sensitive_keys,
            "EmbeddedTables": [embedded.to_json() for embedded in self.embedded_tables],
            "LinkedTables": [linked.to_json() for linked in self.linked_tables],
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "CdcSinkEmbeddedTableConfig":
        type_raw = json_dict.get("Type")
        on_delete_raw = json_dict.get("OnDelete")
        return cls(
            source_table_schema=json_dict.get("SourceTableSchema"),
            source_table_name=json_dict.get("SourceTableName"),
            property_name=json_dict.get("PropertyName"),
            columns=[CdcColumnMapping.from_json(column) for column in (json_dict.get("Columns") or [])],
            primary_key_columns=json_dict.get("PrimaryKeyColumns") or [],
            join_columns=json_dict.get("JoinColumns") or [],
            type=CdcSinkRelationType(type_raw) if type_raw is not None else CdcSinkRelationType.ARRAY,
            patch=json_dict.get("Patch"),
            on_delete=CdcSinkOnDeleteConfig.from_json(on_delete_raw) if on_delete_raw is not None else None,
            case_sensitive_keys=json_dict.get("CaseSensitiveKeys", False),
            embedded_tables=[
                CdcSinkEmbeddedTableConfig.from_json(embedded) for embedded in (json_dict.get("EmbeddedTables") or [])
            ],
            linked_tables=[
                CdcSinkLinkedTableConfig.from_json(linked) for linked in (json_dict.get("LinkedTables") or [])
            ],
        )


class CdcSinkTableConfig:
    def __init__(
        self,
        collection_name: Optional[str] = None,
        source_table_schema: Optional[str] = None,
        source_table_name: Optional[str] = None,
        columns: Optional[List[CdcColumnMapping]] = None,
        primary_key_columns: Optional[List[str]] = None,
        patch: Optional[str] = None,
        on_delete: Optional[CdcSinkOnDeleteConfig] = None,
        disabled: bool = False,
        embedded_tables: Optional[List[CdcSinkEmbeddedTableConfig]] = None,
        linked_tables: Optional[List[CdcSinkLinkedTableConfig]] = None,
    ):
        self.collection_name = collection_name
        self.source_table_schema = source_table_schema
        self.source_table_name = source_table_name
        self.columns = columns if columns is not None else []
        self.primary_key_columns = primary_key_columns if primary_key_columns is not None else []
        self.patch = patch
        self.on_delete = on_delete
        self.disabled = disabled
        self.embedded_tables = embedded_tables if embedded_tables is not None else []
        self.linked_tables = linked_tables if linked_tables is not None else []

    def to_json(self) -> Dict[str, Any]:
        return {
            "CollectionName": self.collection_name,
            "SourceTableSchema": self.source_table_schema,
            "SourceTableName": self.source_table_name,
            "Columns": [column.to_json() for column in self.columns],
            "PrimaryKeyColumns": self.primary_key_columns,
            "Patch": self.patch,
            "OnDelete": self.on_delete.to_json() if self.on_delete is not None else None,
            "Disabled": self.disabled,
            "EmbeddedTables": [embedded.to_json() for embedded in self.embedded_tables],
            "LinkedTables": [linked.to_json() for linked in self.linked_tables],
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "CdcSinkTableConfig":
        on_delete_raw = json_dict.get("OnDelete")
        return cls(
            collection_name=json_dict.get("CollectionName"),
            source_table_schema=json_dict.get("SourceTableSchema"),
            source_table_name=json_dict.get("SourceTableName"),
            columns=[CdcColumnMapping.from_json(column) for column in (json_dict.get("Columns") or [])],
            primary_key_columns=json_dict.get("PrimaryKeyColumns") or [],
            patch=json_dict.get("Patch"),
            on_delete=CdcSinkOnDeleteConfig.from_json(on_delete_raw) if on_delete_raw is not None else None,
            disabled=json_dict.get("Disabled", False),
            embedded_tables=[
                CdcSinkEmbeddedTableConfig.from_json(embedded) for embedded in (json_dict.get("EmbeddedTables") or [])
            ],
            linked_tables=[
                CdcSinkLinkedTableConfig.from_json(linked) for linked in (json_dict.get("LinkedTables") or [])
            ],
        )


class CdcSinkConfiguration:
    def __init__(
        self,
        task_id: int = 0,
        disabled: bool = False,
        name: Optional[str] = None,
        mentor_node: Optional[str] = None,
        pin_to_mentor_node: bool = False,
        connection_string_name: Optional[str] = None,
        tables: Optional[List[CdcSinkTableConfig]] = None,
        postgres: Optional[CdcSinkPostgresSettings] = None,
        skip_initial_load: bool = False,
    ):
        self.task_id = task_id
        self.disabled = disabled
        self.name = name
        self.mentor_node = mentor_node
        self.pin_to_mentor_node = pin_to_mentor_node
        self.connection_string_name = connection_string_name
        self.tables = tables if tables is not None else []
        self.postgres = postgres
        self.skip_initial_load = skip_initial_load

    def get_default_task_name(self) -> str:
        return f"CDC Sink to {self.connection_string_name}"

    def to_json(self) -> Dict[str, Any]:
        return {
            "Name": self.name,
            "TaskId": self.task_id,
            "Disabled": self.disabled,
            "ConnectionStringName": self.connection_string_name,
            "MentorNode": self.mentor_node,
            "PinToMentorNode": self.pin_to_mentor_node,
            "Tables": [table.to_json() for table in self.tables],
            "Postgres": self.postgres.to_json() if self.postgres is not None else None,
            "SkipInitialLoad": self.skip_initial_load,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "CdcSinkConfiguration":
        postgres_raw = json_dict.get("Postgres")
        return cls(
            task_id=json_dict.get("TaskId", 0),
            disabled=json_dict.get("Disabled", False),
            name=json_dict.get("Name"),
            mentor_node=json_dict.get("MentorNode"),
            pin_to_mentor_node=json_dict.get("PinToMentorNode", False),
            connection_string_name=json_dict.get("ConnectionStringName"),
            tables=[CdcSinkTableConfig.from_json(table) for table in (json_dict.get("Tables") or [])],
            postgres=CdcSinkPostgresSettings.from_json(postgres_raw) if postgres_raw is not None else None,
            skip_initial_load=json_dict.get("SkipInitialLoad", False),
        )
