from __future__ import annotations

import enum
from typing import Any, Dict, List, Optional


def _is_blank(value: Optional[str]) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _add_case_insensitive(keys: set, value: str) -> bool:
    key = value.lower()
    if key in keys:
        return False
    keys.add(key)
    return True


class CdcColumnType(enum.Enum):
    DEFAULT = "Default"
    JSON = "Json"
    ATTACHMENT = "Attachment"


class CdcSinkRelationType(enum.Enum):
    ARRAY = "Array"
    MAP = "Map"
    VALUE = "Value"


class CdcColumnMapping:
    def __init__(self, column: Optional[str] = None, name: Optional[str] = None, type: CdcColumnType = None):
        self.column = column
        self.name = name
        self.type = type if type is not None else CdcColumnType.DEFAULT

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
            type=CdcColumnType(type_raw) if type_raw is not None else None,
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
        type: CdcSinkRelationType = None,
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
        self.type = type if type is not None else CdcSinkRelationType.ARRAY
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
            "OnDelete": self.on_delete.to_json() if self.on_delete else None,
            "CaseSensitiveKeys": self.case_sensitive_keys,
            "EmbeddedTables": [embedded.to_json() for embedded in self.embedded_tables],
            "LinkedTables": [linked.to_json() for linked in self.linked_tables],
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "CdcSinkEmbeddedTableConfig":
        type_raw = json_dict.get("Type")
        return cls(
            source_table_schema=json_dict.get("SourceTableSchema"),
            source_table_name=json_dict.get("SourceTableName"),
            property_name=json_dict.get("PropertyName"),
            columns=[CdcColumnMapping.from_json(c) for c in json_dict.get("Columns") or []],
            primary_key_columns=json_dict.get("PrimaryKeyColumns") or [],
            join_columns=json_dict.get("JoinColumns") or [],
            type=CdcSinkRelationType(type_raw) if type_raw is not None else None,
            patch=json_dict.get("Patch"),
            on_delete=(CdcSinkOnDeleteConfig.from_json(json_dict["OnDelete"]) if json_dict.get("OnDelete") else None),
            case_sensitive_keys=json_dict.get("CaseSensitiveKeys", False),
            embedded_tables=[CdcSinkEmbeddedTableConfig.from_json(e) for e in json_dict.get("EmbeddedTables") or []],
            linked_tables=[CdcSinkLinkedTableConfig.from_json(l) for l in json_dict.get("LinkedTables") or []],
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
            "OnDelete": self.on_delete.to_json() if self.on_delete else None,
            "Disabled": self.disabled,
            "EmbeddedTables": [embedded.to_json() for embedded in self.embedded_tables],
            "LinkedTables": [linked.to_json() for linked in self.linked_tables],
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "CdcSinkTableConfig":
        return cls(
            collection_name=json_dict.get("CollectionName"),
            source_table_schema=json_dict.get("SourceTableSchema"),
            source_table_name=json_dict.get("SourceTableName"),
            columns=[CdcColumnMapping.from_json(c) for c in json_dict.get("Columns") or []],
            primary_key_columns=json_dict.get("PrimaryKeyColumns") or [],
            patch=json_dict.get("Patch"),
            on_delete=(CdcSinkOnDeleteConfig.from_json(json_dict["OnDelete"]) if json_dict.get("OnDelete") else None),
            disabled=json_dict.get("Disabled", False),
            embedded_tables=[CdcSinkEmbeddedTableConfig.from_json(e) for e in json_dict.get("EmbeddedTables") or []],
            linked_tables=[CdcSinkLinkedTableConfig.from_json(l) for l in json_dict.get("LinkedTables") or []],
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
    def from_json(cls, json_dict: Dict[str, Any]) -> "CdcSinkConfiguration":
        postgres_dict = json_dict.get("Postgres")
        return cls(
            task_id=json_dict.get("TaskId", 0),
            disabled=json_dict.get("Disabled", False),
            name=json_dict.get("Name"),
            mentor_node=json_dict.get("MentorNode"),
            pin_to_mentor_node=json_dict.get("PinToMentorNode", False),
            connection_string_name=json_dict.get("ConnectionStringName"),
            tables=[CdcSinkTableConfig.from_json(t) for t in json_dict.get("Tables") or []],
            postgres=CdcSinkPostgresSettings.from_json(postgres_dict) if postgres_dict else None,
            skip_initial_load=json_dict.get("SkipInitialLoad", False),
        )

    def validate(self) -> List[str]:
        """Returns the validation errors of this configuration, mirroring the server side CdcSinkConfiguration."""
        errors: List[str] = []

        if _is_blank(self.name):
            errors.append("Name of CDC Sink configuration cannot be empty")

        if _is_blank(self.connection_string_name):
            errors.append("ConnectionStringName cannot be empty")

        # The referenced connection string is validated server side; this client holds only its name.
        if len(self.tables) == 0:
            errors.append("'Tables' list cannot be empty.")

        unique_names = set()

        for table in self.tables:
            if _is_blank(table.collection_name):
                errors.append("Table collection name must not be empty")

            if _is_blank(table.source_table_name):
                errors.append(f"Table '{table.collection_name}' must have a source table name")

            if not table.primary_key_columns:
                errors.append(f"Table '{table.collection_name}' must have at least one primary key column")

            if not table.columns:
                errors.append(f"Table '{table.collection_name}' must have at least one column mapping")

            name_key = table.collection_name.lower() if table.collection_name else None
            if name_key in unique_names:
                errors.append(f"Table name '{table.collection_name}' is already defined. Table names must be unique")
            else:
                unique_names.add(name_key)

            self._validate_primary_key_columns_exist(
                table.collection_name, table.primary_key_columns, table.columns, errors
            )
            self._validate_columns_and_property_names(
                table.collection_name, table.columns, table.embedded_tables, table.linked_tables, errors
            )
            self._validate_embedded_tables(table.embedded_tables, table.collection_name, errors)
            self._validate_linked_tables(table.linked_tables, table.collection_name, errors)

        return errors

    @staticmethod
    def _validate_primary_key_columns_exist(
        table_name: str,
        primary_key_columns: Optional[List[str]],
        columns: Optional[List[CdcColumnMapping]],
        errors: List[str],
    ) -> None:
        if not primary_key_columns or not columns:
            return

        column_names = set()
        for column in columns:
            if column.column is not None:
                column_names.add(column.column.lower())

        for primary_key in primary_key_columns:
            if primary_key is None or primary_key.lower() not in column_names:
                errors.append(
                    f"Table '{table_name}': primary key column '{primary_key}' is not listed in the column "
                    "mappings. Primary key columns must be included in the column mappings so they are stored in "
                    "the document \u2014 without them, the system cannot identify which array element to update or "
                    "delete on subsequent changes. Add a column mapping for this column "
                    f'(e.g. {{ Column = "{primary_key}", Name = "..." }}) or correct the primary key column name.'
                )

    @staticmethod
    def _validate_columns_and_property_names(
        table_name: str,
        columns: Optional[List[CdcColumnMapping]],
        embedded_tables: Optional[List[CdcSinkEmbeddedTableConfig]],
        linked_tables: Optional[List[CdcSinkLinkedTableConfig]],
        errors: List[str],
    ) -> None:
        column_names = set()
        property_names = set()

        if columns is None:
            errors.append(f"Table '{table_name}': Columns list is null")
            return

        for column in columns:
            if _is_blank(column.column):
                name_hint = "" if _is_blank(column.name) else f" (Name: '{column.name}')"
                errors.append(f"Table '{table_name}': column mapping has an empty Column name{name_hint}")
                continue

            if _is_blank(column.name):
                errors.append(f"Table '{table_name}': column '{column.column}' has an empty Name")
                continue

            if not _add_case_insensitive(column_names, column.column):
                errors.append(f"Table '{table_name}': duplicate column '{column.column}'")

            if not _add_case_insensitive(property_names, column.name):
                errors.append(f"Table '{table_name}': duplicate target name '{column.name}' (used by multiple columns)")

        if embedded_tables:
            for embedded in embedded_tables:
                if embedded.property_name is not None and not _add_case_insensitive(
                    property_names, embedded.property_name
                ):
                    errors.append(
                        f"Table '{table_name}': property name '{embedded.property_name}' from embedded table "
                        f"'{embedded.source_table_name}' conflicts with a column mapping or another "
                        "embedded/linked table"
                    )

        if linked_tables:
            for linked in linked_tables:
                if linked.property_name is not None and not _add_case_insensitive(property_names, linked.property_name):
                    errors.append(
                        f"Table '{table_name}': property name '{linked.property_name}' from linked table "
                        f"'{linked.source_table_name}' conflicts with a column mapping or another "
                        "embedded/linked table"
                    )

    @classmethod
    def _validate_embedded_tables(
        cls,
        embedded_tables: Optional[List[CdcSinkEmbeddedTableConfig]],
        parent_name: str,
        errors: List[str],
    ) -> None:
        if embedded_tables is None:
            return

        property_names = set()

        for embedded in embedded_tables:
            if _is_blank(embedded.source_table_name):
                errors.append(f"Embedded table under '{parent_name}' must have a source table name")
            elif parent_name and embedded.source_table_name.lower() == parent_name.lower():
                errors.append(
                    f"Embedded table '{embedded.source_table_name}' under '{parent_name}' cannot reference "
                    "its own parent table"
                )

            if _is_blank(embedded.property_name):
                errors.append(
                    f"Embedded table '{embedded.source_table_name}' under '{parent_name}' must have a property name"
                )
            elif not _add_case_insensitive(property_names, embedded.property_name):
                errors.append(
                    f"Embedded table property name '{embedded.property_name}' under '{parent_name}' is already "
                    "defined. Property names must be unique within the same parent"
                )

            if not embedded.join_columns:
                errors.append(
                    f"Embedded table '{embedded.source_table_name}' under '{parent_name}' must have join columns"
                )

            if not embedded.primary_key_columns:
                errors.append(
                    f"Embedded table '{embedded.source_table_name}' under '{parent_name}' must have primary key columns"
                )

            if not embedded.columns:
                errors.append(
                    f"Embedded table '{embedded.source_table_name}' under '{parent_name}' must have at least one column mapping"
                )

            cls._validate_primary_key_columns_exist(
                embedded.source_table_name, embedded.primary_key_columns, embedded.columns, errors
            )
            cls._validate_columns_and_property_names(
                embedded.source_table_name, embedded.columns, embedded.embedded_tables, embedded.linked_tables, errors
            )
            cls._validate_embedded_tables(embedded.embedded_tables, embedded.source_table_name, errors)
            cls._validate_linked_tables(embedded.linked_tables, embedded.source_table_name, errors)

    @staticmethod
    def _validate_linked_tables(
        linked_tables: Optional[List[CdcSinkLinkedTableConfig]],
        parent_name: str,
        errors: List[str],
    ) -> None:
        if linked_tables is None:
            return

        property_names = set()

        for linked in linked_tables:
            if _is_blank(linked.source_table_name):
                errors.append(f"Linked table under '{parent_name}' must have a source table name")

            if _is_blank(linked.property_name):
                errors.append(
                    f"Linked table '{linked.source_table_name}' under '{parent_name}' must have a property name"
                )
            elif not _add_case_insensitive(property_names, linked.property_name):
                errors.append(
                    f"Linked table property name '{linked.property_name}' under '{parent_name}' is already defined. "
                    "Property names must be unique within the same parent"
                )

            if _is_blank(linked.linked_collection_name):
                errors.append(
                    f"Linked table '{linked.source_table_name}' under '{parent_name}' must have a linked collection name"
                )

            if not linked.join_columns:
                errors.append(f"Linked table '{linked.source_table_name}' under '{parent_name}' must have join columns")
