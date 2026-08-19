from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, Tuple


class CdcSinkTablesDict(dict):
    """A dict whose string-key lookups ignore case while iteration keeps the stored key casing.

    Mirrors the C# Dictionary<string, CdcSinkTableLoadState>(StringComparer.OrdinalIgnoreCase):
    lookups compare case-insensitively, but enumeration yields the original keys.
    """

    def __init__(self, *args, **kwargs):
        super().__init__()
        self._original_keys: Dict[str, str] = {}
        self.update(*args, **kwargs)

    @staticmethod
    def _lower(key):
        return key.lower() if isinstance(key, str) else key

    def __setitem__(self, key, value) -> None:
        lowered = self._lower(key)
        self._original_keys[lowered] = key
        super().__setitem__(lowered, value)

    def __getitem__(self, key):
        return super().__getitem__(self._lower(key))

    def __delitem__(self, key) -> None:
        lowered = self._lower(key)
        self._original_keys.pop(lowered, None)
        super().__delitem__(lowered)

    def __contains__(self, key) -> bool:
        return super().__contains__(self._lower(key))

    def get(self, key, default=None):
        return super().get(self._lower(key), default)

    def pop(self, key, *args):
        lowered = self._lower(key)
        self._original_keys.pop(lowered, None)
        return super().pop(lowered, *args)

    def setdefault(self, key, default=None):
        if key not in self:
            self[key] = default
        return self[key]

    def update(self, *args, **kwargs) -> None:
        for key, value in dict(*args, **kwargs).items():
            self[key] = value

    def keys(self):
        return list(self._original_keys.values())

    def items(self):
        return [(self._original_keys[lowered], value) for lowered, value in super().items()]

    def values(self):
        return list(super().values())

    def __iter__(self) -> Iterator:
        for key in self._original_keys.values():
            yield key

    def __len__(self) -> int:
        return super().__len__()


class CdcSinkTableLoadState:
    def __init__(
        self,
        initial_load_completed: bool = False,
        last_key_values: Optional[List[str]] = None,
        key_columns: Optional[List[str]] = None,
    ):
        self.initial_load_completed = initial_load_completed
        self.last_key_values = last_key_values
        self.key_columns = key_columns

    def to_json(self) -> Dict[str, Any]:
        return {
            "InitialLoadCompleted": self.initial_load_completed,
            "LastKeyValues": self.last_key_values if self.last_key_values is not None else None,
            "KeyColumns": self.key_columns if self.key_columns is not None else None,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "CdcSinkTableLoadState":
        return cls(
            initial_load_completed=json_dict.get("InitialLoadCompleted", False),
            last_key_values=json_dict.get("LastKeyValues"),
            key_columns=json_dict.get("KeyColumns"),
        )


class CdcSinkTaskState:
    collection_name = "@cdc-states"

    def __init__(
        self,
        last_lsn: Optional[str] = None,
        tables: Optional[CdcSinkTablesDict] = None,
        configuration_name: Optional[str] = None,
    ):
        self.last_lsn = last_lsn
        self.tables = tables if tables is not None else CdcSinkTablesDict()
        self.configuration_name = configuration_name

    @classmethod
    def get_document_id(cls, configuration_name: str) -> str:
        return f"{cls.collection_name}/{configuration_name}"

    def to_json(self) -> Dict[str, Any]:
        tables_json: Dict[str, Any] = {}
        for key in self.tables.keys():
            tables_json[key] = self.tables[key].to_json()
        return {
            "ConfigurationName": self.configuration_name,
            "LastLsn": self.last_lsn,
            "Tables": tables_json,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "CdcSinkTaskState":
        tables_raw = json_dict.get("Tables")
        tables = CdcSinkTablesDict()
        if tables_raw:
            for key, value in tables_raw.items():
                tables[key] = CdcSinkTableLoadState.from_json(value)
        return cls(
            last_lsn=json_dict.get("LastLsn"),
            tables=tables,
            configuration_name=json_dict.get("ConfigurationName"),
        )
