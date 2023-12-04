from __future__ import annotations

from typing import Optional

from ravendb.primitives import constants


class MetadataAsDictionary:
    RESERVED_METADATA_PROPERTIES = {
        constants.Documents.Metadata.COLLECTION,
        constants.Documents.Metadata.ID,
        constants.Documents.Metadata.CHANGE_VECTOR,
        constants.Documents.Metadata.LAST_MODIFIED,
        constants.Documents.Metadata.RAVEN_PYTHON_TYPE,
    }

    def __init__(self, metadata=None, parent=None, parent_key=None):
        self._source = None if isinstance(metadata, dict) else metadata
        self._parent = parent
        self._parent_key = parent_key
        self._has_changes: Optional[bool] = None
        if metadata is None or len(metadata) == 0:
            self._metadata = {}
        else:
            self._initialize(metadata)

    def __len__(self):
        return len(self._metadata)

    def __setitem__(self, key, value):
        current_value = self._metadata.get(key, None)
        if current_value is None or current_value != value:
            self._metadata[key] = value
            self._has_changes = True
        return None

    def __delitem__(self, key):
        self._has_changes = True
        del self._metadata[key]

    def __contains__(self, item):
        return item in self._metadata

    def __getitem__(self, item):
        return self._metadata[item]

    def __getattr__(self, item):
        if item in ["update", "popitem"]:
            self.is_dirty = True
            return self._metadata.__getattribute__(item)
        elif item in ["pop", "clear"]:
            return super().__getattribute__(item)
        return self._metadata.__getattribute__(item)

    def clear(self) -> None:
        if self._metadata is None:
            self._initialize(self._source)

        keys_to_remove = set()

        for key, value in self._metadata.items():
            if key in self.RESERVED_METADATA_PROPERTIES:
                continue
            keys_to_remove.add(key)

        if keys_to_remove:
            for s in keys_to_remove:
                del self._metadata[s]
            self._has_changes = True

    def pop(self, key, default=None):  # todo: it'll never throw any exception, because default is always sent
        if self._metadata is None:
            self._initialize(self._source)
        if key in self._metadata:
            self._has_changes = True
        return self._metadata.pop(key, default)

    @property
    def is_dirty(self):
        return self._metadata is not None and self._has_changes is True

    @property
    def metadata(self):
        return self._metadata

    @is_dirty.setter
    def is_dirty(self, value):
        self._has_changes = value

    @property
    def has_parent(self):
        return self._parent is not None

    @classmethod
    def materialize_from_json(cls, metadata: dict) -> MetadataAsDictionary:
        result = cls(None)
        result._initialize(metadata)
        return result

    def _initialize(self, metadata_obj):
        # assign dictionary
        if isinstance(metadata_obj, dict):
            self._metadata = {}
            for key, value in metadata_obj.items():
                self._metadata[key] = self._convert_value(key, value)
            return

        # fetch object fields and put them into dict
        self._metadata = {}
        for key, value in metadata_obj.__dict__.items():
            self._metadata[key] = self._convert_value(key, value)

        if self._parent is not None:  # mark parent as dirty
            self._parent.update(self._parent_key, self)

        return self._metadata

    def _convert_value(self, key: str, value):
        if isinstance(value, (int, str, bool, float, type(None))):
            return value
        if hasattr(value, "__iter__") and type(value) is not dict:  # some collection
            return list(map(lambda x: self._convert_value(key, x), value))
        # not iterable, not simple type -> object or dictionary
        return MetadataAsDictionary(value, parent=self, parent_key=key)
