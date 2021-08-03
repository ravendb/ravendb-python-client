class MetadataAsDictionary:
    def __init__(self, metadata=None, parent=None, parent_key=None):
        self._parent = parent
        self._parent_key = parent_key
        self._is_dirty = False
        if metadata is None or len(metadata) == 0:
            self._metadata = {}
        else:
            self._initialize(metadata)

    def __setitem__(self, key, value):
        self._is_dirty = True
        self._metadata[key] = value

    def __delitem__(self, key):
        self._is_dirty = True
        del self._metadata[key]

    def __getitem__(self, item):
        return self._metadata[item]

    def __getattr__(self, item):
        if item in ["update", "pop", "popitem", "clear"]:
            self._is_dirty = True
        return self._metadata.__getattribute__(item)

    @property
    def is_dirty(self):
        return self._is_dirty

    @property
    def metadata(self):
        return self._metadata

    @is_dirty.setter
    def is_dirty(self, value):
        self._is_dirty = value

    @property
    def has_parent(self):
        return self._parent is not None

    def _initialize(self, metadata_obj):
        self._is_dirty = True
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

    def _convert_value(self, key, value):
        if isinstance(value, (int, str, bool, float, type(None))):
            return value
        if hasattr(value, "__iter__") and type(value) is not dict:  # some collection
            return list(map(lambda x: self._convert_value(key, x), value))
        # not iterable, not simple type -> object or dictionary
        return MetadataAsDictionary(value, parent=self, parent_key=key)
