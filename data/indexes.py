from enum import Enum


class IndexLockMode(Enum):
    Unlock = 0
    LockedIgnore = 1
    LockedError = 2
    SideBySide = 3

    def __str__(self):
        return self.name


# For server
class SortOptions(Enum):
    # Sort using term values as encoded Floats.  Sort values are Float and
    # lower values are at the front.
    long = "Long"
    # Sort using term values as encoded Doubles.
    # Sort values are Double and lower values are at the front.
    float = "Double"
    custom = "Custom"

    def __str__(self):
        return self.name


class IndexDefinition(object):
    def __init__(self, maps, name=None, **kwargs):
        self.name = name
        self.reduce = kwargs.get("reduce", None)
        self.analyzers = kwargs.get("analyzers", {})
        self.disable_in_memory_indexing = kwargs.get("disable_in_memory_indexing", False)
        self.fields = kwargs.get("fields", [])
        self.index_id = kwargs.get("index_id", 0)
        self.indexes = kwargs.get("indexes", {})
        self.internal_fields_mapping = kwargs.get("internal_fields_mapping", {})
        self._is_compiled = False
        self.is_side_by_side_index = kwargs.get("is_side_by_side_index", False)
        self.is_test_index = kwargs.get("is_test_index", False)
        self.is_map_reduce = kwargs.get("is_map_reduce", False)
        self.lock_mod = kwargs.get("lock_mod", IndexLockMode.Unlock)
        self.max_index_outputs_per_document = kwargs.get("max_index_outputs_per_document", None)
        self.sort_options = kwargs.get("sort_options", {})
        self.spatial_indexes = kwargs.get("spatial_indexes", {})
        self.stores = kwargs.get("stores", {})
        self.suggestions = kwargs.get("suggestions", {})
        self.term_vectors = kwargs.get("term_vectors", {})
        self.maps = maps
        self._map = ""

    @property
    def type(self):
        if self.name and self.name.startswith('Auto/'):
            return "Auto"
        if self._is_compiled:
            return "Compiled"
        if self.is_map_reduce:
            return "MapReduce"
        return "Map"

    @property
    def map(self):
        return list(self.maps)[0]

    @map.setter
    def map(self, value):
        if len(self.maps) != 0:
            self.maps.pop()
        self.maps.add(value)

    def to_json(self):
        return {"Analyzers": self.analyzers, "DisableInMemoryIndexing": self.disable_in_memory_indexing,
                "Fields": self.fields, "Indexes": self.indexes, "IndexId": self.index_id,
                "InternalFieldsMapping": self.internal_fields_mapping, "IsCompiled": self._is_compiled,
                "IsMapReduce": self.is_map_reduce, "IsSideBySideIndex": self.is_side_by_side_index,
                "IsTestIndex": self.is_test_index, "LockMode": str(self.lock_mod), "Map": self.map,
                "Maps": list(self.maps),
                "MaxIndexOutputsPerDocument": self.max_index_outputs_per_document, "Name": self.name,
                "Reduce": self.reduce, "SortOptions": self.sort_options, "SpatialIndexes": self.spatial_indexes,
                "Stores": self.stores, "Suggestions": self.suggestions, "TermVectors": self.term_vectors,
                "Type": self.type}


class IndexQuery(object):
    def __init__(self, query="", total_size=0, skipped_results=0, default_operator=None, **kwargs):
        """
        @param query: Actual query that will be performed (Lucene syntax).
        :type str
        @param total_size: For internal use only.
        :type int
        @param skipped_results: For internal use only.
        :type int
        @param default_operator: The operator of the query (AND or OR) the default value is OR
        :type Enum.QueryOperator
    """
        self.query = query
        self.total_size = total_size
        self.skipped_results = skipped_results
        self._page_size = 128
        self.__page_size_set = False
        self.default_operator = default_operator
        self.sort_hints = kwargs.get("sort_hints", {})

    @property
    def page_size(self):
        return self._page_size

    @page_size.setter
    def page_size(self, value):
        self._page_size = value
        self.__page_size_set = True
