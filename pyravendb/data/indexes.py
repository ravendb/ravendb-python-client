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
        return self.value


class FieldIndexing(Enum):
    # Do not index the field value.
    no = "No"
    # Index the tokens produced by running the field's value through an Analyzer
    analyzed = "Analyzed"
    # Index the field's value without using an Analyzer, so it can be searched.
    not_analyzed = "NotAnalyzed"

    def __str__(self):
        return self.value


class IndexDefinition(object):
    def __init__(self, index_map, name=None, **kwargs):
        """
        @param index_map: The map of the index
        :type str or tuple
        @param name: The name of the index
        :type str
        @param kwargs: Can be use to initialize the other option in the index definition
        :type kwargs
        """
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
        self.lock_mod = kwargs.get("lock_mod", IndexLockMode.Unlock)
        self.max_index_outputs_per_document = kwargs.get("max_index_outputs_per_document", None)
        self.sort_options = kwargs.get("sort_options", {})
        self.spatial_indexes = kwargs.get("spatial_indexes", {})
        self.stores = kwargs.get("stores", {})
        self.suggestions = kwargs.get("suggestions", {})
        self.term_vectors = kwargs.get("term_vectors", {})
        self.maps = (index_map,) if isinstance(index_map, str) else tuple(set(index_map, ))

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
    def is_map_reduce(self):
        return True if self.reduce else False

    @property
    def map(self):
        if not isinstance(self.maps, str):
            return self.maps[0]
        return self.maps

    @map.setter
    def map(self, value):
        if len(self.maps) != 0:
            self.maps.pop()
        self.maps.add(value)

    def to_json(self):
        return {"Analyzers": self.analyzers, "DisableInMemoryIndexing": self.disable_in_memory_indexing,
                "Fields": self.fields, "Indexes": {key: str(self.indexes[key]) for key in self.indexes},
                "IndexId": self.index_id,
                "InternalFieldsMapping": self.internal_fields_mapping, "IsCompiled": self._is_compiled,
                "IsMapReduce": self.is_map_reduce, "IsSideBySideIndex": self.is_side_by_side_index,
                "IsTestIndex": self.is_test_index, "LockMode": str(self.lock_mod), "Map": self.map,
                "Maps": self.maps,
                "MaxIndexOutputsPerDocument": self.max_index_outputs_per_document, "Name": self.name,
                "Reduce": self.reduce, "SortOptions": {key: str(self.sort_options[key]) for key in self.sort_options},
                "SpatialIndexes": self.spatial_indexes,
                "Stores": self.stores, "Suggestions": self.suggestions, "TermVectors": self.term_vectors,
                "Type": self.type}


class IndexQuery(object):
    def __init__(self, query="", default_operator=None, start=None, **kwargs):
        """
        @param query: Actual query that will be performed (Lucene syntax).
        :type str
        @param default_operator: The operator of the query (AND or OR) the default value is OR
        :type Enum.QueryOperator
        @param start : offset used to skip a number of results from a query
        :type list
        @param page_size : only using for query 
        :type str
        """
        self.query = query
        self.__page_size_set = False
        self._page_size = 128
        if "page_size" in kwargs:
            self.page_size = kwargs["page_size"]
        self.default_operator = default_operator
        self.sort_hints = kwargs.get("sort_hints", {})
        self.sort_fields = kwargs.get("sort_fields", {})
        self.fetch = kwargs.get("fetch", [])
        self.wait_for_non_stale_results = kwargs.get("wait_for_non_stale_results", False)
        self.start = start

    @property
    def page_size(self):
        return self._page_size

    @page_size.setter
    def page_size(self, value):
        self._page_size = value
        self.__page_size_set = True
