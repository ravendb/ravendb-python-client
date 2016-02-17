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
    # No sort options
    none = 0
    # Sort using term values as Strings.  Sort values are String and lower
    # values are at the front.
    String = 3
    # Sort using term values as encoded Integers.  Sort values are Integer and
    # lower values are at the front.
    Int = 4
    # Sort using term values as encoded Floats.  Sort values are Float and
    # lower values are at the front.
    Float = 5
    # Sort using term values as encoded Longs.  Sort values are Long and
    # lower values are at the front.
    Long = 6
    # Sort using term values as encoded Doubles.
    # Sort values are Double and lower values are at the front.
    Double = 7
    # Sort using term values as encoded Shorts.  Sort values are Short and
    # lower values are at the front.
    Short = 8
    Custom = 9
    # Sort using term values as encoded Bytes.  Sort values are Byte and
    # lower values are at the front.
    Byte = 10
    StringVal = 11

    def __str__(self):
        return self.name


class IndexDefinition(object):
    def __init__(self, maps, name=None):
        self.name = name
        self.reduce = None
        self.analyzers = {}
        self.disable_in_memory_indexing = False
        self.fields = []
        self.index_id = 0
        self.indexes = {}
        self.internal_fields_mapping = {}
        self._is_compiled = False
        self.is_side_by_side_index = False
        self.is_test_index = False
        self.is_map_reduce = False
        self.lock_mod = IndexLockMode.Unlock
        self.max_index_outputs_per_document = None
        self.sort_options = {}
        self.spatial_indexes = {}
        self.stores = {}
        self.suggestions = {}
        self.term_vectors = {}
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
                "Reduce": self.reduce, "SortOptions": {}, "SpatialIndexes": self.spatial_indexes, "Stores": self.stores,
                "Suggestions": self.suggestions, "TermVectors": self.term_vectors, "Type": self.type}


class IndexQuery(object):
    def __init__(self, query="", total_size=0, skipped_results=0):
        """
        @param query: Actual query that will be performed (Lucene syntax).
        :type str
        @param total_size: For internal use only.
        :type int
        @param skipped_results: For internal use only.
        :type int
    """
        self.query = query
        self.total_size = total_size
        self.skipped_results = skipped_results
        self._page_size = 128
        self.__page_size_set = False

    @property
    def page_size(self):
        return self._page_size

    @page_size.setter
    def page_size(self, value):
        self._page_size = value
        self.__page_size_set = True
