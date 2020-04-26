from datetime import timedelta
from enum import Enum
import sys


class IndexLockMode(Enum):
    unlock = "Unlock"
    locked_ignore = "LockedIgnore"
    locked_error = "LockedError"
    side_by_side = "SideBySide"

    def __str__(self):
        return self.value


class IndexPriority(Enum):
    low = "Low"
    normal = "Normal"
    high = "High"

    def __str__(self):
        return self.value


class IndexSourceType(Enum):
    none = "None"
    documents = "Documents"
    time_series = "TimeSeries"
    counters = "Counters"

    def __str__(self):
        return self.value


# The sort options to use for a particular field
class SortOptions(Enum):
    # No sort options
    none = "None"
    # Sort using term values as Strings. Sort values are str and lower values are at the front.
    str = "String"
    # Sort using term values as encoded Doubles and Longs.
    # Sort values are float or longs and lower values are at the front.
    numeric = "Numeric"

    def __str__(self):
        return self.value


class FieldIndexing(Enum):
    # Do not index the field value.
    no = "No"
    # Index the tokens produced by running the field's value through an Analyzer
    search = "Search"
    # Index the field's value without using an Analyzer, so it can be searched.
    exact = "Exact"
    # Index this field using the default internal analyzer: LowerCaseKeywordAnalyzer
    default = "Default"

    def __str__(self):
        return self.value


class FieldTermVector(Enum):
    # Do not store term vectors
    no = "No"
    # Store the term vectors of each document.
    # A term vector is a list of the document's terms and their number of occurrences in that document.
    yes = "Yes"
    # store the term vector + token position information
    with_positions = "WithPositions"
    # Store the term vector + Token offset information
    with_offsets = "WithOffsets"
    # Store the term vector + Token position and offset information
    with_positions_and_offsets = "WithPositionsAndOffsets"

    def __str__(self):
        return self.value


class IndexDefinition(object):
    def __init__(self, name, maps, configuration=None, **kwargs):
        """
        @param name: The name of the index
        :type str
        @param maps: The map of the index
        :type str ,set or list of str
        @param kwargs: Can be use to initialize the other option in the index definition
        :type kwargs
        """
        self.name = name
        self.configuration = configuration if configuration is not None else {}
        self.reduce = kwargs.get("reduce", None)

        self.index_id = kwargs.get("index_id", 0)
        self.is_test_index = kwargs.get("is_test_index", False)

        # IndexLockMode
        self.lock_mod = kwargs.get("lock_mod", None)

        # The priority of the index IndexPriority
        self.priority = kwargs.get("priority", None)
        if isinstance(maps, str):
            self.maps = (maps,)
        else:
            if isinstance(maps, list):
                maps = set(maps)
            self.maps = tuple(maps, )

        # fields is a  key value dict. the key is the name of the field and the value is IndexFieldOptions
        self.fields = kwargs.get("fields", {})

        # set "OutputReduceToCollection"
        self.output_reduce_to_collection = kwargs.get("output_reduce_to_collection", None)

        self._index_source_type = None

    @property
    def source_type(self):
        if self._index_source_type is None:
            if not self.maps:
                raise ValueError("Index definition contains no maps")

            first_map = self.map[:50].lower()
            if "timeseries" in first_map:
                self._index_source_type = IndexSourceType.time_series
            elif "counters" in first_map:
                self._index_source_type = IndexSourceType.counters
            else:
                self._index_source_type = IndexSourceType.documents
        return self._index_source_type

    @property
    def type(self):
        value = "Map"
        if self.name and self.name.startswith('Auto/'):
            value = "AutoMap"
            if self.reduce:
                value += "Reduce"
        elif self.reduce:
            return "MapReduce"
        return value

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
        return {"Configuration": self.configuration,
                "Fields": {key: self.fields[key].to_json() for key in self.fields} if len(
                    self.fields) > 0 else self.fields,
                "IndexId": self.index_id,
                "IsTestIndex": self.is_test_index,
                "LockMode": str(self.lock_mod) if self.lock_mod else None,
                "Maps": self.maps,
                "Name": self.name,
                "Reduce": self.reduce,
                "OutputReduceToCollection": self.output_reduce_to_collection,
                "Priority": str(self.priority) if self.priority else None,
                "Type": self.type,
                "SourceType": self.source_type}


class IndexFieldOptions(object):
    def __init__(self, sort_options=None, indexing=None, storage=None, suggestions=None, term_vector=None,
                 analyzer=None):
        """
        @param SortOptions sort_options: Sort options to use for a particular field
        @param FieldIndexing indexing: Options for indexing a field
        @param bool storage: Specifies whether a field should be stored
        @param bool suggestions: If to produce a suggestions in query
        @param FieldTermVector term_vector: Specifies whether to include term vectors for a field
        @param str analyzer: To make an entity property indexed using a specific Analyzer (Need the name)
        """
        self.sort_options = sort_options
        self.indexing = indexing
        self.storage = storage
        self.suggestions = suggestions
        self.term_vector = term_vector
        self.analyzer = analyzer

    def to_json(self):
        return {"Analyzer": self.analyzer,
                "Indexing": str(self.indexing) if self.indexing else None,
                "Sort": str(self.sort_options) if self.sort_options else None,
                "Spatial": None,
                "Storage": None if self.storage is None else "Yes" if self.storage is True else "No",
                "Suggestions": self.suggestions,
                "TermVector": str(self.term_vector) if self.term_vector else None}
