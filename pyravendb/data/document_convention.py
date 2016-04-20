from pyravendb.data.indexes import SortOptions
from enum import Enum
from inflector import Inflector


class Failover(Enum):
    """
    Allow to read from the secondary server(s), but immediately fail writes
    to the secondary server(s).

    This is usually the safest approach, because it means that you can still serve
    read requests when the primary node is down, but don't have to deal with replication
    conflicts if there are writes to the secondary when the primary node is down.
    """
    allow_reads_from_secondaries = 1,

    """
    Allow reads from and writes to secondary server(s).
    Choosing this option requires that you'll have some way of propagating changes
    made to the secondary server(s) to the primary node when the primary goes back
    up.
    A typical strategy to handle this is to make sure that the replication is setup
    in a master/master relationship, so any writes to the secondary server will be
    replicated to the master server.
    Please note, however, that this means that your code must be prepared to handle
    conflicts in case of different writes to the same document across nodes.
    """

    allow_reads_from_secondaries_and_writes_to_secondaries = 3,

    """
    Immediately fail the request, without attempting any failover. This is true for both
    reads and writes. The RavenDB client will not even check that you are using replication.
    This is mostly useful when your replication setup is meant to be used for backups / external
    needs, and is not meant to be a failover storage.
    """

    fail_immediately = 0,

    """
    Read requests will be spread across all the servers, instead of doing all the work against the master.
    Write requests will always go to the master.
    This is useful for striping, spreading the read load among multiple servers. The idea is that this will give us
    better read performance overall.
    A single session will always use the same server, we don't do read striping within a single session.
    Note that using this means that you cannot set UserOptimisticConcurrency to true,
    because that would generate concurrency exceptions.
    If you want to use that, you have to open the session with ForceReadFromMaster set to true.
    """
    read_from_all_servers = 1024


inflector = Inflector()


class DocumentConvention(object):
    def __init__(self):
        self.max_number_of_request_per_session = 30
        self.max_ids_to_catch = 32
        # timeout for wait to server in seconds
        self.timeout = 30
        self.failover_behavior = Failover.allow_reads_from_secondaries
        self.default_use_optimistic_concurrency = True
        self._system_database = "system"

    @staticmethod
    def default_transform_plural(name):
        return inflector.conditional_plural(2, name)

    @staticmethod
    def default_transform_type_tag_name(name):
        count = sum(1 for c in name if c.isupper())
        if count <= 1:
            return DocumentConvention.default_transform_plural(name.lower())
        return DocumentConvention.default_transform_plural(name)

    @staticmethod
    def build_default_metadata(entity):
        if entity is None:
            return {}
        return {"Raven-Entity-Name": DocumentConvention.default_transform_plural(entity.__class__.__name__),
                "Raven-Python-Type": "{0}.{1}".format(entity.__class__.__module__, entity.__class__.__name__)}

    @staticmethod
    def try_get_type_from_metadata(metadata):
        if "Raven-Python-Type" in metadata:
            return metadata["Raven-Python-Type"]
        return None

    @staticmethod
    def uses_range_type(obj):
        if obj is None:
            return False

        if isinstance(obj, int) or isinstance(obj, long) or isinstance(obj, float):
            return True

    @staticmethod
    def get_default_sort_option(type_name):
        if not type_name:
            return None
        if type_name == "int" or type_name == "float":
            return SortOptions.float.value
        if type_name == "long":
            return SortOptions.long.value

    @property
    def system_database(self):
        return self._system_database
