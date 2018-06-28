from pyravendb.custom_exceptions import exceptions
from pyravendb.commands.raven_commands import RavenCommand
from pyravendb.tools.utils import Utils
from datetime import datetime
from threading import Lock


class RangeValue(object):
    def __init__(self, min_id=1, max_id=0):
        self.min_id = min_id
        self.max_id = max_id
        self.current = self.min_id - 1


class HiLoReturnCommand(RavenCommand):
    def __init__(self, tag, last, end):
        super(HiLoReturnCommand, self).__init__(method="PUT")
        self.tag = tag
        self.last = last
        self.end = end

    def create_request(self, server_node):
        path = "hilo/return?tag={0}&end={1}&last={2}".format(self.tag, self.end, self.last)
        self.url = "{0}/databases/{1}/{2}".format(server_node.url, server_node.database, path)

    def set_response(self, response):
        pass


class NextHiLoCommand(RavenCommand):
    def __init__(self, tag, last_batch_size, last_range_at, identity_parts_separator, last_range_max):
        super(NextHiLoCommand, self).__init__(method="GET")
        self.tag = tag
        self.last_batch_size = last_batch_size
        self.last_range_at = last_range_at
        self.identity_parts_separator = identity_parts_separator
        self.last_range_max = last_range_max
        self.server_tag = None

    def create_request(self, server_node):
        path = "hilo/next?tag={0}&lastBatchSize={1}&lastRangeAt={2}&identityPartsSeparator={3}&lastMax={4}".format(
            self.tag, self.last_batch_size, self.last_range_at, self.identity_parts_separator, self.last_range_max)
        self.url = "{0}/databases/{1}/{2}".format(server_node.url, server_node.database, path)

    def set_response(self, response):
        if response is None:
            raise ValueError("response is invalid.")

        if response.status_code == 201:
            response = response.json()
            return {"prefix": response["Prefix"], "server_tag": response["ServerTag"], "low": response["Low"],
                    "high": response["High"],
                    "last_size": response["LastSize"],
                    "last_range_at": response["LastRangeAt"]}
        if response.status_code == 500:
            raise exceptions.DatabaseDoesNotExistException(response.json()["Error"])
        if response.status_code == 409:
            raise exceptions.FetchConcurrencyException(response.json()["Error"])

        raise exceptions.ErrorResponseException("Something is wrong with the request")


class MultiDatabaseHiLoKeyGenerator(object):
    def __init__(self, store):
        self._store = store
        self.conventions = self._store.conventions
        self._generators = {}

    def generate_document_key(self, db_name, entity):
        db = db_name if db_name is not None else self._store.database
        if db not in self._generators:
            generator = MultiTypeHiLoKeyGenerator(self._store, db)
            self._generators[db] = generator
        else:
            generator = self._generators[db]

        return generator.generate_document_key(entity)

    def return_unused_range(self):
        for key in self._generators:
            self._generators[key].return_unused_range()


class MultiTypeHiLoKeyGenerator(object):
    def __init__(self, store, db_name):
        self._store = store
        self._db_name = db_name
        self._conventions = store.conventions
        self.lock = Lock()
        self.key_generators_by_tag = {}

    def generate_document_key(self, entity):
        tag = self._conventions.default_transform_type_tag_name(entity.__class__.__name__)
        if tag is None:
            return None
        else:
            with self.lock:
                if tag in self.key_generators_by_tag:
                    value = self.key_generators_by_tag[tag]
                else:
                    value = HiLoKeyGenerator(tag, self._store, self._db_name)
                    self.key_generators_by_tag[tag] = value
        return value.generate_document_key()

    def return_unused_range(self):
        for key in self.key_generators_by_tag:
            self.key_generators_by_tag[key].return_unused_range()


class HiLoKeyGenerator(object):
    def __init__(self, tag, store, db_name):
        self._tag = tag
        self._store = store
        self._db_name = db_name if db_name is not None else store.database
        self._last_range_at = datetime(1, 1, 1)
        self._last_batch_size = 0
        self._range = RangeValue()
        self._prefix = None
        self._server_tag = None
        self.conventions = store.conventions
        self._identity_parts_separator = self.conventions.identity_parts_separator
        self.collection_ranges = {}
        self.lock = Lock()

    def get_document_key_from_id(self, next_id):
        return "{0}{1}-{2}".format(self._prefix, next_id, self._server_tag)

    def generate_document_key(self):
        ranges = {self._tag: RangeValue()}
        ranges.update(self.collection_ranges)
        self.collection_ranges = ranges
        self.next_id()
        return self.get_document_key_from_id(self.collection_ranges[self._tag].current)

    def next_id(self):
        while True:
            my_collection_range = self.collection_ranges[self._tag]
            current_max = my_collection_range.max_id
            with self.lock:
                my_collection_range.current += 1
                current_id = my_collection_range.current
            if current_id <= current_max:
                return
            with self.lock:
                if my_collection_range != self.collection_ranges[self._tag]:
                    continue
                try:
                    self.get_next_range()
                    self.collection_ranges[self._tag] = self._range
                except exceptions.FetchConcurrencyException:
                    pass

    def get_next_range(self):
        hilo_command = NextHiLoCommand(self._tag, self._last_batch_size, self._last_range_at,
                                       self._identity_parts_separator, self._range.max_id)
        result = self._store.get_request_executor().execute(hilo_command)
        self._prefix = result["prefix"]
        self._server_tag = result["server_tag"]
        self._last_range_at = Utils.string_to_datetime(result["last_range_at"])
        self._last_batch_size = result["last_size"]
        self._range = RangeValue(result["low"], result["high"])

    def return_unused_range(self):
        return_command = HiLoReturnCommand(self._tag, self._range.current, self._range.max_id)
        self._store.get_request_executor().execute(return_command)
