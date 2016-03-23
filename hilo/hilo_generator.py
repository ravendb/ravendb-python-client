from custom_exceptions import exceptions
from threading import Lock


class Range(object):
    def __init__(self, min_id=1, max_id=0):
        self.min_id = min_id
        self.max_id = max_id
        self.current = self.min_id - 1


class HiloGenerator(object):
    def __init__(self, capacity, database_commands):
        self.capacity = capacity
        self.collection_ranges = {}
        self.lock = Lock()
        self.database_commands = database_commands

    def generate_document_id(self, entity, convention, request_handler):
        """
        @param entity: the object we want to generate id for
        :type object
        @param convention: the document_store convention
        :type Convention()
        @param request_handler: the handler for the requests
        :type HttpRequestsFactory
        @return: the id
        :rtype: int
        """
        type_tag_name = convention.default_transform_type_tag_name(entity.__class__.__name__)
        if type_tag_name in self.collection_ranges:
            self.next_id(type_tag_name, request_handler)
        else:
            with self.lock:
                if type_tag_name in self.collection_ranges:
                    self.next_id(type_tag_name, request_handler)
                    return "{0}/{1}".format(type_tag_name, self.collection_ranges[type_tag_name].current)
                ranges = {type_tag_name: Range()}
                ranges.update(self.collection_ranges)
                self.collection_ranges = ranges
            self.next_id(type_tag_name, request_handler)
        return "{0}/{1}".format(type_tag_name, self.collection_ranges[type_tag_name].current)

    def next_id(self, type_tag_name, request_handler):
        while True:
            my_collection_range = self.collection_ranges[type_tag_name]
            current_max = my_collection_range.max_id
            with self.lock:
                my_collection_range.current += 1
                current_id = my_collection_range.current
            if current_id <= current_max:
                return
            with self.lock:
                if my_collection_range != self.collection_ranges[type_tag_name]:
                    continue
                try:
                    self.collection_ranges[type_tag_name] = self.get_next_range(type_tag_name, request_handler)
                except exceptions.FetchConcurrencyException:
                    pass

    def get_next_range(self, type_tag_name, request_handler):
        while True:
            path = "Raven/Hilo/{0}&id=Raven/ServerPrefixForHilo".format(type_tag_name)
            document = None
            try:
                document = self.database_commands.get(path)["Results"][0]
            except (exceptions.ErrorResponseException, IndexError):
                pass
            etag = ""
            min_id = 1
            if document is None:
                max_id = self.capacity
            else:
                min_id = document["Max"] + 1
                max_id = document["Max"] + self.capacity
                etag = document["@metadata"]["@etag"]

            try:
                request_handler.call_hilo(type_tag_name, max_id, etag)
                return Range(min_id, max_id)
            except exceptions.FetchConcurrencyException:
                pass
