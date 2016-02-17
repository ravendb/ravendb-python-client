class Range(object):
    def __init__(self, min_id, max_id):
        self.min_id = min_id
        self.max_id = max_id
        self.current = min_id


class HiloGenerator(object):
    def __init__(self, capacity):
        self.capacity = capacity
        self.hilo_type = {}

    def generate_document_id(self, entity, convention, request_handler):
        """
        @param entity: the object we want to generate id for
        :type object
        @param convention: the document_store convention
        :type Convention()
        @param request_handler: the handler for the requests
        :type HttpRequestsFactory
        @return: the id and if use any request to server
        :rtype: int and bool
        """
        name = entity.__class__.__name__
        type_tag_name = convention.default_transform_type_tag_name(entity.__class__.__name__)
        if name in self.hilo_type:
            self.hilo_type[name].current += 1
            if not self.hilo_type[name].current > self.hilo_type[name].max_id:
                return "{0}/{1}".format(type_tag_name, str(self.hilo_type[name].current)), False

        max_id_from_server = request_handler.call_hilo(type_tag_name)
        self.hilo_type[name] = Range(max_id_from_server + 1, max_id_from_server + convention.max_ids_to_catch)
        return "{0}/{1}".format(type_tag_name, str(self.hilo_type[name].current)), True
