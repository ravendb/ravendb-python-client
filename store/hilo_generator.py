class HiloGenerator(object):
    def __init__(self, capacity):
        self.capacity = capacity

    def generate_document_id(self, entity, convention, request_handler):
        name = entity.__class__.__name__.lower() + 's'
        pass
