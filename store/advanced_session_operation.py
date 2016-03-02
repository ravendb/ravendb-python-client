from data.indexes import IndexQuery
from tools.utils import Utils


class Advanced(object):
    def __init__(self, session):
        self.document_query = _DocumentQuery(session=session)


class _DocumentQuery(object):
    def __init__(self, session=None, object_type=None):
        self.session = session
        self.query_builder = ""
        self.object_type = object_type

    def __call__(self, index_name):
        self.index_name = index_name
        return self

    def where_equals(self, field_name, value):
        if len(self.query_builder) > 0 and self.query_builder[len(self.query_builder) - 1:] != ")":
            self.query_builder += " "
        self.query_builder += "{0}:{1}".format(Utils.quote_key(field_name), value, is_analyzed=True)
        return self

    def run(self):
        print(self.query_builder)
        response = self.session.document_store.database_commands.query(self.index_name, IndexQuery(self.query_builder))
        results = []
        conventions = self.session.document_store.conventions
        for result in response["Results"]:
            entity, metadata, original_metadata = Utils.convert_to_entity(result, self.object_type, conventions)
            self.session.save_entity(key=original_metadata["@id"], entity=entity, original_metadata=original_metadata,
                                     metadata=metadata, document=result)
            results.append(entity)
        return results
