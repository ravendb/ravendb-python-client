from pyravendb.data.document_conventions import DocumentConventions
from pyravendb.data.query import IndexQuery
from pyravendb.documents.commands.multi_get import Content
from pyravendb.extensions.json_extensions import JsonExtensions


class IndexQueryContent(Content):
    def __init__(self, conventions: DocumentConventions, query: IndexQuery):
        self.__conventions = conventions
        self.__query = query

    def write_content(self) -> dict:
        return JsonExtensions.write_index_query(self.__conventions, self.__query)
