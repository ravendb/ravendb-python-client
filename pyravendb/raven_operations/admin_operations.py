from pyravendb.commands.raven_commands import RavenCommand
from pyravendb.tools.utils import Utils
from pyravendb.custom_exceptions import exceptions
from pyravendb.data.indexes import IndexDefinition
from abc import abstractmethod


class AdminOperation(object):
    __slots__ = ['__operation']

    def __init__(self):
        self.__operation = "AdminOperation"

    @property
    def operation(self):
        return self.__operation

    @abstractmethod
    def get_command(self, conventions):
        raise NotImplementedError


class DeleteIndexOperation(AdminOperation):
    def __init__(self, index_name):
        super(DeleteIndexOperation, self).__init__()
        self._index_name = index_name

    def get_command(self, conventions):
        return self.DeleteIndexCommand(self._index_name)

    class _DeleteIndexCommand(RavenCommand):
        def __init__(self, index_name):
            if not index_name:
                raise ValueError("Invalid index_name")
            super(DeleteIndexOperation._DeleteIndexCommand, self).__init__(method="DELETE")
            self._index_name = index_name

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/indexes?name={2}".format(server_node.url, server_node.database,
                                                                   Utils.quote_key(self._index_name))

        def set_response(self, response):
            pass


class DeleteTransformerOperation(AdminOperation):
    def __init__(self, transformer_name):
        super(DeleteIndexOperation, self).__init__()
        self._transformer_name = transformer_name

    def get_command(self, conventions):
        return self._DeleteTransformerCommand(self._transformer_name)

    class _DeleteTransformerCommand(RavenCommand):
        def __init__(self, transformer_name):
            if not transformer_name:
                raise ValueError("Invalid transformer_name")
            super(DeleteTransformerOperation._DeleteTransformerCommand, self).__init__(method="DELETE")
            self._transformer_name = transformer_name

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/transformers?name={2}".format(server_node.url, server_node.database,
                                                                        Utils.quote_key(self._transformer_name))

        def set_response(self, response):
            pass


class GetIndexNamesOperation(AdminOperation):
    def __init__(self, start, page_size):
        super(GetIndexNamesOperation, self).__init__()
        self._start = start
        self._page_size = page_size

    def get_command(self, conventions):
        return self._GetIndexNamesCommand(self._start, self._page_size)

    class _GetIndexNamesCommand(RavenCommand):
        def __init__(self, start, page_size):
            super(GetIndexNamesOperation._GetIndexNamesCommand, self).__init__(method="GET", is_read_request=True)
            self._start = start
            self._page_size = page_size

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/indexes?start={2}&pageSize={3}&namesOnly=true".format(server_node.url,
                                                                                                server_node.database,
                                                                                                self._start,
                                                                                                self._page_size)

        def set_response(self, response):
            if response is None:
                raise ValueError("Invalid response")

            response = response.json()
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])
            if "Results" not in response:
                raise ValueError("Invalid response")

            return response["Results"]


class GetTransformerNamesOperation(AdminOperation):
    def __init__(self, start, page_size):
        super(GetTransformerNamesOperation, self).__init__()
        self._start = start
        self._page_size = page_size

    def get_command(self, conventions):
        return self._GetTransformerNamesCommand(self._start, self._page_size)

    class _GetTransformerNamesCommand(RavenCommand):
        def __init__(self, start, page_size):
            super(GetTransformerNamesOperation._GetTransformerNamesCommand, self).__init__(method="GET",
                                                                                           is_read_request=True)
            self._start = start
            self._page_size = page_size

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/transformers?start={2}&pageSize={3}&namesOnly=true".format(server_node.url,
                                                                                                     server_node.database,
                                                                                                     self._start,
                                                                                                     self._page_size)

        def set_response(self, response):
            if response is None:
                raise ValueError("Invalid response")

            response = response.json()
            if "Error" in response:
                raise exceptions.ErrorResponseException(response["Error"])
            if "Results" not in response:
                raise ValueError("Invalid response")

            return response["Results"]


class PutIndexesOperation(AdminOperation):
    def __init__(self, *indexes_to_add):
        if len(indexes_to_add) == 0:
            raise ValueError("Invalid indexes_to_add")

        super(PutIndexesOperation, self).__init__()
        self._indexes_to_add = indexes_to_add

    def get_command(self, conventions):
        return self._PutIndexesCommand(*self._indexes_to_add)

    class _PutIndexesCommand(RavenCommand):
        def __init__(self, *index_to_add):
            """
            @param index_to_add: Index to add to the database
            :type args of IndexDefinition
            :rtype dict (etag, transformer)
            """
            super(PutIndexesOperation._PutIndexesCommand, self).__init__(method="PUT")
            if index_to_add is None:
                raise ValueError("None indexes_to_add is not valid")

            self.indexes_to_add = []
            for index_definition in index_to_add:
                if not isinstance(index_definition, IndexDefinition):
                    raise ValueError("index_definition in indexes_to_add must be IndexDefinition type")
                if index_definition.name is None:
                    raise ValueError("None Index name is not valid")
                self.indexes_to_add.append(index_definition.to_json())

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/indexes".format(server_node.url, server_node.database)
            self.data = {"Indexes": self.indexes_to_add}

        def set_response(self, response):
            try:
                response = response.json()
                if "Error" in response:
                    raise exceptions.ErrorResponseException(response["Error"])
                return response["Results"]
            except ValueError:
                response.raise_for_status()


class PutTransformerOperation(AdminOperation):
    def __init__(self, transformer_definition):
        if not transformer_definition:
            raise ValueError("Invalid transformer_definition")
        
        super(PutTransformerOperation, self).__init__()
        self._transformer_definition = transformer_definition

    def get_command(self, conventions):
        return self._PutTransformerCommand(self._transformer_definition)

    class _PutTransformerCommand(RavenCommand):
        def __init__(self, transformer_definition):
            """
            @param transformer_definition: the transformer to add to the database
            :type TransformerDefinition
            """
            super(PutTransformerOperation._PutTransformerCommand, self).__init__(method="PUT")
            if transformer_definition is None or not transformer_definition.name:
                raise ValueError("transformer_definition is not valid")

            self._transformer_definition = transformer_definition

        def create_request(self, server_node):
            self.url = "{0}/databases/{1}/transformers?name={2}".format(server_node.url, server_node.database,
                                                                        Utils.quote_key(
                                                                            self._transformer_definition.name))
            self.data = self._transformer_definition.to_json()

        def set_response(self, response):
            try:
                response = response.json()
                if "Error" in response:
                    raise exceptions.ErrorResponseException(response["Error"])
                return response
            except ValueError:
                response.raise_for_status()
