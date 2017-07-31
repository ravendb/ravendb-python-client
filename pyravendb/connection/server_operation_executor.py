class ServerOperationExecutor:
    def __init__(self, document_store):
        self._store = document_store
        self._request_executor = None

    @property
    def request_executor(self):
        if self._request_executor is None:
            from pyravendb.connection.cluster_requests_executor import ClusterRequestExecutor
            if self._store.conventions.disable_topology_update:
                self._request_executor = ClusterRequestExecutor.create_for_single_node(self._store.urls[0],
                                                                                       self._store.certificate)
            else:
                self._request_executor = ClusterRequestExecutor.create(self._store.urls, self._store.certificate)
        return self._request_executor

    def send(self, operation):
        try:
            operation_type = getattr(operation, 'operation')
            if operation_type != "ServerOperation":
                raise ValueError("operation type cannot be {0} need to be Operation".format(operation_type))
        except AttributeError:
            raise ValueError("Invalid operation")

        command = operation.get_command(self.request_executor.convention)
        return self.request_executor.execute(command)
