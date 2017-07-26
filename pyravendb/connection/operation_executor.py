import time
from pyravendb.custom_exceptions import exceptions


class OperationExecutor(object):
    def __init__(self, document_store, database_name=None):
        self._document_store = document_store
        self._database_name = database_name
        self._request_executor = document_store.get_request_executor(db_name=database_name)

    def wait_for_operation_complete(self, operation_id, timeout=None):
        from pyravendb.d_commands.raven_commands import GetOperationStateCommand
        start_time = time.time()
        try:
            get_operation_command = GetOperationStateCommand(operation_id)
            while True:
                response = self.request_executor.execute(get_operation_command)
                if timeout and time.time() - start_time > timeout:
                    raise exceptions.TimeoutException("The Operation did not finish before the timeout end")
                if response["Status"] == "Completed":
                    return response
                if response["Status"] == "Faulted":
                    raise exceptions.InvalidOperationException(response["Result"]["Error"])
                time.sleep(0.5)
        except ValueError as e:
            raise exceptions.InvalidOperationException(e)

    def send(self, operation):
        command = operation.get_command(self._document_store, self._request_executor.convention)
        return self._request_executor.execute(command)