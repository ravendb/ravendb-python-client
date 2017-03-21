import time
from pyravendb.custom_exceptions import exceptions


class QueryOperationOptions(object):
    """
      @param allow_stale Indicates whether operations are allowed on stale indexes.
      :type bool
      @param stale_timeout: If AllowStale is set to false and index is stale, then this is the maximum timeout to wait
      for index to become non-stale. If timeout is exceeded then exception is thrown.
      None by default - throw immediately if index is stale.
      @param max_ops_per_sec Limits the amount of base operation per second allowed.
      @param retrieve_details Determines whether operation details about each document should be returned by server.
  """

    def __init__(self, allow_stale=True, stale_timeout=None, max_ops_per_sec=None, retrieve_details=False):
        self.allow_stale = allow_stale
        self.stale_timeout = stale_timeout
        self.retrieve_details = retrieve_details
        self.max_ops_per_sec = max_ops_per_sec


class Operations(object):
    def __init__(self, request_executor):
        self.request_executor = request_executor

    def wait_for_operation_complete(self, operation_id, timeout=None):
        from pyravendb.d_commands.raven_commands import GetOperationStateCommand
        start_time = time.time()
        try:
            get_operation_command = GetOperationStateCommand(operation_id)
            while True:
                response = self.request_executor.execute(get_operation_command)
                if timeout and time.time() - start_time > timeout:
                    raise exceptions.TimeoutException("The operation did not finish before the timeout end")
                if response["Status"] == "Completed":
                    return response
                if response["Status"] == "Faulted":
                    raise exceptions.InvalidOperationException(response["Result"]["Error"])
                time.sleep(0.5)
        except ValueError as e:
            raise exceptions.InvalidOperationException(e)
