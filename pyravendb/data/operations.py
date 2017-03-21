from pyravendb.custom_exceptions import exceptions
import time


class BulkOperationOption(object):
    """
      :param allow_stale Indicates whether operations are allowed on stale indexes.
      :param stale_timeout If AllowStale is set to false and index is stale, then this is the maximum timeout to wait
      for index to become non-stale. If timeout is exceeded then exception is thrown.
      None by default - throw immediately if index is stale.
      :param max_ops_per_sec Limits the amount of base operation per second allowed.
      :param retrieve_details Determines whether operation details about each document should be returned by server.
  """

    def __init__(self, allow_stale=True, stale_timeout=None, max_ops_per_sec=None, retrieve_details=False):
        self.allow_stale = allow_stale
        self.stale_timeout = stale_timeout
        self.retrieve_details = retrieve_details
        self.max_ops_per_sec = max_ops_per_sec


class Operations(object):
    def __init__(self, request_handler):
        self.request_handler = request_handler

    def wait_for_operation_complete(self, operation_id, timeout=None):
        start_time = time.time()
        try:
            path = "operation/status?id={0}".format(operation_id)
            while True:
                response = self.request_handler.http_request_handler(path, "GET")
                if response.status_code == 200:
                    response = response.json()
                if timeout and time.time() - start_time > timeout:
                    raise exceptions.TimeoutException("The operation did not finish before the timeout end")
                if response["Faulted"]:
                    if "Error" in response["State"]:
                        error = response["State"]["Error"]
                    else:
                        error = "Something went wrong with the operation"
                    raise exceptions.InvalidOperationException(error)
                if response["Completed"]:
                    return response
                time.sleep(0.5)
        except ValueError as e:
            raise exceptions.InvalidOperationException(e)
