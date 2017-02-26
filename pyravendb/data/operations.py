from enum import Enum


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


class QueryOperator(Enum):
    OR = "OR"
    AND = "AND"