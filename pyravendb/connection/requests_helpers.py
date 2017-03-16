from pyravendb.data.document_convention import WriteBehavior, ReadBehavior
from datetime import timedelta


class ServerNode(object):
    def __init__(self, url, database, api_key=None, current_token=None, is_failed=False):
        self.url = url
        self.database = database
        self.api_key = api_key
        self.current_token = current_token
        self.is_failed = is_failed
        self._response_time = []
        self._is_rate_surpassed = None

    @property
    def response_time(self):
        return self._response_time

    @response_time.setter
    def response_time(self, value):
        self._response_time.insert(len(self._response_time) % 5, value)

    def ewma(self):
        ewma = 0
        divide = len(self._response_time)
        if len(self._response_time) == 0:
            return ewma
        for time in self._response_time:
            ewma += time
        return ewma / divide if divide != 0 else 0

    def is_rate_surpassed(self, request_time_sla_threshold_in_milliseconds):
        if self._is_rate_surpassed:
            self._is_rate_surpassed = self.ewma() >= 0.75 * request_time_sla_threshold_in_milliseconds
        else:
            self._is_rate_surpassed = self.ewma() >= request_time_sla_threshold_in_milliseconds

        return self._is_rate_surpassed


class Topology(object):
    def __init__(self, etag=0, leader_node=None, read_behavior=ReadBehavior.leader_only,
                 write_behavior=WriteBehavior.leader_only, nodes=None, sla=None):
        self.etag = etag
        self.leader_node = leader_node
        self.read_behavior = read_behavior
        self.write_behavior = write_behavior
        self.nodes = [] if nodes is None else nodes
        self.sla = 100 / 1000 if sla is None else sla
