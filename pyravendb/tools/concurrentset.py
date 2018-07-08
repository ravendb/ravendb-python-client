from threading import Lock


class ConcurrentSet(set):
    def __init__(self, seq=()):
        super(ConcurrentSet, self).__init__(seq)
        self._lock = Lock()

    def add(self, element):
        with self._lock:
            super(ConcurrentSet, self).add(element)

    def discard(self, element):
        with self._lock:
            super(ConcurrentSet, self).discard(element)
