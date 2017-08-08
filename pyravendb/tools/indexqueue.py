from time import time as _time
import queue as queue


class IndexQueue(queue.Queue, object):
    def __init__(self):
        super(IndexQueue, self).__init__()

    def peek(self, index=0):
        if not self.qsize():
            return None
        return self.queue[index]

    def get(self, index=None, block=True, timeout=None):
        """Remove and return an item from the queue.

        If optional 'index' is not None the get will return the item in the index place
        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).
        """
        self.not_empty.acquire()
        try:
            if not block:
                if not self._qsize():
                    raise queue.Empty
            elif timeout is None:
                while not self._qsize():
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = _time() + timeout
                while not self._qsize():
                    remaining = endtime - _time()
                    if remaining <= 0.0:
                        raise queue.Empty
                    self.not_empty.wait(remaining)
            if not index or index == 0:
                item = self._get()
            else:
                item = self._get_with_index(index)
            self.not_full.notify()
            return item
        finally:
            self.not_empty.release()

    def _get_with_index(self, index):
        value = self.peek(index)
        self.queue.remove(value)
        return value

    def __lt__(self, other):
        return self.__key__() < other.__key__()

    def __len__(self):
        return self.queue.__len__()
