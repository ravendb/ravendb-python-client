try:
    import Queue as queue  # < 3.0
except ImportError:
    import queue as queue


class PriorityQueue(queue.PriorityQueue):
    def peek(self, index=0):
        if not self.qsize():
            return None
        return self.queue[index]
