from abc import ABCMeta, abstractmethod
from pyravendb.tools.concurrentset import ConcurrentSet
from concurrent.futures import Future
from threading import Lock


class Observable:
    def __init__(self, on_connect=None, on_disconnect=None, executor=None):
        self.on_connect = on_connect
        self._on_disconnect = on_disconnect
        self.last_exception = None
        self.value_lock = Lock()
        self._value = 0
        self._future = None
        self._future_set = Future()
        self._subscribers = ConcurrentSet()
        self._executor = executor

    def __call__(self, filter_method):
        self._filter = filter_method
        return self

    def on_document_change_notification(self, value):
        try:
            if self._filter(value):
                for subscriber in self._subscribers:
                    subscriber.on_next(value)
        except Exception as e:
            self.error(e)

    def subscribe(self, observer):
        """
        @param Observer or func observer: The observer that will do action when changes happens
        :return: method that close the subscriber
        """
        self.inc()
        if not isinstance(observer, Observer):
            observer = ActionObserver(on_next=observer)
        self._subscribers.add(observer)

        def close_action():
            self.dec()
            subscriber = self._subscribers.pop(observer)
            if subscriber.on_complete:
                subscriber.on_complete()

        return close_action

    def inc(self):
        with self.value_lock:
            self._value += 1

    def dec(self):
        with self.value_lock:
            self._value -= 1
            if self._value == 0:
                self.set(self._executor.submit(self._on_disconnect))

    def set(self, future):
        if not self._future_set.done():
            def done_callback(f):
                try:
                    if f.cancelled():
                        self._future_set.cancel()
                    elif f.exception():
                        self._future_set.set_exception(f.exception)
                    else:
                        self._future_set.set_result(None)
                except Exception:
                    raise

            future.add_done_callback(done_callback)
        self._future = future

    def error(self, exception):
        future = Future()
        self.set(future)
        future.set_exception(exception)
        self.last_exception = exception
        for subscriber in self._subscribers:
            subscriber.on_error(exception)

    def close(self):
        future = Future()
        self.set(future)
        future.cancel()

        for subscriber in self._subscribers:
            subscriber.on_completed()

    def ensure_subscribe_now(self):
        self._future.result() if self._future else self._future_set.result()

    def send(self, msg):
        try:
            if self._filter(msg):
                for subscriber in self._subscribers:
                    subscriber.on_next(msg)
        except Exception as e:
            self.error(e)
            return


class Observer(metaclass=ABCMeta):
    @abstractmethod
    def on_completed(self):
        pass

    @abstractmethod
    def on_error(self, exception):
        pass

    @abstractmethod
    def on_next(self, value):
        pass


class ActionObserver(Observer):
    def __init__(self, on_next, on_error=None, on_completed=None):
        self._on_next = on_next
        self._on_error = on_error
        self._on_completed = on_completed

    def on_next(self, value):
        self._on_next(value)

    def on_error(self, exception):
        if self._on_error:
            self._on_error(exception)

    def on_completed(self):
        if self._on_completed:
            self._on_completed()
