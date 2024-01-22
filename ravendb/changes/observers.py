from __future__ import annotations
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Lock
from typing import Callable, Generic, TypeVar, Optional

from ravendb.tools.concurrentset import ConcurrentSet

_T_Change = TypeVar("_T_Change")


class Observable(Generic[_T_Change]):
    def __init__(
        self,
        on_connect: Optional[Callable[[], None]] = None,
        on_disconnect: Optional[Callable[[], None]] = None,
        executor: Optional[ThreadPoolExecutor] = None,
    ):
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

    def on_document_change_notification(self, value: _T_Change):
        try:
            if self._filter(value):
                for subscriber in self._subscribers:
                    subscriber.on_next(value)
        except Exception as e:
            self.error(e)

    def subscribe(self, on_next_callback: Callable[[], None]) -> Callable[[], None]:
        """
        @param func on_next_callback: The action that observer will do when changes happens
        :return: method that close the subscriber
        """
        self.inc()
        observer = ActionObserver(on_next=on_next_callback)
        self._subscribers.add(observer)

        def close_action() -> None:
            self.dec()
            self._subscribers.remove(observer)
            if observer.on_completed_callback is not None:
                observer.on_completed()

        return close_action

    def subscribe_with_observer(self, observer: ActionObserver) -> Callable[[], None]:
        """
        @param Observer observer: The observer that will do action when changes happens
        :return: method that close the subscriber
        """
        self.inc()
        self._subscribers.add(observer)

        def close_action() -> None:
            self.dec()
            self._subscribers.remove(observer)
            if observer.on_completed_callback is not None:
                observer.on_completed()

        return close_action

    def inc(self):
        with self.value_lock:
            self._value += 1

    def dec(self):
        with self.value_lock:
            self._value -= 1
            if self._value == 0:
                self.set(self._executor.submit(self._on_disconnect))

    def set(self, future: Future) -> None:
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

    def error(self, exception: Exception):
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

    def send(self, msg: _T_Change):
        try:
            if self._filter(msg):
                for subscriber in self._subscribers:
                    subscriber.on_next(msg)
        except Exception as e:
            self.error(e)
            return


class ActionObserver:
    def __init__(
        self,
        on_next: Callable[[_T_Change], None],
        on_error: Callable[[Exception], None] = None,
        on_completed: Callable[[], None] = None,
    ):
        self._on_next_callback = on_next
        self._on_error_callback = on_error
        self._on_completed_callback = on_completed

    def on_next(self, value: _T_Change) -> None:
        self._on_next_callback(value)

    def on_error(self, exception: Exception) -> None:
        if self._on_error_callback:
            self._on_error_callback(exception)

    def on_completed(self) -> None:
        if self._on_completed_callback:
            self._on_completed_callback()

    @property
    def on_completed_callback(self):
        return self._on_completed_callback
