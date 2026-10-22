from __future__ import annotations

import datetime
from enum import Enum
from typing import Optional


class SubscriptionWorkerState(Enum):
    """
    The stage a subscription worker is currently in. Read it through SubscriptionWorker.status.
    """

    # The worker was created but run() was not called yet, so it is not doing anything.
    NOT_STARTED = "NotStarted"
    # Opening a connection to the server, which includes resolving the node that owns the subscription and
    # the TCP handshake. This is also the state while reconnecting after a failure.
    CONNECTING = "Connecting"
    # Connected to the server and waiting for it to send the next batch. The worker is healthy and idle.
    WAITING_FOR_DOCUMENTS = "WaitingForDocuments"
    # A batch was received and handed to the subscriber callback. The state stays here until the callback
    # returned and the batch was acknowledged to the server.
    PROCESSING = "Processing"
    # The connection failed and the worker is going to reconnect. SubscriptionWorkerStatus.exception holds
    # the failure that caused it.
    RETRYING = "Retrying"
    # The worker gave up: the failure it hit cannot be recovered from by reconnecting, so it stopped and the
    # future returned from run() completed with that failure. SubscriptionWorkerStatus.exception holds it.
    # This state is terminal.
    FAULTED = "Faulted"
    # The worker stopped on request, it was closed. This state is terminal.
    STOPPED = "Stopped"


class SubscriptionWorkerStatus:
    """
    An immutable snapshot of what a subscription worker was doing at a single point in time. Taking a snapshot
    keeps the state and the failure that produced it consistent with each other.

    All timestamps are naive datetimes in UTC.
    """

    __slots__ = ("_state", "_exception", "_since_utc", "_failing_since_utc")

    def __init__(
        self,
        state: SubscriptionWorkerState,
        exception: Optional[Exception],
        since_utc: datetime.datetime,
        failing_since_utc: Optional[datetime.datetime],
    ):
        self._state = state
        self._exception = exception
        self._since_utc = since_utc
        self._failing_since_utc = failing_since_utc

    @property
    def state(self) -> SubscriptionWorkerState:
        """
        The stage the worker was in.
        """
        return self._state

    @property
    def exception(self) -> Optional[Exception]:
        """
        The failure that led to the current state. Set for RETRYING and FAULTED, None otherwise. On a worker
        that keeps failing this is the most recent failure, which is not necessarily the one that started
        the trouble, see failing_since_utc.
        """
        return self._exception

    @property
    def since_utc(self) -> datetime.datetime:
        """
        When the worker entered this state. Reaching the same state again does not move it, so a worker that
        keeps processing batches reports the time its current batch started. A worker that cannot reach the
        server alternates between CONNECTING and RETRYING, so this moves on every attempt. Use
        failing_since_utc to tell how long it has been in trouble.
        """
        return self._since_utc

    @property
    def failing_since_utc(self) -> Optional[datetime.datetime]:
        """
        When the worker last stopped being able to talk to the server, or None while it is connected. Unlike
        since_utc this survives the whole reconnect cycle, so it tells how long a worker that is retrying in a
        loop has been broken. It is cleared as soon as the worker is connected again, which makes a value
        other than None the thing to alert on.
        """
        return self._failing_since_utc

    def __str__(self) -> str:
        text = f"{self._state.value} since {self._since_utc.isoformat()}"

        if self._failing_since_utc is not None:
            text += f", failing since {self._failing_since_utc.isoformat()}"

        if self._exception is not None:
            text += f" because of {type(self._exception).__name__}: {self._exception}"

        return text

    def __repr__(self) -> str:
        return f"SubscriptionWorkerStatus({self})"
