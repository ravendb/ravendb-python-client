from __future__ import annotations

from datetime import datetime
from typing import Optional

from ravendb.primitives.exceptions import OperationCancelledException


class CancellationTokenSource:
    def __init__(self):
        self.cancelled = False
        self._cancel_after_date: Optional[float] = None

    def get_token(self) -> CancellationToken:
        return self.CancellationToken(self)

    class CancellationToken:
        def __init__(self, source: CancellationTokenSource):
            self.__source = source

        def is_cancellation_requested(self) -> bool:
            return self.__source.cancelled or (
                self.__source._cancel_after_date is not None
                and datetime.now().timestamp() > self.__source._cancel_after_date
            )

        def throw_if_cancellation_requested(self) -> None:
            if self.is_cancellation_requested():
                raise OperationCancelledException()

    def cancel(self) -> None:
        self.cancelled = True

    def cancel_after(self, timeout_in_seconds: float) -> None:
        self._cancel_after_date = datetime.now().timestamp() + timeout_in_seconds
