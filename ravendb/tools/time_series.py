from datetime import datetime


class TSRangeHelper:
    @staticmethod
    def left(date: datetime) -> datetime:
        return date or datetime.min

    @staticmethod
    def right(date: datetime) -> datetime:
        return date or datetime.max
