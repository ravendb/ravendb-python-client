from typing import List, Optional


class _CommandData(object):
    def __init__(self, key, command_type, change_vector=None, metadata=None):
        self.key = key
        self._type = command_type
        self.metadata = metadata
        self.change_vector = change_vector
        self.additionalData = None
        self.__command = True

    def to_json(self):
        raise NotImplementedError

    @property
    def command(self):
        return self.__command

    @property
    def type(self):
        return self._type


class TimeSeriesBatchCommandData(_CommandData):
    def __init__(
        self,
        document_id: str,
        name: str,
        operation: "TimeSeriesOperation",
        change_vector=None,
    ):

        if not document_id:
            raise ValueError("None or empty document id is Invalid")

        if not name:
            raise ValueError("None or empty name is Invalid")

        super().__init__(key=document_id, command_type="TimeSeries", change_vector=change_vector)
        self._time_series = operation

    @property
    def time_series(self):
        return self._time_series

    def to_json(self):
        return {
            "Id": self.key,
            "TimeSeries": self.time_series.to_json(),
            "Type": self.type,
        }


class CountersBatchCommandData(_CommandData):
    def __init__(
        self,
        document_id: str,
        counter_operations: List[CounterOperation] or CounterOperation,
        from_etl: Optional[bool] = None,
        change_vector=None,
    ):
        if not document_id:
            raise ValueError("None or empty document id is Invalid")
        if not counter_operations:
            raise ValueError("invalid counter_operations")
        if not isinstance(counter_operations, list):
            counter_operations = [counter_operations]

        super().__init__(key=document_id, command_type="Counters", change_vector=change_vector)
        self._from_etl = from_etl
        self._counters = DocumentCountersOperation(document_id=self.key, operations=counter_operations)

    def has_increment(self, counter_name):
        self.has_operation_of_type(CounterOperationType.increment, counter_name)

    def has_delete(self, counter_name):
        self.has_operation_of_type(CounterOperationType.delete, counter_name)

    def has_operation_of_type(self, operation_type: CounterOperationType, counter_name: str):
        for op in self.counters.operations:
            if op.counter_name != counter_name:
                continue

            if op.counter_operation_type == operation_type:
                return True
        return False

    @property
    def counters(self):
        return self._counters

    def to_json(self):
        json_dict = {
            "Id": self.key,
            "Counters": self.counters.to_json(),
            "Type": self.type,
        }
        if self._from_etl:
            json_dict["FromEtl"] = self._from_etl
        return json_dict
